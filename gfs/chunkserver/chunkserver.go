package chunkserver

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"

	// "sync"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	log "github.com/sirupsen/logrus"

	"gfs"
	"gfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	address    gfs.ServerAddress // chunkserver address
	master     gfs.ServerAddress // master address
	serverRoot string            // path to data storage
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to true if server is shutdown

	dl                     *downloadBuffer                 // expiring download buffer
	pendingLeaseExtensions *util.ArraySet[gfs.ChunkHandle] // pending lease extension
	chunk                  map[gfs.ChunkHandle]*ChunkInfo  // chunk information
	chunkLock              sync.RWMutex
	garbage                map[gfs.ChunkHandle]bool // set of garbage chunks
}

type ChunkInfo struct {
	lock    sync.RWMutex
	Length  gfs.Offset
	Version gfs.ChunkVersion // version number of the chunk in disk
}

const (
	GarbageCollectInterval = 10 * time.Second
	metadataFilename       = "chunkserver.meta"
)

// checkChunkFiles checks the chunk files in the server root directory.
// Currently it only checks if the file exists.
// TODO: add checksum
func (cs *ChunkServer) checkChunkFiles() {
	cs.chunkLock.Lock()
	defer cs.chunkLock.Unlock()
	for handle := range cs.chunk {
		chunkpath := cs.getChunkPath(handle)
		info, err := os.Stat(chunkpath)
		if err != nil || info.IsDir() {
			log.Warnf("[%v] checkChunkFiles: chunk %v error", cs.address, handle)
			delete(cs.chunk, handle)
		}
	}
}

// loadMetadata loads metadata from disk
func (cs *ChunkServer) loadMetadata() error {
	name := path.Join(cs.serverRoot, metadataFilename)
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	var tmp map[gfs.ChunkHandle]*ChunkInfo
	dec := gob.NewDecoder(f)
	err = dec.Decode(&tmp)
	if err != nil {
		return err
	}

	cs.chunkLock.Lock()
	cs.chunk = tmp
	cs.chunkLock.Unlock()
	cs.checkChunkFiles()
	return nil
}

// saveMetadata saves metadata to disk
func (cs *ChunkServer) saveMetadata() error {
	cs.chunkLock.RLock()
	defer cs.chunkLock.RUnlock()

	name := path.Join(cs.serverRoot, metadataFilename)
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	err = enc.Encode(cs.chunk)
	return err
}

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:                addr,
		shutdown:               make(chan struct{}),
		master:                 masterAddr,
		serverRoot:             serverRoot,
		dl:                     newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet[gfs.ChunkHandle]),
		chunk:                  make(map[gfs.ChunkHandle]*ChunkInfo),
	}

	info, err := os.Stat(serverRoot)
	if err != nil {
		log.Info("ServerRoot not found, creating...")
		err := os.Mkdir(serverRoot, 0755)
		if err != nil {
			log.Fatalf("Create serverRoot error: %v", err)
		}
	} else if !info.IsDir() {
		log.Fatal("ServerRoot is not a directory")
	}

	err = cs.loadMetadata()
	if err != nil {
		log.Warn("load metadata error: ", err)
	}

	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				// if chunk server is dead, ignores connection error
				if !cs.dead {
					log.Fatal(err)
				}
			}
		}
	}()

	// Heartbeat
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			cs.heartbeat()
			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	// Garbage collection
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			cs.garbageCollect()
			time.Sleep(GarbageCollectInterval)
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

func (cs *ChunkServer) heartbeat() {
	le := cs.pendingLeaseExtensions.GetAllAndClear()
	args := &gfs.HeartbeatArg{
		Address:         cs.address,
		LeaseExtensions: le,
	}
	var reply gfs.HeartbeatReply
	err := util.Call(cs.master, "Master.RPCHeartbeat", args, &reply)
	if err != nil {
		log.Error("heartbeat rpc error ", err)
	}
	// add garbage chunks
	cs.chunkLock.Lock()
	for _, handle := range reply.Garbage {
		cs.garbage[handle] = true
	}
	cs.chunkLock.Unlock()
}

// garbageCollect removes the chunks that are not in the master's record.
func (cs *ChunkServer) garbageCollect() {
	log.Infof("[%v] garbageCollect", cs.address)
	cs.chunkLock.Lock()
	defer cs.chunkLock.Unlock()
	for handle := range cs.garbage {
		log.Infof("[%v] garbageCollect: remove chunk %v", cs.address, handle)
		chunkpath := cs.getChunkPath(handle)
		err := os.Remove(chunkpath)
		if err != nil {
			log.Errorf("[%v] garbageCollect: remove chunk %v error: %v", cs.address, handle, err)
		}
		delete(cs.chunk, handle)
		delete(cs.garbage, handle)
	}
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
	err := cs.saveMetadata()
	if err != nil {
		log.Error("save metadata error: ", err)
	}
}

// RPCReportChunks is called by master to report chunks that the chunkserver holds.
func (cs *ChunkServer) RPCReportChunks(args gfs.ReportChunksArg, reply *gfs.ReportChunksReply) error {
	cs.chunkLock.RLock()
	defer cs.chunkLock.RUnlock()
	for handle, chunk := range cs.chunk {
		reply.Chunks = append(reply.Chunks, gfs.ChunkInfo{
			Handle:  handle,
			Length:  chunk.Length,
			Version: chunk.Version,
		})
	}
	return nil
}

// triggerReportChunks triggers the master to ask this chunkserver for chunks.
func (cs *ChunkServer) triggerReportChunks() error {
	return util.Call(cs.master, "Master.RPCTriggerReportChunks",
		gfs.TriggerReportChunksArg{Address: cs.address}, nil)
}

// RPCCheckVersion is called by master to check the version of a chunk.
// If the version matches, it increments the version number recorded in the chunkserver.
func (cs *ChunkServer) RPCCheckVersion(args gfs.CheckVersionArg, reply *gfs.CheckVersionReply) error {
	log.Infof("[%v] RPCCheckVersion: chunk %v want %v", cs.address, args.Handle, args.Version)
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[args.Handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}
	chunk.lock.Lock()
	defer chunk.lock.Unlock()
	reply.Version = chunk.Version
	if chunk.Version == args.Version {
		chunk.Version++
	} else {
		log.Errorf("[%v] chunk %v (%p) version mismatch: local %v, master %v", cs.address, args.Handle, chunk, chunk.Version, args.Version)
	}
	return nil
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error {
	id := gfs.NewDataID(args.Handle)
	cs.dl.Set(id, args.Data)
	for _, server := range args.ForwardTo {
		var reply_ gfs.ForwardDataReply
		err := util.Call(server, "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: id, Data: args.Data}, nil)
		reply.Err = reply_.Err
		if err != nil { // TODO: check
			return err
		}
	}
	reply.DataID = id
	return nil
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	cs.dl.Set(args.DataID, args.Data)
	reply.Err = gfs.Success
	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	cs.chunkLock.Lock()
	defer cs.chunkLock.Unlock()
	_, ok := cs.chunk[args.Handle]
	if ok {
		return fmt.Errorf("chunk %v already exists", args.Handle)
	}
	cs.chunk[args.Handle] = &ChunkInfo{
		Length:  0,
		Version: 0,
	}
	log.Infof("[%v] create chunk %v (%p)", cs.address, args.Handle, cs.chunk[args.Handle])
	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	b := make([]byte, args.Length)
	n, err := cs.readChunk(args.Handle, args.Offset, b)
	reply.Data = b[:n]
	reply.Length = n
	// TODO: consider reply.Err
	if err == io.EOF {
		reply.Err = gfs.ReadEOF
		return nil // If an error is returned, the reply parameter will not be sent back to the client.
	}
	return err
}

// applyMutationTo asks secondaries to apply the mutation.
// If any of the secondaries fails, it returns the error.
func (cs *ChunkServer) applyMutationTo(secondaries []gfs.ServerAddress, m gfs.ApplyMutationArg) error {
	log.Infof("[%v] applyMutationTo secondaries: %v", cs.address, secondaries)
	defer log.Infof("[%v] applyMutationTo secondaries %v done", cs.address, secondaries)
	errs := util.CallAll(secondaries, "ChunkServer.RPCApplyMutation", m)
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCWriteChunk is called by client.
// It applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("data %v not found", args.DataID)
	}
	if args.Offset+gfs.Offset(len(data)) > gfs.MaxChunkSize {
		return fmt.Errorf("offset %v exceeds max chunk size", args.Offset)
	}

	// get chunk info
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[args.DataID.Handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", args.DataID.Handle)
	}

	// apply mutation locally
	chunk.lock.Lock()
	defer chunk.lock.Unlock()
	err := cs.writeFile(args.DataID.Handle, args.Offset, data)
	if err != nil {
		return err
	}
	chunk.Length = max(chunk.Length, args.Offset+gfs.Offset(len(data)))

	// ask secondaries to apply mutation
	m := gfs.ApplyMutationArg{
		Mtype:  gfs.MutationWrite,
		DataID: args.DataID,
		Offset: args.Offset,
	}
	return cs.applyMutationTo(args.Secondaries, m)
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("data %v not found", args.DataID)
	}
	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("append size %v exceeds max append size", len(data))
	}

	// get chunk info
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[args.DataID.Handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", args.DataID.Handle)
	}

	chunk.lock.Lock()
	defer chunk.lock.Unlock()
	m := gfs.ApplyMutationArg{
		DataID: args.DataID,
	}
	if chunk.Length+gfs.Offset(len(data)) > gfs.MaxChunkSize {
		// pad current chunk
		log.Infof("[%v] pad chunk %v", cs.address, args.DataID.Handle)
		m.Mtype = gfs.MutationPad
		m.Offset = chunk.Length
		reply.Offset = chunk.Length
		reply.Err = gfs.AppendExceedChunkSize
		// apply mutation locally
		b := make([]byte, gfs.MaxChunkSize-chunk.Length)
		err := cs.writeFile(args.DataID.Handle, m.Offset, b)
		if err != nil {
			return err
		}
		chunk.Length = gfs.MaxChunkSize // update chunk length
		// ask secondaries to apply mutation
		return cs.applyMutationTo(args.Secondaries, m)
	} else {
		// no need to pad, normal append
		m.Mtype = gfs.MutationAppend
		m.Offset = chunk.Length
		reply.Offset = chunk.Length
		reply.Err = gfs.Success
		// apply mutation locall
		err := cs.writeFile(args.DataID.Handle, m.Offset, data)
		if err != nil {
			return err
		}
		chunk.Length += gfs.Offset(len(data)) // update chunk length
		// ask secondaries to apply mutation
		return cs.applyMutationTo(args.Secondaries, m)
	}
}

// RPCApplyWriteChunk is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	var data []byte
	var ok bool
	if args.Mtype == gfs.MutationPad {
		data = make([]byte, gfs.MaxChunkSize-args.Offset)
	} else {
		data, ok = cs.dl.Get(args.DataID)
		if !ok {
			return fmt.Errorf("data %v not found", args.DataID)
		}
	}
	log.Infof("[%v] apply mutation %+v", cs.address, args)
	log.Infof("offset %v, length %v", args.Offset, len(data))
	// get the chunk info
	handle := args.DataID.Handle
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", handle)
	}
	if args.Offset+gfs.Offset(len(data)) > gfs.MaxChunkSize {
		return fmt.Errorf("offset+length exceeds chunk size limit")
	}
	// lock the chunk
	chunk.lock.Lock()
	defer chunk.lock.Unlock()
	// write data to disk
	err := cs.writeFile(handle, args.Offset, data)
	if err != nil {
		return err
	}
	// update chunk info
	chunk.Length = max(chunk.Length, args.Offset+gfs.Offset(len(data)))
	log.Infof("[%v] chunk %v length %v", cs.address, handle, chunk.Length)
	return nil
}

// RPCSendCCopy is called by master. It sends a copy of the chunk to the chunkserver at given address.
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[args.Handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}
	chunk.lock.RLock()
	defer chunk.lock.RUnlock()
	data := make([]byte, chunk.Length)
	_, err := cs.readChunk(args.Handle, 0, data)
	if err != nil {
		return err
	}
	var reply_ gfs.ApplyCopyReply
	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy",
		gfs.ApplyCopyArg{
			Handle:  args.Handle,
			Data:    data,
			Version: chunk.Version}, &reply_)
	return err
}

// RPCSendCCopy is called by another replica.
// It receives the whole copy of the chunk and use it to replace the current chunk.
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[args.Handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}
	err := cs.writeFile(args.Handle, 0, args.Data)
	if err != nil {
		return err
	}
	chunk.lock.Lock()
	chunk.Version = args.Version
	chunk.Length = gfs.Offset(len(args.Data))
	log.Infof("[%v] apply copy: chunk %v (%p) version %v length %v", cs.address, args.Handle, chunk, chunk.Version, chunk.Length)
	chunk.lock.Unlock()
	return nil
}

// readChunk reads the chunk from disk.
// The chunk info is read-locked during the read to ensure the atomicity of the read.
func (cs *ChunkServer) readChunk(handle gfs.ChunkHandle, offset gfs.Offset, b []byte) (n int, err error) {
	cs.chunkLock.RLock()
	_, ok := cs.chunk[handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return 0, fmt.Errorf("chunk %v not found", handle)
	}
	log.Infof("read chunk %v offset %v length %v", handle, offset, len(b))
	// if offset+gfs.Offset(len(b)) > chunk.length {
	// 	return 0, fmt.Errorf("offset+length exceeds chunk length %v", chunk.length)
	// }
	// Should return EOF?
	chunkpath := cs.getChunkPath(handle)
	f, err := os.Open(chunkpath)
	if err != nil {
		log.Warnf("[%v] readChunk: chunk %v error: %v", cs.address, handle, err)
		go cs.selfCheck()
		return 0, err
	}
	defer f.Close()
	n, err = f.ReadAt(b, int64(offset))
	if err != nil && err != io.EOF {
		log.Warnf("[%v] readChunk: chunk %v error: %v", cs.address, handle, err)
		go cs.selfCheck()
	}
	return
}

// getChunkPath returns the path to the chunk file.
func (cs *ChunkServer) getChunkPath(handle gfs.ChunkHandle) string {
	return path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
}

func max(a, b gfs.Offset) gfs.Offset {
	if a > b {
		return a
	}
	return b
}

func (cs *ChunkServer) writeFile(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	chunkpath := cs.getChunkPath(handle)
	f, err := os.OpenFile(chunkpath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Warnf("[%v] writeFile: chunk %v error: %v", cs.address, handle, err)
		go cs.selfCheck()
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(data, int64(offset))
	if err != nil {
		log.Warnf("[%v] writeFile: chunk %v error: %v", cs.address, handle, err)
		go cs.selfCheck()
	}
	return err
}

// selfCheck checks the chunk files and triggers report chunks to master.
// This is called when disk error is detected.
func (cs *ChunkServer) selfCheck() {
	log.Warnf("[%v] self check", cs.address)

	// check server root directory
	info, err := os.Stat(cs.serverRoot)
	if errors.Is(err, os.ErrNotExist) {
		log.Errorf("[%v] root dir not found, creating...", cs.address)
		err := os.Mkdir(cs.serverRoot, 0755)
		if err != nil {
			log.Fatalf("[%v] create root dir error: %v", cs.address, err)
		}
	} else if err != nil {
		log.Errorf("[%v] root dir error: %v", cs.address, err)
	} else if !info.IsDir() {
		log.Fatalf("[%v] root is not a directory", cs.address)
	}

	cs.checkChunkFiles()
	log.Infof("[%v] self check: chunk files done, trigger report chunks", cs.address)
	err = cs.triggerReportChunks()
	if err != nil {
		log.Error("trigger report chunks error: ", err)
	}
	log.Infof("[%v] self check done", cs.address)
}
