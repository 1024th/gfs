package chunkserver

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"

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
	chunk                  map[gfs.ChunkHandle]*chunkInfo  // chunk information
	chunkLock              sync.RWMutex
}

type Mutation struct {
	mtype  gfs.MutationType
	data   []byte
	offset gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length        gfs.Offset
	version       gfs.ChunkVersion // version number of the chunk in disk
	newestVersion gfs.ChunkVersion // allocated newest version number
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
		chunk:                  make(map[gfs.ChunkHandle]*chunkInfo),
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
			le := cs.pendingLeaseExtensions.GetAllAndClear()
			args := &gfs.HeartbeatArg{
				Address:         addr,
				LeaseExtensions: le,
			}
			if err := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); err != nil {
				log.Fatal("heartbeat rpc error ", err)
				log.Exit(1)
			}

			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
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
	cs.chunk[args.Handle] = &chunkInfo{
		length:        0,
		version:       0,
		newestVersion: 0,
	}
	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	b := make([]byte, args.Length)
	n, err := cs.readChunk(args.Handle, args.Offset, b)
	reply.Data = b[:n]
	reply.Length = n
	// TODO: consider reply.Err
	return err
}

// RPCWriteChunk is called by client.
// It applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	b, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("data %v not found", args.DataID)
	}
	if args.Offset+gfs.Offset(len(b)) > gfs.MaxChunkSize {
		return fmt.Errorf("offset %v exceeds chunk size limit %v", args.Offset, gfs.MaxChunkSize)
	}
	// apply mutation locally
	m := &Mutation{
		mtype:  gfs.MutationWrite,
		data:   b,
		offset: args.Offset,
	}
	err := cs.applyMutation(args.DataID.Handle, m)
	if err != nil {
		return err
	}
	// ask secondaries to apply mutation
	errs := util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", gfs.ApplyMutationArg{
		Mtype: gfs.MutationWrite, DataID: args.DataID, Offset: args.Offset,
	})
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	return nil
}

// RPCApplyWriteChunk is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("data %v not found", args.DataID)
	}
	m := &Mutation{
		mtype:  args.Mtype,
		data:   data,
		offset: args.Offset,
	}
	err := cs.applyMutation(args.DataID.Handle, m)
	return err
}

// RPCSendCCopy is called by master. It sends a copy of the chunk to the chunkserver at given address.
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[args.Handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}
	data := make([]byte, chunk.length)
	_, err := cs.readChunk(args.Handle, 0, data)
	if err != nil {
		return err
	}
	var reply_ gfs.ApplyCopyReply
	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy",
		gfs.ApplyCopyArg{Handle: args.Handle, Data: data, Version: chunk.version}, &reply_)
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
	err := cs.applyMutation(args.Handle, &Mutation{
		mtype:  gfs.MutationWrite,
		data:   args.Data,
		offset: 0,
	})
	if err != nil {
		return err
	}
	chunk.Lock()
	chunk.version = args.Version
	chunk.Unlock()
	return nil
}

// readChunk reads the chunk from disk.
// The chunk info is read-locked during the read to ensure the atomicity of the read.
func (cs *ChunkServer) readChunk(handle gfs.ChunkHandle, offset gfs.Offset, b []byte) (n int, err error) {
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return 0, fmt.Errorf("chunk %v not found", handle)
	}
	if offset+gfs.Offset(len(b)) >= chunk.length {
		return 0, fmt.Errorf("offset+length exceeds chunk length")
	}
	chunkpath := path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
	f, err := os.Open(chunkpath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err = f.ReadAt(b, int64(offset))
	return
}

// applyMutation applies a mutation to the chunk locally.
// The chunk info is locked during the mutation to ensure the atomicity of the mutation.
func (cs *ChunkServer) applyMutation(handle gfs.ChunkHandle, m *Mutation) error {
	// get the chunk info
	cs.chunkLock.RLock()
	chunk, ok := cs.chunk[handle]
	cs.chunkLock.RUnlock()
	if !ok {
		return fmt.Errorf("chunk %v not found", handle)
	}
	if m.offset+gfs.Offset(len(m.data)) >= gfs.MaxChunkSize {
		return fmt.Errorf("offset+length exceeds chunk size limit")
	}
	// lock the chunk
	chunk.Lock()
	defer chunk.Unlock()
	// write data to disk
	chunkpath := path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
	f, err := os.OpenFile(chunkpath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	switch m.mtype {
	case gfs.MutationPad:
		m.data = make([]byte, gfs.MaxChunkSize-m.offset)
		fallthrough
	case gfs.MutationAppend:
		m.offset = chunk.length
	}
	_, err = f.WriteAt(m.data, int64(m.offset))
	if err != nil {
		return err
	}
	// update chunk info
	chunk.length = m.offset + gfs.Offset(len(m.data))
	return nil
}
