package master

import (
	"encoding/gob"
	"fmt"
	"gfs/util"
	"net"
	"net/rpc"
	"os"
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress // master server address
	serverRoot string            // path to metadata storage
	l          net.Listener
	shutdown   chan struct{}

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

type Metadata struct {
	NsRoot         *NsTree
	Chunks         *map[gfs.ChunkHandle]*ChunkInfo
	Files          *map[gfs.Path]*FileChunks
	NumChunkHandle gfs.ChunkHandle
}

const (
	metadataFilename = "master.meta"
)

func nsTreeToString(n *NsTree, indent int) string {
	s := strings.Repeat("  ", indent)
	s += fmt.Sprintf("%v: %v, %v\n", n.Name, n.IsDir, n.Chunks)
	for _, child := range n.Children {
		s += nsTreeToString(child, indent+1)
	}
	return s
}

// saveMetadata saves the metadata to disk
func (m *Master) saveMetadata() error {
	metadata := Metadata{
		NsRoot:         m.nm.root,
		Chunks:         &m.cm.chunk,
		Files:          &m.cm.file,
		NumChunkHandle: m.cm.numChunkHandle,
	}
	name := path.Join(m.serverRoot, metadataFilename)
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	err = enc.Encode(metadata)
	log.Infof("Saved metadata: %v", metadata)
	log.Infof("Namespace tree: \n%v", nsTreeToString(metadata.NsRoot, 0))
	return err
}

// loadMetadata loads the metadata from disk
func (m *Master) loadMetadata() error {
	var metadata Metadata
	name := path.Join(m.serverRoot, metadataFilename)
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	err = dec.Decode(&metadata)
	if err != nil {
		return err
	}
	log.Infof("Loaded metadata: %v", metadata)
	log.Infof("Namespace tree: \n%v", nsTreeToString(metadata.NsRoot, 0))
	m.nm.root = metadata.NsRoot
	m.cm.chunk = *metadata.Chunks
	m.cm.file = *metadata.Files
	m.cm.numChunkHandle = metadata.NumChunkHandle
	return nil
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
	}

	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()

	err := m.loadMetadata()
	if err != nil {
		log.Warnf("Failed to load metadata: %v", err)
	}

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Errorf("Master accept error: %v", err)
			}
		}
	}()

	// Background Task
	go func() {
		ticker := time.NewTicker(gfs.BackgroundInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			<-ticker.C

			err := m.BackgroundActivity()
			if err != nil {
				log.Error("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
	err := m.l.Close()
	if err != nil {
		log.Errorf("Failed to close listener: %v", err)
	}
	err = m.saveMetadata()
	if err != nil {
		log.Errorf("Failed to save metadata: %v", err)
	}
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	// Dead chunkserver handling
	dead := m.csm.DetectDeadServers()
	for _, addr := range dead {
		log.Warnf("Chunkserver %v is dead", addr)
		err := m.RemoveServer(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// RemoveServer removes a chunkserver. It removes related metadata and performs
// re-replication if necessary.
func (m *Master) RemoveServer(addr gfs.ServerAddress) error {
	handles, err := m.csm.RemoveServer(addr)
	if err != nil {
		return err
	}
	for _, handle := range handles {
		n, err := m.cm.RemoveReplica(handle, addr)
		if err != nil {
			return err
		}
		if n < gfs.MinimumNumReplicas {
			err := m.ReReplicate(handle)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ReReplicate is called when a chunk has less than MinimumNumReplicas replicas.
// It chooses chunkservers and performs re-replication.
func (m *Master) ReReplicate(handle gfs.ChunkHandle) error {
	from, to, err := m.csm.ChooseReReplication(handle)
	if err != nil {
		return err
	}
	log.Infof("Re-replicating chunk %v from %v to %v", handle, from, to)
	err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, nil)
	if err != nil {
		return err
	}
	err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, nil)
	if err != nil {
		return err
	}
	err = m.cm.RegisterReplica(handle, to)
	if err != nil {
		return err
	}
	err = m.csm.AddChunk([]gfs.ServerAddress{to}, handle)
	if err != nil {
		return err
	}
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	isNew := m.csm.Heartbeat(args.Address)
	for _, handle := range args.LeaseExtensions {
		err := m.cm.ExtendLease(handle, args.Address)
		if err != nil {
			return err
		}
	}
	if isNew {
		err := m.askChunkserverForChunks(args.Address)
		if err != nil {
			return err
		}
	}
	reply.Garbage = m.csm.GetGarbage(args.Address)
	m.csm.ClearGarbage(args.Address)
	return nil
}

// askChunkserverForChunks asks a chunkserver to report all the chunks it has
// and register them to the master.
func (m *Master) askChunkserverForChunks(addr gfs.ServerAddress) error {
	var reply gfs.ReportChunksReply
	err := util.Call(addr, "ChunkServer.RPCReportChunks", gfs.ReportChunksArg{}, &reply)
	if err != nil {
		return err
	}
	for _, info := range reply.Chunks {
		err := m.cm.RegisterReplica(info.Handle, addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCTriggerReportChunks is called by chunkserver. It means that the master
// should clear the chunkserver's chunk list and ask it to report all the
// chunks it has.
func (m *Master) RPCTriggerReportChunks(args gfs.TriggerReportChunksArg, reply *gfs.TriggerReportChunksReply) error {
	log.Infof("%v TriggerReportChunks", args.Address)
	err := m.RemoveServer(args.Address)
	if err != nil {
		return err
	}
	log.Infof("%v TriggerReportChunks, After RemoveServer", args.Address)
	m.csm.Heartbeat(args.Address)
	err = m.askChunkserverForChunks(args.Address)
	if err != nil {
		return err
	}
	log.Infof("After TriggerReportChunks, chunkserver %v has: %v", args.Address, m.csm.servers[args.Address].chunks)
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	p, stale, err := m.cm.GetLeaseHolder(args.Handle)
	for _, addr := range stale {
		m.csm.AddGarbage(addr, args.Handle)
	}
	if err != nil {
		return err
	}
	reply.Primary = p.primary
	reply.Expire = p.expire
	reply.Secondaries = p.secondaries
	log.Infof("RPCGetPrimaryAndSecondaries of %v: %v", args.Handle, *reply)
	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	l, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	reply.Locations = l.GetAll()
	return nil
}

// RPCList is called by client to list all files under a directory
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	infos, err := m.nm.List(args.Path)
	if err != nil {
		return err
	}
	reply.Files = infos
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	return m.nm.Create(args.Path)
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	return m.nm.Mkdir(args.Path)
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	info, err := m.nm.GetPathInfo(args.Path)
	if err != nil {
		return err
	}
	reply.IsDir = info.IsDir
	reply.Chunks = info.Chunks
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	// info, err := m.nm.GetPathInfo(args.Path)
	// We may need to modify the nsTree, so we use low-level function directly
	return m.nm.withRLock(args.Path, func(n *NsTree) error {
		n.lock.RLock()
		defer n.lock.RUnlock()
		if n.IsDir {
			return fmt.Errorf("%v is a directory", args.Path)
		}
		if args.Index < 0 || args.Index > gfs.ChunkIndex(n.Chunks) {
			return fmt.Errorf("index %v is out of range", args.Index)
		}
		if args.Index == gfs.ChunkIndex(n.Chunks) {
			// create a new chunk
			log.Infof("Creating a new chunk for %v", args.Path)
			addrs, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
			log.Infof("Chosen servers: %v", addrs)
			if err != nil {
				return err
			}
			handle, err := m.cm.CreateChunk(args.Path, addrs)
			if err != nil {
				return err
			}
			err = m.csm.AddChunk(addrs, handle)
			if err != nil {
				return err
			}
			// update the number of chunks, be careful about the lock order
			n.lock.RUnlock()
			n.lock.Lock()
			n.Chunks++
			n.lock.Unlock()
			n.lock.RLock()
			reply.Handle = handle
		} else {
			handle, err := m.cm.GetChunk(args.Path, args.Index)
			if err != nil {
				return err
			}
			reply.Handle = handle
		}
		return nil
	})
}
