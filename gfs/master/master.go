package master

import (
	"fmt"
	"gfs/util"
	"net"
	"net/rpc"
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
				log.Fatal("accept error:", err)
				log.Exit(1)
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
				log.Fatal("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	// Dead chunkserver handling
	dead := m.csm.DetectDeadServers()
	for _, addr := range dead {
		log.Warnf("Chunkserver %v is dead", addr)
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
	m.csm.Heartbeat(args.Address)
	for _, handle := range args.LeaseExtensions {
		err := m.cm.ExtendLease(handle, args.Address)
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	p, err := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}
	reply.Primary = p.primary
	reply.Expire = p.expire
	reply.Secondaries = p.secondaries
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
	reply.Length = info.Length
	reply.Chunks = info.Chunks
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	// info, err := m.nm.GetPathInfo(args.Path)
	// We may need to modify the nsTree, so we use low-level function directly
	return m.nm.withRLock(args.Path, func(n *nsTree) error {
		n.RLock()
		defer n.RUnlock()
		if n.isDir {
			return fmt.Errorf("%v is a directory", args.Path)
		}
		if args.Index < 0 || args.Index > gfs.ChunkIndex(n.chunks) {
			return fmt.Errorf("index %v is out of range", args.Index)
		}
		if args.Index == gfs.ChunkIndex(n.chunks) {
			// create a new chunk
			log.Infof("Creating a new chunk for %v", args.Path)
			addrs, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
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
			n.RUnlock()
			n.Lock()
			n.chunks++
			n.Unlock()
			n.RLock()
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
