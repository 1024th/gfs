package master

import (
	"fmt"
	"gfs"
	"gfs/util"
	"sort"
	"sync"
	"time"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

// GetSortedServers returns a list of chunkservers sorted by the number of chunks they have in ascending order.
func (csm *chunkServerManager) getSortedServers() []gfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()
	l := make([]gfs.ServerAddress, 0, len(csm.servers))
	for addr := range csm.servers {
		l = append(l, addr)
	}
	sort.SliceStable(l, func(i, j int) bool {
		return len(csm.servers[l[i]].chunks) < len(csm.servers[l[j]].chunks)
	})
	return l
}

// Hearbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) {
	csm.Lock()
	defer csm.Unlock()
	server, ok := csm.servers[addr]
	if !ok {
		// new chunkserver
		server = &chunkServerInfo{
			chunks: make(map[gfs.ChunkHandle]bool),
		}
		csm.servers[addr] = server
	}
	server.lastHeartbeat = time.Now()
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	errs := util.CallAll(addrs, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle})
	for _, e := range errs {
		if e != nil {
			// If any error occurs, discard the operation.
			// This may produce some garbage chunks, but they will be garbage collected later.
			return e
		}
	}
	// No error, update chunkserver info
	csm.Lock()
	defer csm.Unlock()
	for _, addr := range addrs {
		csm.servers[addr].chunks[handle] = true
	}
	return nil
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	l := csm.getSortedServers()
	if len(l) < 2 {
		return "", "", fmt.Errorf("not enough chunkservers")
	}
	csm.RLock()
	defer csm.RUnlock()
	// choose the first server that has the chunk
	for _, addr := range l {
		if csm.servers[addr].chunks[handle] {
			from = addr
			break
		}
	}
	if from == "" {
		return "", "", fmt.Errorf("no chunkserver has chunk %v", handle)
	}
	// choose the first servers that do not have the chunk
	for _, addr := range l {
		if !csm.servers[addr].chunks[handle] {
			to = addr
			break
		}
	}
	if to == "" {
		return "", "", fmt.Errorf("no chunkserver does not have chunk %v", handle)
	}
	return from, to, nil
}

// ChooseServers returns servers to store new chunk.
// It is called when a new chunk is created.
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {
	servers := csm.getSortedServers()
	if len(servers) < num {
		return nil, fmt.Errorf("not enough chunkservers")
	}
	return servers[:num], nil
}

// DetectDeadServers detects disconnected chunkservers according to last heartbeat time.
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	csm.Lock()
	defer csm.Unlock()
	now := time.Now()
	var dead []gfs.ServerAddress
	for addr, info := range csm.servers {
		if now.Sub(info.lastHeartbeat) > gfs.ServerTimeout {
			dead = append(dead, addr)
		}
	}
	return dead
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds.
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()
	info, ok := csm.servers[addr]
	if !ok {
		return nil, fmt.Errorf("chunkserver %s not found", addr)
	}
	delete(csm.servers, addr)
	for handle := range info.chunks {
		handles = append(handles, handle)
	}
	return handles, nil
}
