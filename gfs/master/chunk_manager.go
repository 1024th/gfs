package master

import (
	"fmt"
	"gfs"
	"gfs/util"
	"sync"
	"time"
)

// chunkManager manages chunks, including:
//   - Chunk handle allocation, file path to chunk handles mapping,
//   - Chunk replica location management, and
//   - Chunk lease management.
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	numChunkHandle gfs.ChunkHandle
}

type ServerSet = util.ArraySet[gfs.ServerAddress]

type chunkInfo struct {
	sync.RWMutex
	location ServerSet         // set of replica locations
	primary  gfs.ServerAddress // primary chunkserver
	expire   time.Time         // lease expire time
	path     gfs.Path
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}

type lease struct {
	primary     gfs.ServerAddress
	expire      time.Time
	secondaries []gfs.ServerAddress
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.RLock()
	defer cm.RUnlock()
	chunk_info, ok := cm.chunk[handle]
	if !ok {
		return fmt.Errorf("chunk %v not found", handle)
	}
	chunk_info.Lock()
	defer chunk_info.Unlock()
	chunk_info.location.Add(addr)
	return nil
}

// RemoveReplica removes a replica for a chunk. Returns the number of replicas left.
func (cm *chunkManager) RemoveReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) (int, error) {
	cm.RLock()
	defer cm.RUnlock()
	chunk_info, ok := cm.chunk[handle]
	if !ok {
		return 0, fmt.Errorf("chunk %v not found", handle)
	}
	chunk_info.Lock()
	defer chunk_info.Unlock()
	chunk_info.location.Delete(addr)
	return chunk_info.location.Size(), nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (*ServerSet, error) {
	cm.RLock()
	defer cm.RUnlock()
	chunk_info, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	chunk_info.RLock()
	defer chunk_info.RUnlock()
	return &chunk_info.location, nil // TODO: copy?
}

// GetChunk returns the chunk handle for (path, index).
// If the index exceeds the number of chunks in the file, returns an error.
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	defer cm.RUnlock()
	file_info, ok := cm.file[path]
	if !ok {
		return 0, fmt.Errorf("file %v not found", path)
	}
	if index < 0 || index >= gfs.ChunkIndex(len(file_info.handles)) {
		return 0, fmt.Errorf("chunk index %v out of range", index)
	}
	return file_info.handles[index], nil
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*lease, error) {
	cm.RLock()
	defer cm.RUnlock()
	chunk_info, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	chunk_info.RLock()
	defer chunk_info.RUnlock()
	if chunk_info.primary == "" { // no one has a lease, grant a new one
		chunk_info.RUnlock()
		chunk_info.Lock()
		chunk_info.primary = chunk_info.location.RandomPick()
		chunk_info.expire = time.Now().Add(gfs.LeaseExpire)
		chunk_info.Unlock()
		chunk_info.RLock()
	}
	l := lease{
		primary: chunk_info.primary,
		expire:  chunk_info.expire,
	}
	l.secondaries = make([]gfs.ServerAddress, 0, chunk_info.location.Size()-1)
	for _, addr := range chunk_info.location.GetAll() {
		if addr != chunk_info.primary {
			svr_addr := addr
			l.secondaries = append(l.secondaries, svr_addr)
		}
	}
	return &l, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.RLock()
	defer cm.RUnlock()
	chunk_info, ok := cm.chunk[handle]
	if !ok {
		return fmt.Errorf("chunk %v not found", handle)
	}
	chunk_info.Lock()
	defer chunk_info.Unlock()
	if chunk_info.primary != primary {
		return fmt.Errorf("server %v is not primary", primary)
	}
	chunk_info.expire = time.Now().Add(gfs.LeaseExpire)
	return nil
}

// CreateChunk creates a new chunk for path. The new chunk is appended to the
// end of the file.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, error) {
	cm.Lock()
	defer cm.Unlock()
	file_info, ok := cm.file[path]
	if !ok {
		// create a new file
		file_info = &fileInfo{
			handles: make([]gfs.ChunkHandle, 0),
		}
		cm.file[path] = file_info
	}
	chunk_info := &chunkInfo{
		primary: "",
		path:    path,
	}
	for _, addr := range addrs {
		chunk_info.location.Add(addr)
	}
	handle := cm.numChunkHandle
	cm.chunk[handle] = chunk_info
	file_info.handles = append(file_info.handles, handle)
	cm.numChunkHandle++
	return handle, nil
}
