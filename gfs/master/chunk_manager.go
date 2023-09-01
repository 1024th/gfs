package master

import (
	"fmt"
	"gfs"
	"gfs/util"

	// "sync"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	log "github.com/sirupsen/logrus"
)

// chunkManager manages chunks, including:
//   - Chunk handle allocation, file path to chunk handles mapping,
//   - Chunk replica location management, and
//   - Chunk lease management.
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*ChunkInfo
	file  map[gfs.Path]*FileChunks

	numChunkHandle gfs.ChunkHandle
}

type ServerSet = util.ArraySet[gfs.ServerAddress]

type ChunkInfo struct {
	lock     sync.RWMutex
	Location ServerSet         // set of replica locations
	Primary  gfs.ServerAddress // primary chunkserver
	Expire   time.Time         // lease expire time
	Version  gfs.ChunkVersion
	Path     gfs.Path
}

type FileChunks struct {
	Handles []gfs.ChunkHandle
}

type lease struct {
	primary     gfs.ServerAddress
	expire      time.Time
	secondaries []gfs.ServerAddress
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*ChunkInfo),
		file:  make(map[gfs.Path]*FileChunks),
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
	chunk_info.lock.Lock()
	defer chunk_info.lock.Unlock()
	chunk_info.Location.Add(addr)
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
	chunk_info.lock.Lock()
	defer chunk_info.lock.Unlock()
	chunk_info.Location.Delete(addr)
	return chunk_info.Location.Size(), nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (*ServerSet, error) {
	cm.RLock()
	defer cm.RUnlock()
	chunk_info, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", handle)
	}
	chunk_info.lock.RLock()
	defer chunk_info.lock.RUnlock()
	return &chunk_info.Location, nil // TODO: copy?
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
	if index < 0 || index >= gfs.ChunkIndex(len(file_info.Handles)) {
		return 0, fmt.Errorf("chunk index %v out of range", index)
	}
	return file_info.Handles[index], nil
}

// checkVersion checks if the chunk version recorded in chunkservers
// matches the version recorded in master. If there exists a replica
// with correct version, update the chunk version and replica locations
// in master. It assumes that the caller holds the write lock of chunk_info.
func (cm *chunkManager) checkVersion(handle gfs.ChunkHandle, chunk_info *ChunkInfo) error {
	loc := chunk_info.Location.GetAll()
	newLoc := ServerSet{}
	var wg sync.WaitGroup
	for _, addr := range loc {
		wg.Add(1)
		go func(addr gfs.ServerAddress) {
			defer wg.Done()
			var reply gfs.CheckVersionReply
			err := util.Call(addr, "ChunkServer.RPCCheckVersion", gfs.CheckVersionArg{
				Handle:  handle,
				Version: chunk_info.Version,
			}, &reply)
			if err != nil {
				log.Errorf("RPCCheckVersion failed: %v", err)
				return
			}
			if reply.Version == chunk_info.Version {
				newLoc.Add(addr)
			} else {
				log.Warnf("chunk version mismatch: %v", addr)
				// TODO: delete chunk (GC)
			}
		}(addr)
	}
	wg.Wait()

	if newLoc.Size() != 0 {
		chunk_info.Version++
		chunk_info.Location.Clear()
		chunk_info.Location.AddAll(newLoc.GetAll())
	} else {
		log.Errorf("no replica with correct version")
		return fmt.Errorf("no replica with correct version")
	}
	return nil
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
	chunk_info.lock.RLock()
	defer chunk_info.lock.RUnlock()
	if chunk_info.Primary == "" ||
		chunk_info.Expire.Before(time.Now()) { // no one has a lease, grant a new one
		chunk_info.lock.RUnlock()
		chunk_info.lock.Lock()
		err := cm.checkVersion(handle, chunk_info)
		if err == nil {
			chunk_info.Primary = chunk_info.Location.RandomPick()
			chunk_info.Expire = time.Now().Add(gfs.LeaseExpire)
		}
		chunk_info.lock.Unlock()
		chunk_info.lock.RLock()
		if err != nil {
			return nil, err
		}
	}
	l := lease{
		primary: chunk_info.Primary,
		expire:  chunk_info.Expire,
	}
	l.secondaries = make([]gfs.ServerAddress, 0, chunk_info.Location.Size()-1)
	for _, addr := range chunk_info.Location.GetAll() {
		if addr != chunk_info.Primary {
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
	chunk_info.lock.Lock()
	defer chunk_info.lock.Unlock()
	if chunk_info.Primary != primary {
		return fmt.Errorf("server %v is not primary", primary)
	}
	chunk_info.Expire = time.Now().Add(gfs.LeaseExpire)
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
		file_info = &FileChunks{
			Handles: make([]gfs.ChunkHandle, 0),
		}
		cm.file[path] = file_info
	}
	chunk_info := &ChunkInfo{
		Primary: "",
		Path:    path,
	}
	for _, addr := range addrs {
		chunk_info.Location.Add(addr)
	}
	handle := cm.numChunkHandle
	cm.chunk[handle] = chunk_info
	file_info.Handles = append(file_info.Handles, handle)
	cm.numChunkHandle++
	return handle, nil
}
