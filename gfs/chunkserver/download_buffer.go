package chunkserver

import (
	// "sync"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	"gfs"
)

type downloadItem struct {
	data   []byte
	expire time.Time
}

type downloadBuffer struct {
	sync.RWMutex
	buffer map[gfs.DataBufferID]downloadItem
	expire time.Duration
	tick   time.Duration
}

// newDownloadBuffer returns a downloadBuffer. Default expire time is expire.
// The downloadBuffer will cleanup expired items every tick.
func newDownloadBuffer(expire, tick time.Duration) *downloadBuffer {
	buf := &downloadBuffer{
		buffer: make(map[gfs.DataBufferID]downloadItem),
		expire: expire,
		tick:   tick,
	}

	// cleanup
	go func() {
		for range time.Tick(tick) {
			now := time.Now()
			buf.RLock()
			for id, item := range buf.buffer {
				if item.expire.Before(now) {
					buf.RUnlock()
					buf.Delete(id)
					buf.RLock()
				}
			}
			buf.RUnlock()
		}
	}()

	return buf
}

func (buf *downloadBuffer) Set(id gfs.DataBufferID, data []byte) {
	buf.Lock()
	defer buf.Unlock()
	buf.buffer[id] = downloadItem{data, time.Now().Add(buf.expire)}
}

func (buf *downloadBuffer) Get(id gfs.DataBufferID) ([]byte, bool) {
	buf.Lock()
	defer buf.Unlock()
	item, ok := buf.buffer[id]
	if !ok {
		return nil, ok
	}
	item.expire = time.Now().Add(buf.expire) // touch
	return item.data, ok
}

func (buf *downloadBuffer) Delete(id gfs.DataBufferID) {
	buf.Lock()
	defer buf.Unlock()
	delete(buf.buffer, id)
}
