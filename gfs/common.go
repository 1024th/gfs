package gfs

import (
	"path"
	"strings"
	"time"
)

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64

func (p Path) Split() (dir Path, file string) {
	dir_, file_ := path.Split(string(p))
	return Path(dir_), file_
}

func (p Path) SplitList() []string {
	// return strings.Split(string(p), "/")
	// Note: `strings.Split` will return ["", ""] for "/",
	// so we use `strings.FieldsFunc` instead.
	splitFn := func(c rune) bool {
		return c == '/'
	}
	return strings.FieldsFunc(string(p), splitFn)
}

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int64
}

// allocate a new DataID for given handle
func NewDataID(handle ChunkHandle) DataBufferID {
	now := time.Now()
	timeStamp := now.UnixNano()
	return DataBufferID{Handle: handle, TimeStamp: timeStamp}
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

type MutationType int

const (
	MutationWrite MutationType = iota
	MutationAppend
	MutationPad
)

type ErrorCode int

const (
	Success ErrorCode = iota
	UnknownError
	AppendExceedChunkSize
	WriteExceedChunkSize
	ReadEOF
	NotAvailableForCopy
)

// extended error type with error code
type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}

// system config
const (
	LeaseExpire        = 2 * time.Second //1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	BackgroundInterval = 200 * time.Millisecond //
	ServerTimeout      = 1 * time.Second        //

	MaxChunkSize  = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize = MaxChunkSize / 4

	DefaultNumReplicas = 3
	MinimumNumReplicas = 2

	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 10 * time.Second
)
