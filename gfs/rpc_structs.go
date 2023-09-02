package gfs

import (
	"time"
)

//------ ChunkServer

type ReportChunksArg struct{}

type ChunkInfo struct {
	Handle  ChunkHandle
	Length  Offset
	Version ChunkVersion // version number of the chunk in disk
}

type ReportChunksReply struct {
	Chunks []ChunkInfo
}

type CheckVersionArg struct {
	Handle  ChunkHandle
	Version ChunkVersion // chunk version recorded in master
}

type CheckVersionReply struct {
	Version ChunkVersion // chunk version recorded in chunkserver
}

type PushDataAndForwardArg struct {
	Handle    ChunkHandle
	Data      []byte
	ForwardTo []ServerAddress
}
type PushDataAndForwardReply struct {
	DataID DataBufferID
	Err    ErrorCode
}

type ForwardDataArg struct {
	DataID DataBufferID
	Data   []byte
}
type ForwardDataReply struct {
	Err ErrorCode
}

type CreateChunkArg struct {
	Handle ChunkHandle
}
type CreateChunkReply struct {
	Err ErrorCode
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      Offset
	Secondaries []ServerAddress
}
type WriteChunkReply struct {
	Err ErrorCode
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []ServerAddress
}
type AppendChunkReply struct {
	Offset Offset
	Err    ErrorCode
}

type ApplyMutationArg struct {
	Mtype  MutationType
	DataID DataBufferID
	Offset Offset
}
type ApplyMutationReply struct {
	Err ErrorCode
}

type PadChunkArg struct {
	Handle ChunkHandle
}
type PadChunkReply struct {
	Err ErrorCode
}

type ReadChunkArg struct {
	Handle ChunkHandle
	Offset Offset
	Length int
}
type ReadChunkReply struct {
	Data   []byte
	Length int
	Err    ErrorCode
}

type SendCopyArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type SendCopyReply struct {
	Err ErrorCode
}

type ApplyCopyArg struct {
	Handle  ChunkHandle
	Data    []byte
	Version ChunkVersion
}
type ApplyCopyReply struct {
	Err ErrorCode
}

//------ Master

type HeartbeatArg struct {
	Address         ServerAddress // chunkserver address
	LeaseExtensions []ChunkHandle // leases to be extended
	// TODO: report a subset of the chunks that are stored on this chunkserver
	//       so that the master can detect orphaned chunks and do garbage collection
}
type HeartbeatReply struct {
	Garbage []ChunkHandle
}

type TriggerReportChunksArg struct {
	Address ServerAddress
}

type TriggerReportChunksReply struct{}

type GetPrimaryAndSecondariesArg struct {
	Handle ChunkHandle
}
type GetPrimaryAndSecondariesReply struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type ExtendLeaseArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type ExtendLeaseReply struct {
	Expire time.Time
}

type GetReplicasArg struct {
	Handle ChunkHandle
}
type GetReplicasReply struct {
	Locations []ServerAddress
}

type GetFileInfoArg struct {
	Path Path
}
type GetFileInfoReply struct {
	IsDir  bool
	Length int64
	Chunks int64
}

type CreateFileArg struct {
	Path Path
}
type CreateFileReply struct{}

type DeleteFileArg struct {
	Path Path
}
type DeleteFileReply struct{}

type MkdirArg struct {
	Path Path
}
type MkdirReply struct{}

type ListArg struct {
	Path Path
}
type ListReply struct {
	Files []PathInfo
}

type GetChunkHandleArg struct {
	Path  Path
	Index ChunkIndex
}
type GetChunkHandleReply struct {
	Handle ChunkHandle
}
