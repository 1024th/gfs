package client

import (
	"fmt"
	"gfs"
	"gfs/util"
	"io"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	// ReadRetryNum is the maximum number of retries for a read operation.
	ReadRetryNum = 3
	// ReadRetryInterval is the interval between retries for a read operation.
	ReadRetryInterval = gfs.ServerTimeout
	// ModifyRetryNum is the maximum number of retries for a modify operation.
	// (e.g. write, append)
	ModifyRetryNum = 3
	// ModifyRetryInterval is the interval between retries for a modify operation.
	ModifyRetryInterval = gfs.LeaseExpire
	// AppendCreateRetryNum is the maximum number of retries for creating a new chunk
	// for an append operation.
	AppendCreateRetryNum = 2
)

// Client struct is the GFS client-side driver
type Client struct {
	master gfs.ServerAddress
}

// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	return &Client{
		master: master,
	}
}

// Create creates a new file on the specific path on GFS.
func (c *Client) Create(path gfs.Path) error {
	return util.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{Path: path}, nil)
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) error {
	return util.Call(c.master, "Master.RPCMkdir", gfs.MkdirArg{Path: path}, nil)
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	var reply gfs.ListReply
	err := util.Call(c.master, "Master.RPCList", gfs.ListArg{Path: path}, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Files, nil
}

func min(a, b gfs.Offset) gfs.Offset {
	if a < b {
		return a
	}
	return b
}

// Read reads the file at specific offset.
// It reads up to len(data) bytes form the File.
// It return the number of bytes, and an error if any.
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	var file_info gfs.GetFileInfoReply
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &file_info)
	if err != nil {
		return 0, err
	}
	if file_info.IsDir {
		return 0, fmt.Errorf("cannot read a directory")
	}

	end := offset + gfs.Offset(len(data))
	n = 0
	for offset < end {
		chunkIdx := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize
		if chunkIdx >= gfs.ChunkIndex(file_info.Chunks) {
			return 0, fmt.Errorf("offset is out of range")
		}
		chunkHandle, err := c.GetChunkHandle(path, chunkIdx)
		if err != nil {
			return 0, err
		}

		len := int(min(gfs.MaxChunkSize-chunkOffset, end-offset))
		buf := make([]byte, len)
		var read_len int
		for i := 0; i < ReadRetryNum; i++ {
			read_len, err = c.ReadChunk(chunkHandle, chunkOffset, buf)
			if err == nil || err == io.EOF {
				break
			}
			logrus.Warnf("Read error: %v, retrying for %v times", err, ReadRetryNum-i)
			time.Sleep(ReadRetryInterval)
		}
		copy(data[n:], buf[:read_len])
		n += read_len
		if err != nil {
			return n, err
		}
		if read_len != len {
			return n, fmt.Errorf("read length mismatch")
		}
		offset += gfs.Offset(read_len)
	}
	return n, nil
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	var file_info gfs.GetFileInfoReply
	err := util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &file_info)
	if err != nil {
		return err
	}
	if file_info.IsDir {
		return fmt.Errorf("cannot write a directory")
	}

	end := offset + gfs.Offset(len(data))
	logrus.Infof("Writing %v bytes to file %v at offset %v", len(data), path, offset)
	cur_offset := offset
	for cur_offset < end {
		chunkIdx := gfs.ChunkIndex(cur_offset / gfs.MaxChunkSize)
		chunkOffset := cur_offset % gfs.MaxChunkSize
		chunkHandle, err := c.GetChunkHandle(path, chunkIdx)
		if err != nil {
			return err
		}

		len := min(gfs.MaxChunkSize-chunkOffset, end-cur_offset)
		buf := data[cur_offset-offset : cur_offset-offset+len]
		for i := 0; i < ModifyRetryNum; i++ {
			err = c.WriteChunk(chunkHandle, chunkOffset, buf)
			if err == nil {
				break
			}
			time.Sleep(ModifyRetryInterval)
		}
		if err != nil {
			return err
		}
		cur_offset += len
	}
	return nil
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	var file_info gfs.GetFileInfoReply
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &file_info)
	if err != nil {
		return 0, err
	}
	if file_info.IsDir {
		return 0, fmt.Errorf("cannot append to a directory")
	}

	var chunkIdx gfs.ChunkIndex
	var chunkOffset gfs.Offset
	if file_info.Chunks == 0 {
		chunkIdx = 0
	} else {
		chunkIdx = gfs.ChunkIndex(file_info.Chunks - 1)
	}
	maxIdx := chunkIdx + gfs.ChunkIndex(AppendCreateRetryNum)
	for chunkIdx <= maxIdx {
		// If the append causes the last chunk to exceed the max chunk size,
		// create a new chunk and append to it.
		chunkHandle, err := c.GetChunkHandle(path, chunkIdx)
		if err != nil {
			return 0, err
		}
		for i := 0; i < ModifyRetryNum; i++ {
			chunkOffset, err = c.AppendChunk(chunkHandle, data)
			if err == nil || gfs.GetErrorCode(err) == gfs.AppendExceedChunkSize {
				break
			}
			logrus.Warnf("Append error: %v, retrying for %v times", err, ModifyRetryNum-i)
			time.Sleep(ModifyRetryInterval)
		}
		offset = gfs.Offset(chunkIdx)*gfs.MaxChunkSize + chunkOffset
		if err == nil || gfs.GetErrorCode(err) != gfs.AppendExceedChunkSize {
			return offset, err
		}
		// Error is AppendExceedChunkSize, create a new chunk and retry.
		chunkIdx++
		logrus.Warnf("Appending to a new chunk %v of file %v", chunkIdx, path)
	}
	return 0, fmt.Errorf("Append failed after %v retries of creating new chunks", AppendCreateRetryNum)
}

// chooseServer randomly chooses a server from the list.
// Note: In the original GFS paper, the client will choose the closest server.
func chooseServer(servers []gfs.ServerAddress) (index int, server gfs.ServerAddress) {
	index = rand.Intn(len(servers))
	return index, servers[index]
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one,
// master will create a new chunk and return its handle.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	// TODO: cache chunk handle using the file name and chunk index as the key.
	var reply gfs.GetChunkHandleReply
	err := util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{Path: path, Index: index}, &reply)
	if err != nil {
		return 0, err
	}
	return reply.Handle, nil
}

// getReplicas returns the locations of the chunk replicas.
func (c *Client) getReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	// TODO: cache chunk replicas
	var reply gfs.GetReplicasReply
	err := util.Call(c.master, "Master.RPCGetReplicas", gfs.GetReplicasArg{Handle: handle}, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Locations, nil
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset  should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	l, err := c.getReplicas(handle)
	if err != nil {
		return 0, err
	}
	_, server := chooseServer(l)
	var reply gfs.ReadChunkReply
	err = util.Call(server, "ChunkServer.RPCReadChunk", gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: len(data)}, &reply)
	if err != nil {
		return 0, err
	}
	copy(data, reply.Data)
	if reply.Err == gfs.ReadEOF {
		err = io.EOF
	}
	return reply.Length, err
}

// getPrimaryAndSecondaries returns the primary and secondary chunk servers of the chunk.
func (c *Client) getPrimaryAndSecondaries(handle gfs.ChunkHandle) (primary gfs.ServerAddress, secondaries []gfs.ServerAddress, err error) {
	// TODO: cache
	var reply gfs.GetPrimaryAndSecondariesReply
	err = util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{Handle: handle}, &reply)
	if err != nil {
		return "", nil, err
	}
	return reply.Primary, reply.Secondaries, nil
}

func (c *Client) pushData(servers []gfs.ServerAddress, handle gfs.ChunkHandle, data []byte, reply *gfs.PushDataAndForwardReply,
) error {
	server_index, server := chooseServer(servers)
	forward_to := append(servers[:server_index], servers[server_index+1:]...)
	args := gfs.PushDataAndForwardArg{
		Handle:    handle,
		Data:      data,
		ForwardTo: forward_to,
	}
	err := util.Call(server, "ChunkServer.RPCPushDataAndForward", args, &reply)
	if err != nil {
		logrus.Warnf("PushDataAndForward error: %v", err)
		return err
	}
	return nil
}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	primary, secondaries, err := c.getPrimaryAndSecondaries(handle)
	if err != nil {
		return err
	}
	// push data to all the replicas
	var reply gfs.PushDataAndForwardReply
	replicas := append([]gfs.ServerAddress{primary}, secondaries...)
	c.pushData(replicas, handle, data, &reply)
	// send the write request to the primary
	var write_reply gfs.WriteChunkReply
	err = util.Call(primary, "ChunkServer.RPCWriteChunk",
		gfs.WriteChunkArg{DataID: reply.DataID, Offset: offset, Secondaries: secondaries}, &write_reply)
	if err != nil {
		return err
	}
	return nil
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	if len(data) > gfs.MaxAppendSize {
		return 0, fmt.Errorf("append size exceeds the limit")
	}
	primary, secondaries, err := c.getPrimaryAndSecondaries(handle)
	if err != nil {
		return 0, err
	}
	// push data to all the replicas
	var reply gfs.PushDataAndForwardReply
	replicas := append([]gfs.ServerAddress{primary}, secondaries...)
	c.pushData(replicas, handle, data, &reply)
	// send the append request to the primary
	var append_reply gfs.AppendChunkReply
	err = util.Call(primary, "ChunkServer.RPCAppendChunk",
		gfs.AppendChunkArg{DataID: reply.DataID, Secondaries: secondaries}, &append_reply)
	if err != nil {
		return 0, err
	}
	offset = append_reply.Offset
	if append_reply.Err != gfs.Success {
		return offset, gfs.Error{Code: append_reply.Err, Err: "append error"}
	}
	return offset, nil
}
