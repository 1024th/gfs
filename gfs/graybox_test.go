package gfs_test

import (
	"gfs"
	"gfs/chunkserver"
	"gfs/client"
	"gfs/master"
	"gfs/util"
	"reflect"

	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	//"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	m     *master.Master
	cs    []*chunkserver.ChunkServer
	c     *client.Client
	csAdd []gfs.ServerAddress
	root  string // root of tmp file path
)

const (
	mAdd  = ":7777"
	csNum = 5
	N     = 10
)

/*
 *  TEST SUITE 1 - Basic File Operation
 */

func TestFileDir(t *testing.T) {
	assert := assert.New(t)

	// Test CreateFile
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{"/test1.txt"}, nil))
	assert.NotNil(
		m.RPCCreateFile(gfs.CreateFileArg{"/test1.txt"}, nil),
		"The same file has been created twice.")

	// Test Mkdir
	assert.Nil(m.RPCMkdir(gfs.MkdirArg{"/dir1"}, nil))
	assert.Nil(m.RPCMkdir(gfs.MkdirArg{"/dir2"}, nil))
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{"/file1.txt"}, nil))
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{"/file2.txt"}, nil))
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{"/dir1/file3.txt"}, nil))
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{"/dir1/file4.txt"}, nil))
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{"/dir2/file5.txt"}, nil))

	assert.NotNil(
		m.RPCCreateFile(gfs.CreateFileArg{"/dir2/file5.txt"}, nil),
		"The same file has been created twice.")

	assert.NotNil(m.RPCMkdir(gfs.MkdirArg{"/dir1"}, nil),
		"The same dirctory has been created twice.")

	// Test List
	var l gfs.ListReply
	assert.Nil(m.RPCList(gfs.ListArg{"/"}, &l))
	fn := func(p gfs.PathInfo) string { return p.Name }
	got, ok := toStringSet(l.Files, fn)
	assert.True(ok)
	want := map[string]bool{
		"test1.txt": true,
		"dir1":      true, "dir2": true, "file1.txt": true, "file2.txt": true,
	}
	assert.Equal(want, got, "Error in list root path.")

	assert.Nil(m.RPCList(gfs.ListArg{"/dir1"}, &l))
	got, ok = toStringSet(l.Files, fn)
	assert.True(ok)
	want = map[string]bool{
		"file3.txt": true, "file4.txt": true,
	}
	assert.Equal(want, got, "Error in list /dir1.")

	// Test GetChunkHandle
	path := gfs.Path("/test1.txt")

	var r1, r2 gfs.GetChunkHandleReply
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r1))
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r2))
	assert.Equal(r1.Handle, r2.Handle,
		"Got two different handles of the same chunk.")

	assert.NotNil(
		m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 2}, &r2),
		"Discontinuous chunk should not be created.")
}

// toStringSet converts a list of objects to a set of strings.
// If there are duplicated strings in the list, it returns false.
func toStringSet[T any](l []T, fn func(T) string) (s map[string]bool, ok bool) {
	s = make(map[string]bool)
	for _, t := range l {
		v := fn(t)
		if _, ok := s[v]; ok {
			return nil, false
		}
		s[v] = true
	}
	return s, true
}

func TestWriteReadChunk(t *testing.T) {
	assert := assert.New(t)

	// Test WriteChunk
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestWriteChunk.txt")
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{p}, nil))
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1))
	wg := sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			err := c.WriteChunk(r1.Handle, gfs.Offset(x*2), []byte(fmt.Sprintf("%2d", x)))
			assert.Nil(err)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Test ReadChunk
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1))
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			buf := make([]byte, 2)
			n, err := c.ReadChunk(r1.Handle, gfs.Offset(x*2), buf)
			assert.Nil(err)
			assert.Equal(2, n)
			expected := []byte(fmt.Sprintf("%2d", x))
			assert.Equal(expected, buf)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Test Replica Equality
	var data [][]byte
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1))

	n := checkReplicas(r1.Handle, N*2, t)
	assert.Equal(gfs.DefaultNumReplicas, n,
		"expect %v replicas, got %v", gfs.DefaultNumReplicas, len(data))
}

// check if the content of replicas are the same, returns the number of replicas
func checkReplicas(handle gfs.ChunkHandle, length int, t *testing.T) int {
	assert := assert.New(t)
	var data [][]byte

	// get replicas location from master
	var l gfs.GetReplicasReply
	assert.Nil(m.RPCGetReplicas(gfs.GetReplicasArg{handle}, &l))
	log.Info("Replicas: ", l.Locations)

	// read
	args := gfs.ReadChunkArg{handle, 0, length}
	for _, addr := range l.Locations {
		var r gfs.ReadChunkReply
		err := util.Call(addr, "ChunkServer.RPCReadChunk", args, &r)
		if err == nil {
			data = append(data, r.Data)
			//fmt.Println("find in ", addr)
		} else {
			fmt.Println("not find in ", addr, " error: ", err)
		}
	}

	// check equality
	for i := 1; i < len(data); i++ {
		assert.Equal(data[0], data[i], "Replicas are different.")
	}

	return len(data)
}

func TestAppendChunk(t *testing.T) {
	assert := assert.New(t)
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestAppendChunk.txt")
	assert.Nil(m.RPCCreateFile(gfs.CreateFileArg{p}, nil))
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1))
	expected := make(map[int][]byte)
	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%3d", i))
	}

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			_, err := c.AppendChunk(r1.Handle, expected[x])
			assert.Nil(err)
			wg.Done()
		}(i)
	}
	wg.Wait()

	checkReplicas(r1.Handle, N*3, t)

	for x := 0; x < N; x++ {
		buf := make([]byte, 3)
		n, err := c.ReadChunk(r1.Handle, gfs.Offset(x*3), buf)
		assert.Nil(err)
		assert.Equal(3, n, "should read exactly 3 bytes")

		key := -1
		for k, v := range expected {
			if reflect.DeepEqual(buf, v) {
				key = k
				break
			}
		}
		assert.NotEqual(-1, key, "incorrect data", buf)

		delete(expected, key)
	}

	assert.Equal(0, len(expected), "incorrect data")
}

/*
 *  TEST SUITE 2 - Client API
 */

// if the append would cause the chunk to exceed the maximum size
// this chunk should be pad and the data should be appended to the next chunk
func TestPadOver(t *testing.T) {
	assert := assert.New(t)
	p := gfs.Path("/appendover.txt")

	assert.Nil(c.Create(p))

	bound := gfs.MaxAppendSize - 1
	buf := make([]byte, bound)
	for i := 0; i < bound; i++ {
		buf[i] = byte(i%26 + 'a')
	}

	for i := 0; i < 4; i++ {
		_, err := c.Append(p, buf)
		assert.Nil(err)
	}

	buf = buf[:5]
	// an append cause pad, and client should retry to next chunk
	offset, err := c.Append(p, buf)
	assert.Nil(err)
	assert.Equal(gfs.Offset(gfs.MaxChunkSize), offset, "should append to next chunk")
}

// big data that invokes several chunks
func TestWriteReadBigData(t *testing.T) {
	assert := assert.New(t)
	p := gfs.Path("/bigData.txt")

	assert.Nil(c.Create(p))

	size := gfs.MaxChunkSize * 3
	expected := make([]byte, size)
	for i := 0; i < size; i++ {
		expected[i] = byte(i%26 + 'a')
	}

	// write large data
	assert.Nil(c.Write(p, gfs.MaxChunkSize/2, expected))

	// read
	buf := make([]byte, size)
	n, err := c.Read(p, gfs.MaxChunkSize/2, buf)
	assert.Nil(err)
	assert.Equal(size, n, "should read exactly %v bytes", size)
	assert.Equal(expected, buf, "read wrong data")

	// test read at EOF
	_, err = c.Read(p, gfs.MaxChunkSize/2+gfs.Offset(size), buf)
	assert.NotNil(err, "should return error if read at EOF")

	// test append offset
	var offset gfs.Offset
	buf = buf[:gfs.MaxAppendSize-1]
	offset, err = c.Append(p, buf)
	assert.Nil(err)
	assert.Equal(gfs.MaxChunkSize/2+gfs.Offset(size), offset, "append in wrong offset")
}

type Counter struct {
	sync.Mutex
	ct int
}

func (ct *Counter) Next() int {
	ct.Lock()
	defer ct.Unlock()
	ct.ct++
	return ct.ct - 1
}

// a concurrent producer-consumer number collector for testing race contiditon
func TestComprehensiveOperation(t *testing.T) {
	assert := assert.New(t)
	createTick := 100 * time.Millisecond

	done := make(chan struct{})

	var line []chan gfs.Path
	for i := 0; i < 3; i++ {
		line = append(line, make(chan gfs.Path, N*200))
	}

	// Hard !!
	go func() {
		return
		i := 1
		cs[i-1].Shutdown()
		time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)
		for {
			select {
			case <-done:
				return
			default:
			}
			j := (i - 1 + csNum) % csNum
			jj := strconv.Itoa(j)
			cs[i].Shutdown()
			cs[j] = chunkserver.NewAndServe(csAdd[j], mAdd, path.Join(root, "cs"+jj))
			i = (i + 1) % csNum
			time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)
		}
	}()

	// create
	var wg0 sync.WaitGroup
	var createCt Counter
	wg0.Add(2)
	for i := 0; i < 2; i++ {
		go func(x int) {
			ticker := time.Tick(createTick)
		loop:
			for {
				select {
				case <-done:
					break loop
				case p := <-line[1]: // get back from append
					line[0] <- p
				case <-ticker: // create new file
					x := createCt.Next()
					p := gfs.Path(fmt.Sprintf("/haha%v.txt", x))

					//fmt.Println("create ", p)
					assert.Nil(c.Create(p))
					line[0] <- p
				}
			}
			wg0.Done()
		}(i)
	}

	go func() {
		wg0.Wait()
		close(line[0])
	}()

	// append 0, 1, 2, ..., sendCounter
	var wg1 sync.WaitGroup
	var sendCt Counter
	wg1.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			for p := range line[0] {
				x := sendCt.Next()

				//fmt.Println("append ", p, "  ", tmp)
				_, err := c.Append(p, []byte(fmt.Sprintf("%10d,", x)))
				assert.Nil(err)

				line[1] <- p
				line[2] <- p
			}
			wg1.Done()
		}(i)
	}

	go func() {
		wg1.Wait()
		close(line[2])
	}()

	// read and collect numbers
	var wg2 sync.WaitGroup
	var lock2 sync.RWMutex
	receiveData := make(map[int]int)
	fileOffset := make(map[gfs.Path]int)
	wg2.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			for p := range line[2] {
				lock2.RLock()
				pos := fileOffset[p]
				lock2.RUnlock()
				buf := make([]byte, 10000) // large enough
				n, err := c.Read(p, gfs.Offset(pos), buf)
				assert.True(err == nil || err == io.EOF)

				if n == 0 {
					continue
				}
				buf = buf[:n-1]
				//fmt.Println("read ", p, " at ", pos, " : ", string(buf))

				lock2.Lock()
				for _, v := range strings.Split(string(buf), ",") {
					i, err := strconv.Atoi(strings.TrimSpace(v))
					assert.Nil(err)
					receiveData[i]++
				}
				if pos+n > fileOffset[p] {
					fileOffset[p] = pos + n
				}
				lock2.Unlock()
				time.Sleep(N * time.Millisecond)
			}
			wg2.Done()
		}(i)
	}

	// wait to test race contition
	fmt.Println("###### Continue life for the elder to pass a long time test...")
	for i := 0; i < 6; i++ {
		fmt.Print(" +1s ")
		time.Sleep(time.Second)
	}
	fmt.Println("")
	close(done)
	wg2.Wait()

	// check correctness
	total := sendCt.Next() - 1
	for i := 0; i < total; i++ {
		assert.True(receiveData[i] >= 1, "error occured in sending data %v", i)
	}
	fmt.Printf("##### You send %v numbers in total\n", total)
}

/*
 *  TEST SUITE 3 - Fault Tolerance
 */

// Shutdown two chunk servers during appending
func TestShutdownInAppend(t *testing.T) {
	assert := assert.New(t)
	p := gfs.Path("/shutdown.txt")
	assert.Nil(c.Create(p))

	expected := make(map[int][]byte)
	todelete := make(map[int][]byte)
	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%2d", i))
		todelete[i] = []byte(fmt.Sprintf("%2d", i))
	}

	// get two replica locations
	var r1 gfs.GetChunkHandleReply
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1))
	var l gfs.GetReplicasReply
	assert.Nil(m.RPCGetReplicas(gfs.GetReplicasArg{r1.Handle}, &l))

	for i := 0; i < N; i++ {
		go func(x int) {
			_, err := c.Append(p, expected[x])
			assert.Nil(err)
		}(i)
	}

	time.Sleep(N * time.Millisecond)
	// choose two servers to shutdown during appending
	for i, v := range cs {
		if csAdd[i] == l.Locations[0] || csAdd[i] == l.Locations[1] {
			v.Shutdown()
		}
	}

	// check correctness, append at least once
	// TODO : stricter - check replicas
	for x := 0; x < gfs.MaxChunkSize/2 && len(todelete) > 0; x++ {
		buf := make([]byte, 2)
		n, err := c.Read(p, gfs.Offset(x*2), buf)
		assert.Nil(err, "read error")
		assert.Equal(2, n, "should read exactly 2 bytes")

		key := -1
		for k, v := range expected {
			if reflect.DeepEqual(buf, v) {
				key = k
				break
			}
		}
		assert.NotEqual(-1, key, "incorrect data", buf)

		delete(todelete, key)
	}
	assert.Equal(0, len(todelete), "missing data %v", todelete)

	// restart
	for i := range cs {
		if csAdd[i] == l.Locations[0] || csAdd[i] == l.Locations[1] {
			ii := strconv.Itoa(i)
			cs[i] = chunkserver.NewAndServe(csAdd[i], mAdd, path.Join(root, "cs"+ii))
		}
	}
}

// Shutdown all servers in turns. You should perform re-replication well
func TestReReplication(t *testing.T) {
	assert := assert.New(t)
	p := gfs.Path("/re-replication.txt")

	assert.Nil(c.Create(p))

	c.Append(p, []byte("Dangerous"))

	fmt.Println("###### Mr. Disaster is coming...")
	time.Sleep(gfs.LeaseExpire)

	cs[1].Shutdown()
	cs[2].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[1] = chunkserver.NewAndServe(csAdd[1], mAdd, path.Join(root, "cs1"))
	cs[2] = chunkserver.NewAndServe(csAdd[2], mAdd, path.Join(root, "cs2"))

	cs[3].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[4].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[3] = chunkserver.NewAndServe(csAdd[3], mAdd, path.Join(root, "cs3"))
	cs[4] = chunkserver.NewAndServe(csAdd[4], mAdd, path.Join(root, "cs4"))
	time.Sleep(gfs.ServerTimeout)

	cs[0].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[0] = chunkserver.NewAndServe(csAdd[0], mAdd, path.Join(root, "cs0"))
	time.Sleep(gfs.ServerTimeout)

	// check equality and number of replicas
	var r1 gfs.GetChunkHandleReply
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1))
	n := checkReplicas(r1.Handle, N*2, t)

	assert.True(n >= gfs.MinimumNumReplicas,
		"Cannot perform replicas promptly, only get %v replicas", n)

	fmt.Printf("###### Well done, you save %v replicas during disaster\n", n)
}

// some file operations to check if the server is still working
func checkWork(p gfs.Path, msg []byte, t *testing.T) {
	assert := assert.New(t)

	// append again to confirm all information about this chunk has been reloaded properly
	offset, err := c.Append(p, msg)
	assert.Nil(err)

	// read and check data
	buf := make([]byte, len(msg)*2)
	// read at defined region
	_, err = c.Read(p, offset-gfs.Offset(len(msg)), buf)
	assert.Nil(err)

	msg = append(msg, msg...)
	assert.Equal(msg, buf, "read wrong data")

	// other file operation
	assert.Nil(c.Mkdir(gfs.Path("/" + string(msg))))
	newfile := gfs.Path("/" + string(msg) + "/" + string(msg) + ".txt")
	assert.Nil(c.Create(newfile))
	assert.Nil(c.Write(newfile, 4, msg))

	// read and check data again
	buf = make([]byte, len(msg))
	_, err = c.Read(p, 0, buf)
	assert.Nil(err)
	assert.Equal(msg, buf, "read wrong data")
}

// Shutdown all chunk servers. You must store the meta data of chunkserver persistently
func TestPersistentChunkServer(t *testing.T) {
	assert := assert.New(t)
	p := gfs.Path("/persistent-chunkserver.txt")
	msg := []byte("Don't Lose Me. ")

	assert.Nil(c.Create(p))

	_, err := c.Append(p, msg)
	assert.Nil(err)

	// shut all down
	fmt.Println("###### SHUT All DOWN")
	for _, v := range cs {
		v.Shutdown()
	}
	time.Sleep(2*gfs.ServerTimeout + gfs.LeaseExpire)

	// restart
	for i := 0; i < csNum; i++ {
		ii := strconv.Itoa(i)
		cs[i] = chunkserver.NewAndServe(csAdd[i], mAdd, path.Join(root, "cs"+ii))
	}

	fmt.Println("###### Waiting for Chunk Servers to report their chunks to master...")
	time.Sleep(2*gfs.ServerTimeout + gfs.LeaseExpire)

	// check recovery
	checkWork(p, msg, t)
}

// Shutdown master. You must store the meta data of master persistently
func TestPersistentMaster(t *testing.T) {
	assert := assert.New(t)
	p := gfs.Path("/persistent/master.txt")
	msg := []byte("Don't Lose Yourself. ")

	assert.Nil(c.Mkdir("/persistent"))
	assert.Nil(c.Create(p))

	_, err := c.Append(p, msg)
	assert.Nil(err)

	// shut master down
	fmt.Println("###### Shutdown Master")
	m.Shutdown()
	time.Sleep(2 * gfs.ServerTimeout)

	// restart
	m = master.NewAndServe(mAdd, path.Join(root, "m"))
	time.Sleep(2 * gfs.ServerTimeout)

	// check recovery
	checkWork(p, msg, t)
}

// delete all files in two chunkservers ...
func TestDiskError(t *testing.T) {
	assert := assert.New(t)
	p := gfs.Path("/virus.die")
	msg := []byte("fucking disk!")

	assert.Nil(c.Create(p))

	_, err := c.Append(p, msg)
	assert.Nil(err)

	// get replica locations
	var r1 gfs.GetChunkHandleReply
	assert.Nil(m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1))
	var l gfs.GetReplicasReply
	assert.Nil(m.RPCGetReplicas(gfs.GetReplicasArg{r1.Handle}, &l))

	fmt.Println("###### Destory two chunkserver's diskes")
	// destory two server's disk
	log.Info(l.Locations[:2])
	for i, _ := range cs {
		if csAdd[i] == l.Locations[0] || csAdd[i] == l.Locations[1] {
			ii := strconv.Itoa(i)
			os.RemoveAll(path.Join(root, "cs"+ii))
		}
	}
	fmt.Println("###### Waiting for recovery")
	time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)

	_, err = c.Append(p, msg)
	assert.Nil(err)

	time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)

	// TODO: check re-replication
	// check recovery
	checkWork(p, msg, t)
}

/*
 *  TEST SUITE 4 - Challenge
 */

// todo : simulate an extremely adverse condition

func TestMain(tm *testing.M) {
	// create temporary directory
	var err error
	root, err = os.MkdirTemp("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

	//log.SetLevel(log.FatalLevel)

	// run master
	os.Mkdir(path.Join(root, "m"), 0755)
	m = master.NewAndServe(mAdd, path.Join(root, "m"))

	// run chunkservers
	csAdd = make([]gfs.ServerAddress, csNum)
	cs = make([]*chunkserver.ChunkServer, csNum)
	for i := 0; i < csNum; i++ {
		ii := strconv.Itoa(i)
		os.Mkdir(path.Join(root, "cs"+ii), 0755)
		csAdd[i] = gfs.ServerAddress(fmt.Sprintf(":%v", 10000+i))
		cs[i] = chunkserver.NewAndServe(csAdd[i], mAdd, path.Join(root, "cs"+ii))
	}

	// init client
	c = client.NewClient(mAdd)
	time.Sleep(300 * time.Millisecond)

	// run tests
	ret := tm.Run()

	// shutdown
	for _, v := range cs {
		v.Shutdown()
	}
	m.Shutdown()
	os.RemoveAll(root)

	os.Exit(ret)
}
