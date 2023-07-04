package util

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"

	"gfs"
)

// Call is RPC call helper
func Call(srv gfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}

// CallAll applies the rpc call to all destinations.
func CallAll(dst []gfs.ServerAddress, rpcname string, args interface{}) []error {
	errs := make([]error, len(dst))
	var wg sync.WaitGroup
	wg.Add(len(dst))
	for i, d := range dst {
		go func(addr gfs.ServerAddress, err *error) {
			defer wg.Done()
			*err = Call(addr, rpcname, args, nil)
		}(d, &errs[i])
	}
	wg.Wait()
	return errs
}

// Sample randomly chooses k elements from {0, 1, ..., n-1}.
// n should not be less than k.
func Sample(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}
