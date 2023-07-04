package master

import (
	"fmt"
	"gfs"
	"sync"
)

type namespaceManager struct {
	root *nsTree
}

type nsTree struct {
	sync.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	length int64
	chunks int64
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{isDir: true,
			children: make(map[string]*nsTree)},
	}
	return nm
}

// withRLock locks all the parent directories of path p for reading, and
// calls f on the last node without lock. You should lock the last node
// inside f if necessary.
// If any of the parent directories does not exist, it returns an error.
// Otherwise, it returns the error returned by f.
func (nm *namespaceManager) withRLock(p gfs.Path, f func(*nsTree) error) error {
	parts := p.SplitList()
	nodes := make([]*nsTree, len(parts)+1)
	nodes[0] = nm.root
	for i := 0; i < len(parts); i++ {
		nodes[i].RLock()
		defer nodes[i].RUnlock()
		var ok bool
		nodes[i+1], ok = nodes[i].children[parts[i]]
		if !ok {
			return fmt.Errorf("directory %v not found", parts[i])
		}
	}
	return f(nodes[len(parts)])
}

// GetPathInfo returns the information of a file or directory.
// If the file or directory does not exist, it returns an error.
func (nm *namespaceManager) GetPathInfo(p gfs.Path) (gfs.PathInfo, error) {
	var info gfs.PathInfo
	nm.withRLock(p, func(n *nsTree) error {
		n.RLock()
		defer n.RUnlock()
		info.IsDir = n.isDir
		info.Length = n.length
		info.Chunks = n.chunks
		return nil
	})
	return info, nil
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	dir, file := p.Split()
	return nm.withRLock(dir, func(n *nsTree) error {
		n.Lock()
		defer n.Unlock()
		if _, ok := n.children[file]; ok {
			return fmt.Errorf("file %v already exists", file)
		}
		n.children[file] = &nsTree{isDir: false, length: 0, chunks: 0}
		return nil
	})
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	dir, dir_ := p.Split()
	return nm.withRLock(dir, func(n *nsTree) error {
		n.Lock()
		defer n.Unlock()
		if _, ok := n.children[dir_]; ok {
			return fmt.Errorf("directory %v already exists", dir_)
		}
		n.children[dir_] = &nsTree{isDir: true, children: make(map[string]*nsTree)}
		return nil
	})
}
