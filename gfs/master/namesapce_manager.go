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

	name string

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
	// logrus.Infof("withRLock p: %v", p)
	parts := p.SplitList()
	// logrus.Infof("withRLock parts: %v, len: %v", parts, len(parts))
	nodes := make([]*nsTree, len(parts)+1)
	nodes[0] = nm.root
	for i := 0; i < len(parts); i++ {
		// logrus.Infof("withRLock i: %v, parts[i]: %v", i, parts[i])
		nodes[i].RLock()
		defer nodes[i].RUnlock()
		var ok bool
		nodes[i+1], ok = nodes[i].children[parts[i]]
		if !ok {
			return fmt.Errorf("%v not found", parts[i])
		}
	}
	return f(nodes[len(parts)])
}

func copy(n *nsTree, info *gfs.PathInfo) {
	info.Name = n.name
	info.IsDir = n.isDir
	info.Length = n.length
	info.Chunks = n.chunks
}

// GetPathInfo returns the information of a file or directory.
// If the file or directory does not exist, it returns an error.
func (nm *namespaceManager) GetPathInfo(p gfs.Path) (gfs.PathInfo, error) {
	var info gfs.PathInfo
	err := nm.withRLock(p, func(n *nsTree) error {
		n.RLock()
		defer n.RUnlock()
		copy(n, &info)
		return nil
	})
	return info, err
}

// List lists all the files and directories under a directory.
// If the directory does not exist, it returns an error.
func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	var infos []gfs.PathInfo
	err := nm.withRLock(p, func(n *nsTree) error {
		n.RLock()
		defer n.RUnlock()
		for _, v := range n.children {
			var info gfs.PathInfo
			copy(v, &info)
			infos = append(infos, info)
		}
		return nil
	})
	return infos, err
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
		n.children[file] = &nsTree{name: file, isDir: false, length: 0, chunks: 0}
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
		n.children[dir_] = &nsTree{name: dir_, isDir: true, children: make(map[string]*nsTree)}
		return nil
	})
}
