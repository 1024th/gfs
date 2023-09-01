package master

import (
	"fmt"
	"gfs"

	// "sync"

	sync "github.com/sasha-s/go-deadlock"
)

type namespaceManager struct {
	root *NsTree
}

type NsTree struct {
	lock sync.RWMutex

	Name string

	// if it is a directory
	IsDir    bool
	Children map[string]*NsTree

	// if it is a file
	Chunks int64
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &NsTree{
			IsDir:    true,
			Children: make(map[string]*NsTree),
		},
	}
	return nm
}

// withRLock locks all the parent directories of path p for reading, and
// calls f on the last node without lock. You should lock the last node
// inside f if necessary.
// If any of the parent directories does not exist, it returns an error.
// Otherwise, it returns the error returned by f.
func (nm *namespaceManager) withRLock(p gfs.Path, f func(*NsTree) error) error {
	// logrus.Infof("withRLock p: %v", p)
	parts := p.SplitList()
	// logrus.Infof("withRLock parts: %v, len: %v", parts, len(parts))
	nodes := make([]*NsTree, len(parts)+1)
	nodes[0] = nm.root
	for i := 0; i < len(parts); i++ {
		// logrus.Infof("withRLock i: %v, parts[i]: %v", i, parts[i])
		nodes[i].lock.RLock()
		defer nodes[i].lock.RUnlock()
		var ok bool
		nodes[i+1], ok = nodes[i].Children[parts[i]]
		if !ok {
			return fmt.Errorf("%v not found", parts[i])
		}
	}
	return f(nodes[len(parts)])
}

func copy(n *NsTree, info *gfs.PathInfo) {
	info.Name = n.Name
	info.IsDir = n.IsDir
	info.Chunks = n.Chunks
}

// GetPathInfo returns the information of a file or directory.
// If the file or directory does not exist, it returns an error.
func (nm *namespaceManager) GetPathInfo(p gfs.Path) (gfs.PathInfo, error) {
	var info gfs.PathInfo
	err := nm.withRLock(p, func(n *NsTree) error {
		n.lock.RLock()
		defer n.lock.RUnlock()
		copy(n, &info)
		return nil
	})
	return info, err
}

// List lists all the files and directories under a directory.
// If the directory does not exist, it returns an error.
func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	var infos []gfs.PathInfo
	err := nm.withRLock(p, func(n *NsTree) error {
		n.lock.RLock()
		defer n.lock.RUnlock()
		for _, v := range n.Children {
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
	return nm.withRLock(dir, func(n *NsTree) error {
		n.lock.Lock()
		defer n.lock.Unlock()
		if _, ok := n.Children[file]; ok {
			return fmt.Errorf("file %v already exists", file)
		}
		n.Children[file] = &NsTree{Name: file, IsDir: false, Chunks: 0}
		return nil
	})
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	dir, dir_ := p.Split()
	return nm.withRLock(dir, func(n *NsTree) error {
		n.lock.Lock()
		defer n.lock.Unlock()
		if _, ok := n.Children[dir_]; ok {
			return fmt.Errorf("directory %v already exists", dir_)
		}
		n.Children[dir_] = &NsTree{Name: dir_, IsDir: true, Children: make(map[string]*NsTree)}
		return nil
	})
}
