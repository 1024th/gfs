package util

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
)

// ArraySet is a set implemented using array. I suppose it'll provide better
// performance than golang builtin map when the set is really really small.
// It is thread-safe since a mutex is used.
type ArraySet[T comparable] struct {
	arr  []T
	lock sync.RWMutex
}

func (s *ArraySet[T]) GobEncode() ([]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	w := new(bytes.Buffer)
	gob.NewEncoder(w).Encode(s.arr)
	return w.Bytes(), nil
}

func (s *ArraySet[T]) GobDecode(buf []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	r := bytes.NewBuffer(buf)
	err := gob.NewDecoder(r).Decode(&s.arr)
	return err
}

// Add adds an element to the set.
func (s *ArraySet[T]) Add(element T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, v := range s.arr {
		if v == element {
			return
		}
	}
	s.arr = append(s.arr, element)
}

// AddAll adds all elements of a slice to the set.
func (s *ArraySet[T]) AddAll(elements []T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, element := range elements {
		for _, v := range s.arr {
			if v == element {
				goto next
			}
		}
		s.arr = append(s.arr, element)
	next:
	}
}

// Delete delete an element in the set.
func (s *ArraySet[T]) Delete(element T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, v := range s.arr {
		if v == element {
			s.arr = append(s.arr[:i], s.arr[i+1:]...)
			break
		}
	}
}

// Clear clears the set.
func (s *ArraySet[T]) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.arr = make([]T, 0)
}

// Size returns the size of the set.
func (s *ArraySet[T]) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.arr)
}

// RandomPick picks a random element from the set.
func (s *ArraySet[T]) RandomPick() T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.arr[rand.Intn(len(s.arr))]
}

// GetAll returns all elements of the set.
func (s *ArraySet[T]) GetAll() []T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return append([]T(nil), s.arr...)
}

// GetAllAndClear returns all elements of the set.
func (s *ArraySet[T]) GetAllAndClear() []T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	old := s.arr
	s.arr = make([]T, 0)
	return old
}
