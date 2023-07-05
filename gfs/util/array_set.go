package util

import (
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
