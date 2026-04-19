package store

import (
	"sync"
	"time"
)

type Value struct {
	Data      string
	List      []string // used for LPUSH,LRANGE and RPUSH
	ExpiresAt *time.Time
}

type Store struct {
	mu      sync.RWMutex
	data    map[string]Value
	waiters map[string][]chan string
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]Value),
		waiters: make(map[string][]chan string),
	}
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	_, existed := s.data[key]
	delete(s.data, key)
	s.mu.Unlock()
	return existed
}

func (s *Store) Set(key string, value Value) {
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
}

func (s *Store) Get(key string) (Value, bool) {
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return Value{}, false
	}
	// lazy expiration on access , I think this is better than having a cleanup go-routine
	if val.ExpiresAt != nil && time.Now().After(*val.ExpiresAt) {
		s.Delete(key)
		return Value{}, false
	}

	return val, true
}

func (s *Store) RPush(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	list := s.data[key].List
	updated := append(list, values...)
	length := len(updated) // capture length BEFORE notify consumes an element
	updated, _ = s.notifyWaiterIfAny(key, updated)
	s.data[key] = Value{List: updated}
	return length // return pre-notify length
}

func (s *Store) LPush(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	list := s.data[key].List
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
	updated := append(values, list...)
	length := len(updated)
	updated, _ = s.notifyWaiterIfAny(key, updated)
	s.data[key] = Value{List: updated}
	return length
}

func (s *Store) LPop(key string, count int) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list := s.data[key].List
	if len(list) == 0 {
		return nil, false
	}

	if count <= 0 {
		return nil, false
	}

	if count > len(list) {
		count = len(list)
	}

	popped := list[:count]
	remaining := list[count:]

	s.data[key] = Value{List: remaining}

	return popped, true
}

func (s *Store) BLPop(key string, timeout time.Duration) (string, bool) {
	// fast path + waiter registration must be atomic — no gap between
	// "list is empty" and "I'm registered to receive notifications"
	s.mu.Lock()
	list := s.data[key].List
	if len(list) > 0 {
		val := list[0]
		s.data[key] = Value{List: list[1:]}
		s.mu.Unlock()
		return val, true
	}

	// list is empty — register waiter while still holding the lock
	ch := make(chan string, 1)
	s.waiters[key] = append(s.waiters[key], ch)
	s.mu.Unlock()

	if timeout == 0 {
		val := <-ch
		return val, true
	}

	select {
	case val := <-ch:
		return val, true
	case <-time.After(timeout):
		s.removeWaiter(key, ch)
		return "", false
	}
}

func (s *Store) addWaiter(key string, ch chan string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.waiters[key] = append(s.waiters[key], ch)
}

func (s *Store) removeWaiter(key string, target chan string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	waiters := s.waiters[key]
	for i, ch := range waiters {
		if ch == target {
			s.waiters[key] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
}

func (s *Store) notifyWaiterIfAny(key string, list []string) ([]string, bool) {
	waiters := s.waiters[key]
	if len(waiters) == 0 {
		return list, false
	}

	ch := waiters[0]
	s.waiters[key] = waiters[1:]

	val := list[0]
	list = list[1:]

	go func() { ch <- val }()

	return list, true
}
