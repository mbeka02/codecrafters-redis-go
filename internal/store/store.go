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
	mu   sync.RWMutex
	data map[string]Value
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]Value),
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
