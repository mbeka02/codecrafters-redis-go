package store

import (
	"sync"
	"time"
)

type Value struct {
	Data      string
	List      []string // used for LPUSH,LRANGE and RPUSH
	Stream    []StreamEntry
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
