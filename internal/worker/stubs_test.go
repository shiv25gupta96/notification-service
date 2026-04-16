package worker

import (
	"context"
	"sync"
	"time"
)

// memCache is a thread-safe in-memory implementation of workerCache for tests.
type memCache struct {
	mu   sync.Mutex
	data map[string]string
}

func newMemRedis() *memCache {
	return &memCache{data: make(map[string]string)}
}

func (m *memCache) Get(_ context.Context, key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok
}

func (m *memCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	m.mu.Lock()
	m.data[key] = value
	m.mu.Unlock()
	return nil
}
