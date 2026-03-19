package db

import (
	"hash/fnv"
	"strconv"
	"sync"

	"github.com/paerx/anhebridgedb/internal/storage"
)

const mapShardCount = 256

type recordShardMap struct {
	shards [mapShardCount]struct {
		mu    sync.RWMutex
		items map[string]Record
	}
}

type uint64ShardMap struct {
	shards [mapShardCount]struct {
		mu    sync.RWMutex
		items map[string]uint64
	}
}

type keyIndexShardMap struct {
	shards [mapShardCount]struct {
		mu    sync.RWMutex
		items map[string]storage.KeyIndexEntry
	}
}

type eventRefShardMap struct {
	shards [mapShardCount]struct {
		mu    sync.RWMutex
		items map[uint64]storage.EventRef
	}
}

func newRecordShardMap() *recordShardMap {
	m := &recordShardMap{}
	for i := range m.shards {
		m.shards[i].items = map[string]Record{}
	}
	return m
}

func newUint64ShardMap() *uint64ShardMap {
	m := &uint64ShardMap{}
	for i := range m.shards {
		m.shards[i].items = map[string]uint64{}
	}
	return m
}

func newKeyIndexShardMap() *keyIndexShardMap {
	m := &keyIndexShardMap{}
	for i := range m.shards {
		m.shards[i].items = map[string]storage.KeyIndexEntry{}
	}
	return m
}

func newEventRefShardMap() *eventRefShardMap {
	m := &eventRefShardMap{}
	for i := range m.shards {
		m.shards[i].items = map[uint64]storage.EventRef{}
	}
	return m
}

func shardIndexForString(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % mapShardCount)
}

func shardIndexForUint64(id uint64) int {
	return shardIndexForString(strconv.FormatUint(id, 10))
}

func (m *recordShardMap) load(key string) (Record, bool) {
	shard := &m.shards[shardIndexForString(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.items[key]
	return value, ok
}

func (m *recordShardMap) store(key string, value Record) {
	shard := &m.shards[shardIndexForString(key)]
	shard.mu.Lock()
	shard.items[key] = value
	shard.mu.Unlock()
}

func (m *recordShardMap) delete(key string) {
	shard := &m.shards[shardIndexForString(key)]
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

func (m *recordShardMap) rangeAll(fn func(string, Record) bool) {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		stop := false
		for key, value := range shard.items {
			if !fn(key, value) {
				stop = true
				break
			}
		}
		shard.mu.RUnlock()
		if stop {
			return
		}
	}
}

func (m *recordShardMap) count() int {
	total := 0
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		total += len(shard.items)
		shard.mu.RUnlock()
	}
	return total
}

func (m *recordShardMap) shardSizes() []int {
	sizes := make([]int, mapShardCount)
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		sizes[i] = len(shard.items)
		shard.mu.RUnlock()
	}
	return sizes
}

func (m *uint64ShardMap) load(key string) (uint64, bool) {
	shard := &m.shards[shardIndexForString(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.items[key]
	return value, ok
}

func (m *uint64ShardMap) store(key string, value uint64) {
	shard := &m.shards[shardIndexForString(key)]
	shard.mu.Lock()
	shard.items[key] = value
	shard.mu.Unlock()
}

func (m *keyIndexShardMap) load(key string) (storage.KeyIndexEntry, bool) {
	shard := &m.shards[shardIndexForString(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.items[key]
	return value, ok
}

func (m *keyIndexShardMap) store(key string, value storage.KeyIndexEntry) {
	shard := &m.shards[shardIndexForString(key)]
	shard.mu.Lock()
	shard.items[key] = value
	shard.mu.Unlock()
}

func (m *keyIndexShardMap) rangeAll(fn func(string, storage.KeyIndexEntry) bool) {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		stop := false
		for key, value := range shard.items {
			if !fn(key, value) {
				stop = true
				break
			}
		}
		shard.mu.RUnlock()
		if stop {
			return
		}
	}
}

func (m *keyIndexShardMap) count() int {
	total := 0
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		total += len(shard.items)
		shard.mu.RUnlock()
	}
	return total
}

func (m *keyIndexShardMap) shardSizes() []int {
	sizes := make([]int, mapShardCount)
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		sizes[i] = len(shard.items)
		shard.mu.RUnlock()
	}
	return sizes
}

func (m *eventRefShardMap) load(id uint64) (storage.EventRef, bool) {
	shard := &m.shards[shardIndexForUint64(id)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.items[id]
	return value, ok
}

func (m *eventRefShardMap) store(id uint64, value storage.EventRef) {
	shard := &m.shards[shardIndexForUint64(id)]
	shard.mu.Lock()
	shard.items[id] = value
	shard.mu.Unlock()
}

func (m *eventRefShardMap) count() int {
	total := 0
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		total += len(shard.items)
		shard.mu.RUnlock()
	}
	return total
}

func (m *eventRefShardMap) shardSizes() []int {
	sizes := make([]int, mapShardCount)
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		sizes[i] = len(shard.items)
		shard.mu.RUnlock()
	}
	return sizes
}
