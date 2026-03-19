package db

import (
	"container/list"
	"encoding/json"
	"sync"

	"github.com/paerx/anhebridgedb/internal/storage"
)

type eventCache struct {
	mu       sync.Mutex
	maxItems int
	maxBytes int
	items    map[uint64]*list.Element
	order    *list.List
	size     int
	bytes    int
	hits     uint64
	misses   uint64
}

type eventCacheEntry struct {
	id    uint64
	event storage.Event
	size  int
}

func newEventCache(maxItems, maxBytes int) *eventCache {
	return &eventCache{
		maxItems: maxItems,
		maxBytes: maxBytes,
		items:    map[uint64]*list.Element{},
		order:    list.New(),
	}
}

func (c *eventCache) get(id uint64) (storage.Event, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[id]
	if !ok {
		c.misses++
		return storage.Event{}, false
	}
	c.hits++
	c.order.MoveToFront(elem)
	return elem.Value.(*eventCacheEntry).event, true
}

func (c *eventCache) add(event storage.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[event.EventID]; ok {
		entry := elem.Value.(*eventCacheEntry)
		c.bytes -= entry.size
		entry.event = event
		entry.size = estimateEventSize(event)
		c.bytes += entry.size
		c.order.MoveToFront(elem)
		c.evictLocked()
		return
	}
	entry := &eventCacheEntry{id: event.EventID, event: event, size: estimateEventSize(event)}
	elem := c.order.PushFront(entry)
	c.items[event.EventID] = elem
	c.size++
	c.bytes += entry.size
	c.evictLocked()
}

func (c *eventCache) evictLocked() {
	for (c.maxItems > 0 && c.size > c.maxItems) || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		tail := c.order.Back()
		if tail == nil {
			return
		}
		entry := tail.Value.(*eventCacheEntry)
		delete(c.items, entry.id)
		c.order.Remove(tail)
		c.size--
		c.bytes -= entry.size
	}
}

func (c *eventCache) stats() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := c.hits + c.misses
	ratio := 0.0
	if total > 0 {
		ratio = float64(c.hits) / float64(total)
	}
	return map[string]any{
		"event_cache_items": c.size,
		"event_cache_bytes": c.bytes,
		"cache_hit_ratio":   ratio,
	}
}

func estimateEventSize(event storage.Event) int {
	bytes, err := json.Marshal(event)
	if err != nil {
		return len(event.Key) + len(event.Operation) + len(event.AuthTag)
	}
	return len(bytes)
}
