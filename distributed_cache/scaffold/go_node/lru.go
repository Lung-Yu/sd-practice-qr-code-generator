package main

import (
	"container/list"
	"errors"
	"math"
	"sync"
	"time"
)

var ErrMiss = errors.New("miss")
var ErrExpired = errors.New("expired")

type entry struct {
	key       string
	value     string
	expiresAt *time.Time
}

type LRUCache struct {
	capacity  int
	mu        sync.Mutex
	items     map[string]*list.Element
	list      *list.List
	hits      int64
	misses    int64
	evictions int64
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (c *LRUCache) Get(key string) (string, *int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		c.misses++
		return "", nil, ErrMiss
	}

	e := el.Value.(*entry)
	if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
		c.list.Remove(el)
		delete(c.items, key)
		c.misses++
		return "", nil, ErrExpired
	}

	c.list.MoveToFront(el)
	c.hits++

	var rem *int
	if e.expiresAt != nil {
		secs := int(math.Round(time.Until(*e.expiresAt).Seconds()))
		rem = &secs
	}
	return e.value, rem, nil
}

func (c *LRUCache) Set(key, value string, ttl *int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var exp *time.Time
	if ttl != nil {
		t := time.Now().Add(time.Duration(*ttl) * time.Second)
		exp = &t
	}

	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		e.value = value
		e.expiresAt = exp
		c.list.MoveToFront(el)
		return
	}

	if c.list.Len() >= c.capacity {
		back := c.list.Back()
		if back != nil {
			e := back.Value.(*entry)
			delete(c.items, e.key)
			c.list.Remove(back)
			c.evictions++
		}
	}

	e := &entry{key: key, value: value, expiresAt: exp}
	el := c.list.PushFront(e)
	c.items[key] = el
}

func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.list.Remove(el)
		delete(c.items, key)
	}
}

type CacheStats struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Size      int   `json:"size"`
	Capacity  int   `json:"capacity"`
}

func (c *LRUCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
		Size:      c.list.Len(),
		Capacity:  c.capacity,
	}
}
