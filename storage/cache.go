package storage

import (
	"time"
)

var cache *Cache

func init() {
	cache = NewCache()
}

func GetCache() *Cache {
	return cache
}

type Cache struct {
	entries map[string]*entry
}

type entry struct {
	value    *string
	expireAt int64
}

func NewCache() *Cache {
	return &Cache{
		entries: make(map[string]*entry),
	}
}

func (c *Cache) Set(key, value string, expireAfter uint64) error {
	e := &entry{
		value:    &value,
		expireAt: 0,
	}

	if expireAfter > 0 {
		e.expireAt = time.Now().UnixMilli() + int64(expireAfter)
	}

	c.entries[key] = e
	return nil
}

func (c *Cache) Get(key string) (*string, error) {
	if e, ok := c.entries[key]; ok &&
		(e.expireAt == 0 || e.expireAt >= time.Now().UnixMilli()) {
		return e.value, nil
	}

	return nil, nil
}
