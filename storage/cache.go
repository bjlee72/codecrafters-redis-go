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

func (c *Cache) Reset() {
	c.entries = make(map[string]*entry)
}

func (c *Cache) Set(key, value string, expireAfter int64) error {
	e := &entry{
		value:    &value,
		expireAt: 0,
	}

	if expireAfter > 0 {
		e.expireAt = time.Now().UnixMilli() + expireAfter
	}

	c.entries[key] = e
	return nil
}

func (c *Cache) Get(key string) (*string, error) {
	e, ok := c.entries[key]
	if !ok {
		return nil, nil
	}

	if e.expireAt == 0 || e.expireAt >= time.Now().UnixMilli() {
		return e.value, nil
	}

	delete(c.entries, key)

	return nil, nil
}

func (c *Cache) Keys() ([]string, error) {
	result := make([]string, 0, len(c.entries))
	for k := range c.entries {
		result = append(result, k)
	}
	return result, nil
}
