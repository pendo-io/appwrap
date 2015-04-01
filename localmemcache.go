package appengine_interfaces

import (
	"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"
	"strconv"
	"time"
)

type cachedItem struct {
	value   []byte
	expires time.Time
	addedAt time.Time
}

type LocalMemcache struct {
	items map[string]cachedItem
}

func NewLocalMemcache() Memcache {
	l := &LocalMemcache{}
	l.Flush()
	return l
}

func (mc *LocalMemcache) Add(item *memcache.Item) error {
	if _, exists := mc.items[item.Key]; exists {
		return memcache.ErrNotStored
	} else {
		return mc.Set(item)
	}

	return nil
}

func (mc *LocalMemcache) AddMulti(items []*memcache.Item) error {
	errList := make(appengine.MultiError, len(items))
	errors := false

	for i, item := range items {
		if err := mc.Add(item); err != nil {
			errList[i] = err
			errors = true
		}
	}

	if errors {
		return errList
	}

	return errList
}

func (mc *LocalMemcache) CompareAndSwap(item *memcache.Item) error {
	if entry, err := mc.Get(item.Key); err != nil && err == memcache.ErrCacheMiss {
		return memcache.ErrNotStored
	} else if err != nil {
		return err
	} else if !entry.Object.(time.Time).Equal(item.Object.(time.Time)) {
		return memcache.ErrCASConflict
	}

	return mc.SetMulti([]*memcache.Item{item})
}

func (mc *LocalMemcache) Delete(key string) error {
	if _, exists := mc.items[key]; !exists {
		return memcache.ErrCacheMiss
	}

	delete(mc.items, key)
	return nil
}

func (mc *LocalMemcache) DeleteMulti(keys []string) error {
	errors := false
	multiError := make(appengine.MultiError, len(keys))
	for i, key := range keys {
		if _, exists := mc.items[key]; !exists {
			multiError[i] = memcache.ErrCacheMiss
			errors = true
		} else {
			delete(mc.items, key)
		}
	}

	if errors {
		return multiError
	}

	return nil
}

func (mc *LocalMemcache) Flush() error {
	mc.items = make(map[string]cachedItem)
	return nil
}

func (mc *LocalMemcache) Get(key string) (*memcache.Item, error) {
	if item, exists := mc.items[key]; !exists {
		return nil, memcache.ErrCacheMiss
	} else if item.expires.Before(time.Now()) {
		delete(mc.items, key)
		return nil, memcache.ErrCacheMiss
	} else {
		return &memcache.Item{Key: key, Value: item.value, Object: item.addedAt}, nil
	}
}

func (mc *LocalMemcache) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	results := make(map[string]*memcache.Item)

	for _, key := range keys {
		if item, err := mc.Get(key); err == nil {
			cpy := *item
			results[key] = &cpy
		}
	}

	return results, nil
}

func (mc *LocalMemcache) IncrementExisting(key string, amount int64) (uint64, error) {
	// this doesn't reset the expiration time. probably should?
	if item, exists := mc.items[key]; !exists {
		return 0, memcache.ErrCacheMiss
	} else if val, err := strconv.ParseInt(string(item.value), 10, 64); err != nil {
		return 0, err
	} else {
		item.value = []byte(fmt.Sprintf("%d", val+amount))
		mc.items[key] = item
		return uint64(val + amount), nil
	}
}

func (mc *LocalMemcache) Set(item *memcache.Item) error {
	var expires time.Time

	if item.Expiration > 0 {
		expires = time.Now().Add(item.Expiration)
	} else {
		expires = time.Now().Add(time.Hour * 24)
	}

	mc.items[item.Key] = cachedItem{value: item.Value, expires: expires, addedAt: time.Now()}
	return nil
}

func (mc *LocalMemcache) SetMulti(items []*memcache.Item) error {
	for _, item := range items {
		mc.Set(item)
	}

	return nil
}

func (mc *LocalMemcache) Namespace(ns string) Memcache {
	// this should do something maybe
	return mc
}
