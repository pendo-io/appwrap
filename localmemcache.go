package appwrap

import (
	"strconv"
	"sync"
	"time"
)

type cachedItem struct {
	value   []byte
	expires time.Time
	addedAt time.Time
}

type LocalMemcache struct {
	items map[string]cachedItem
	mtx   sync.Mutex
}

func NewLocalMemcache() Memcache {
	return &LocalMemcache{items: make(map[string]cachedItem)}
}

func (mc *LocalMemcache) Add(item *CacheItem) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	var expires time.Time
	addedAt := time.Now()
	if item.Expiration > 0 {
		expires = addedAt.Add(item.Expiration)
	}
	return mc.set(item.Key, cachedItem{value: item.Value, expires: expires, addedAt: addedAt}, true)
}

func (mc *LocalMemcache) AddMulti(items []*CacheItem) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	errList := make(MultiError, len(items))
	errors := false

	var expires time.Time
	addedAt := time.Now()
	for i, item := range items {
		if item.Expiration > 0 {
			expires = addedAt.Add(item.Expiration)
		}
		if err := mc.set(item.Key, cachedItem{value: item.Value, expires: expires, addedAt: addedAt}, true); err != nil {
			errList[i] = err
			errors = true
		}
	}

	if errors {
		return errList
	}

	return nil
}

func (mc *LocalMemcache) CompareAndSwap(item *CacheItem) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	if item == nil {
		panic("item cannot be nil")
	}

	existingItem, found := mc.items[item.Key]

	if !found {
		return CacheErrNotStored
	}

	if !existingItem.addedAt.Equal(item.casTime.(time.Time)) {
		return CacheErrCASConflict
	}

	existingItem.value = item.Value
	existingItem.addedAt = time.Now()
	mc.items[item.Key] = existingItem
	return nil
}

func (mc *LocalMemcache) Delete(key string) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	return mc.delete(key)
}

func (mc *LocalMemcache) DeleteMulti(keys []string) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	hasErrors := false
	multiError := make(MultiError, len(keys))
	for i, key := range keys {
		if err := mc.delete(key); err != nil {
			multiError[i] = err
			hasErrors = true
		}
	}

	if hasErrors {
		return multiError
	}

	return nil
}

func (mc *LocalMemcache) delete(key string) error {
	if _, found := mc.items[key]; !found {
		return ErrCacheMiss
	}
	delete(mc.items, key)
	return nil
}

func (mc *LocalMemcache) Flush() error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	clear(mc.items)
	return nil
}

func (mc *LocalMemcache) FlushShard(_ int) error {
	return mc.Flush()
}

func (mc *LocalMemcache) get(key string) (*CacheItem, bool) {
	cachedItem, found := mc.items[key]
	if !found {
		return nil, false
	}

	if !cachedItem.expires.IsZero() && cachedItem.expires.Before(time.Now()) {
		delete(mc.items, key)
		return nil, false
	}

	item := &CacheItem{
		Key:     key,
		Value:   cachedItem.value,
		casTime: cachedItem.addedAt,
	}

	if !cachedItem.expires.IsZero() {
		item.Expiration = cachedItem.expires.Sub(cachedItem.addedAt)
	}

	return item, true
}

func (mc *LocalMemcache) Get(key string) (*CacheItem, error) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	item, exists := mc.get(key)
	if !exists {
		return nil, ErrCacheMiss
	}
	return item, nil
}

func (mc *LocalMemcache) GetMulti(keys []string) (map[string]*CacheItem, error) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	results := make(map[string]*CacheItem)

	for _, key := range keys {
		if item, found := mc.get(key); found {
			cpy := *item
			results[key] = &cpy
		}
	}

	return results, nil
}

func (mc *LocalMemcache) Increment(key string, amount int64, initialValue uint64, initialExpires time.Duration) (uint64, error) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	return mc.increment(key, amount, &initialValue, initialExpires)
}

func (mc *LocalMemcache) IncrementExisting(key string, amount int64) (uint64, error) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	return mc.increment(key, amount, nil, time.Duration(0))
}

func (mc *LocalMemcache) increment(key string, amount int64, initialValue *uint64, initialExpires time.Duration) (uint64, error) {
	var oldValue uint64

	item, exists := mc.items[key]
	if !exists {
		if initialValue == nil {
			return 0, ErrCacheMiss
		}
		oldValue = *initialValue
		item.addedAt = time.Now()
		if initialExpires > 0 {
			item.expires = item.addedAt.Add(initialExpires)
		}
	} else {
		var err error
		if oldValue, err = strconv.ParseUint(string(item.value), 10, 64); err != nil {
			return 0, err
		}
	}

	var newValue uint64
	if amount < 0 {
		newValue = oldValue - uint64(-amount)
	} else {
		newValue = oldValue + uint64(amount)
	}

	item.value = []byte(strconv.FormatUint(newValue, 10))
	mc.items[key] = item
	return newValue, nil
}

func (mc *LocalMemcache) set(key string, cachedItem cachedItem, addOnly bool) error {
	if addOnly {
		if _, exists := mc.items[key]; exists && (cachedItem.expires.IsZero() || !cachedItem.expires.Before(time.Now())) {
			return CacheErrNotStored
		}
	}
	mc.items[key] = cachedItem
	return nil
}

func (mc *LocalMemcache) Set(item *CacheItem) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	var expires time.Time
	addedAt := time.Now()
	if item.Expiration > 0 {
		expires = addedAt.Add(item.Expiration)
	}
	return mc.set(item.Key, cachedItem{value: item.Value, expires: expires, addedAt: addedAt}, false)
}

func (mc *LocalMemcache) SetMulti(items []*CacheItem) error {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	var expires time.Time
	addedAt := time.Now()
	for _, item := range items {
		if item.Expiration > 0 {
			expires = addedAt.Add(item.Expiration)
		}
		_ = mc.set(item.Key, cachedItem{value: item.Value, expires: expires, addedAt: addedAt}, false) // Set cannot error here, so ignore
	}
	return nil
}

func (mc *LocalMemcache) Namespace(_ string) Memcache {
	// this should do something maybe
	return mc
}
