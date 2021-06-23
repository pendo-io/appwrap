// +build memcached

package appwrap

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	cloudmemcache "cloud.google.com/go/memcache/apiv1"
	"github.com/go-redis/redis/v8"
	"github.com/pendo-io/gomemcache/memcache"
	"go.opencensus.io/trace"
	memcachepb "google.golang.org/genproto/googleapis/cloud/memcache/v1"
)

var ErrCacheMiss = memcache.ErrCacheMiss
var CacheErrNotStored = memcache.ErrNotStored
var CacheErrCASConflict = memcache.ErrCASConflict
var CacheErrServerError = memcache.ErrServerError

func (c CacheItem) toMemcacheItem() *memcache.Item {
	expiration := int32(0)
	if c.Expiration > 0 {
		expiration = int32(c.Expiration / time.Second)
		if expiration == 0 {
			expiration = 1
		}
	}

	return &memcache.Item{
		Key:        c.Key,
		Value:      c.Value,
		Flags:      c.Flags,
		Expiration: expiration,
	}
}

func memcacheItemToCacheItem(m *memcache.Item) *CacheItem {
	return &CacheItem{
		Key:        m.Key,
		Value:      m.Value,
		Flags:      m.Flags,
		Expiration: time.Duration(m.Expiration) * time.Second,
	}
}

type memcachedClient interface {
	Add(item *memcache.Item) error
	CompareAndSwap(item *memcache.Item) error
	Delete(key string) error
	FlushAll() error
	Get(key string) (item *memcache.Item, err error)
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Increment(key string, delta uint64) (newValue uint64, err error)
	Set(item *memcache.Item) error
}

type memcached struct {
	ctx    context.Context
	client memcachedClient
	ns     string
	log    Logging
}

var memcacheClient *memcache.Client
var memcacheMtx sync.Mutex
var addrs []string

func NewAppengineMemcache(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards) (Memcache, error) {
	if memcacheClient == nil {
		memcacheMtx.Lock()
		defer memcacheMtx.Unlock()
		if memcacheClient == nil {
			var err error
			addr, err := getDiscoveryAddress(loc, name)
			if err != nil {
				return nil, err
			}
			memcacheClient, err = memcache.NewDiscoveryClient(addr, 5*time.Second)
			memcacheClient.MaxIdleConns = 10 // complete guess - per address
			if err != nil {
				return nil, err
			}
		}
	}

	return memcached{
		ctx:    c,
		client: memcacheClient,
		log:    NewStackdriverLogging(c),
	}, nil
}

func getDiscoveryAddress(loc CacheLocation, name CacheName) (string, error) {
	if len(addrs) > 0 {
		return addrs[0], nil
	}

	appInfo := NewAppengineInfoFromContext(context.Background())
	projectId := appInfo.NativeProjectID()

	client, err := cloudmemcache.NewCloudMemcacheClient(context.Background())
	if err != nil {
		return "", err
	}
	defer func() {
		_ = client.Close()
	}()

	instance, err := client.GetInstance(context.Background(), &memcachepb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/instances/%s", projectId, loc, name),
	})
	if err != nil {
		return "", err
	}

	addrs = []string{instance.DiscoveryEndpoint}
	return instance.DiscoveryEndpoint, nil
}

func NewAppengineRateLimitedMemcache(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards, log Logging, createLimiters func(shard int, log Logging) redis.Limiter) (Memcache, error) {
	return NewAppengineMemcache(c, appInfo, loc, name, shards)
}

func InitializeRedisAddrs(newAddrs []string) {
	addrs = newAddrs
}

func (m memcached) namespacedKey(key string) string {
	nsKey := m.ns + ":" + key
	if len(nsKey) > 250 {
		truncatedKey := nsKey[:200]
		hash := sha256.Sum256([]byte(nsKey))
		truncatedKey = truncatedKey + base64.StdEncoding.EncodeToString(hash[:])
		nsKey = truncatedKey
	}
	m.log.Debugf("Key: %s", nsKey)
	return nsKey
}

func (m memcached) Add(item *CacheItem) error {
	_, span := trace.StartSpan(m.ctx, traceMemcacheAdd)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, item.Key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	mItem := item.toMemcacheItem()
	mItem.Key = m.namespacedKey(item.Key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, mItem.Key))

	return m.client.Add(mItem)
}

func (m memcached) AddMulti(items []*CacheItem) error {
	errList := make(MultiError, len(items))

	haveErrors := false
	for i, item := range items {
		if err := m.Add(item); err != nil {
			errList[i] = err
			haveErrors = true
		}
	}

	if haveErrors {
		return errList
	}

	return nil
}

func (m memcached) CompareAndSwap(item *CacheItem) error {
	_, span := trace.StartSpan(m.ctx, traceMemcacheCAS)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, item.Key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	mItem := item.toMemcacheItem()
	mItem.Key = m.namespacedKey(item.Key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, mItem.Key))

	return m.client.CompareAndSwap(mItem)
}

func (m memcached) Delete(key string) error {
	_, span := trace.StartSpan(m.ctx, traceMemcacheDelete)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	key = m.namespacedKey(key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, key))

	return m.client.Delete(key)
}

func (m memcached) DeleteMulti(keys []string) error {
	errList := make(MultiError, len(keys))

	haveErrors := false
	for i, key := range keys {
		if err := m.Delete(key); err != nil {
			errList[i] = err
			haveErrors = true
		}
	}

	if haveErrors {
		return errList
	}

	return nil
}

func (m memcached) Flush() error {
	return m.client.FlushAll()
}

func (m memcached) FlushShard(shard int) error {
	panic("not implemented")
}

func (m memcached) Get(key string) (*CacheItem, error) {
	_, span := trace.StartSpan(m.ctx, traceMemcacheGet)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	nsKey := m.namespacedKey(key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, nsKey))

	res, err := m.client.Get(nsKey)
	if err != nil {
		return nil, err
	}
	res.Key = key
	return memcacheItemToCacheItem(res), nil
}

func (m memcached) GetMulti(keys []string) (map[string]*CacheItem, error) {
	nsKeys := make([]string, len(keys))
	nsKeyToRegularKey := make(map[string]string, len(keys))
	for i, key := range keys {
		nsKey := m.namespacedKey(key)
		nsKeys[i] = nsKey
		nsKeyToRegularKey[nsKey] = key
	}

	_, span := trace.StartSpan(m.ctx, traceMemcacheGetMulti)
	defer span.End()

	if len(keys) > 0 {
		span.AddAttributes(trace.StringAttribute(traceLabelFirstKey, keys[0]))
		span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))
		span.AddAttributes(trace.StringAttribute(traceLabelFullKey, nsKeys[0]))
	}

	mRes, err := m.client.GetMulti(nsKeys)
	res := make(map[string]*CacheItem, len(mRes))
	for nsKey, v := range mRes {
		item := memcacheItemToCacheItem(v)
		key := nsKeyToRegularKey[nsKey]
		item.Key = key
		res[key] = item
	}
	return res, err
}

func (m memcached) Increment(key string, amount int64, initialValue uint64) (uint64, error) {
	_, span := trace.StartSpan(m.ctx, traceMemcacheIncr)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	key = m.namespacedKey(key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, key))

	var amt uint64
	var err error
	if amt, err = m.client.Increment(key, uint64(amount)); err == ErrCacheMiss {
		_ = m.client.Add(&memcache.Item{
			Key:   key,
			Value: []byte(fmt.Sprintf("%d", initialValue)),
		})
		amt, err = m.client.Increment(key, uint64(amount))
	}

	return amt, err
}

func (m memcached) IncrementExisting(key string, amount int64) (uint64, error) {
	_, span := trace.StartSpan(m.ctx, traceMemcacheIncrExisting)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	key = m.namespacedKey(key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, key))

	return m.client.Increment(key, uint64(amount))
}

func (m memcached) Namespace(ns string) Memcache {
	return memcached{
		ctx:    m.ctx,
		client: m.client,
		ns:     ns,
		log:    m.log,
	}
}

func (m memcached) Set(item *CacheItem) error {
	_, span := trace.StartSpan(m.ctx, traceMemcacheSet)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, item.Key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	mItem := item.toMemcacheItem()
	mItem.Key = m.namespacedKey(item.Key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, mItem.Key))

	return m.client.Set(mItem)
}

func (m memcached) SetMulti(items []*CacheItem) error {
	errList := make(MultiError, len(items))

	haveErrors := false
	for i, item := range items {
		if err := m.Set(item); err != nil {
			errList[i] = err
			haveErrors = true
		}
	}

	if haveErrors {
		return errList
	}

	return nil
}
