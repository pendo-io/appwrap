package appwrap

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cloudmemcache "cloud.google.com/go/memcache/apiv1"
	"github.com/cespare/xxhash/v2"
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
}

type memcacheService struct {
	client            *memcache.Client
	lock              sync.Mutex
	discoveryEndpoint string
}

var globalMemcacheService memcacheService

func NewAppengineMemcache(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName) (Memcache, error) {
	return globalMemcacheService.NewAppengineMemcache(c, appInfo, loc, name)
}

func (m *memcacheService) NewAppengineMemcache(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName) (Memcache, error) {
	if m.client == nil {
		m.lock.Lock()
		defer m.lock.Unlock()
		if m.client == nil {
			var err error
			addr, err := m.getDiscoveryAddress(appInfo, loc, name)
			if err != nil {
				return nil, err
			}
			m.client, err = memcache.NewDiscoveryClientFromSelector(addr, 5*time.Second, &consistentHashServerSelector{})
			m.client.MaxIdleConns = memcachedPoolSize / 2
			m.client.SetMaxActiveConns(memcachedPoolSize)
			m.client.PoolTimeout = memcachedPoolTimeout
			m.client.Timeout = memcachedOperationTimeout
			if err != nil {
				return nil, err
			}
		}
	}

	return memcached{
		ctx:    c,
		client: m.client,
	}, nil
}

func (m *memcacheService) getDiscoveryAddress(appInfo AppengineInfo, loc CacheLocation, name CacheName) (string, error) {
	if m.discoveryEndpoint != "" {
		return m.discoveryEndpoint, nil
	}

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

	m.discoveryEndpoint = instance.DiscoveryEndpoint
	return m.discoveryEndpoint, nil
}

func InitializeMemcacheDiscovery(endpoint string) {
	globalMemcacheService.lock.Lock()
	defer globalMemcacheService.lock.Unlock()
	if globalMemcacheService.discoveryEndpoint != endpoint {
		if globalMemcacheService.client != nil {
			globalMemcacheService.client.StopPolling()
		}
		globalMemcacheService.client = nil
		globalMemcacheService.discoveryEndpoint = endpoint
	}
}

func (m memcached) encodedNamespacedKey(key string) string {
	nsKey := base64.StdEncoding.EncodeToString([]byte(m.ns + ":" + key))

	if len(nsKey) > 250 {
		truncatedKey := nsKey[:200]
		hash := sha256.Sum256([]byte(nsKey))
		truncatedKey = truncatedKey + base64.StdEncoding.EncodeToString(hash[:])
		nsKey = truncatedKey
	}

	return nsKey
}

func (m memcached) Add(item *CacheItem) error {
	_, span := trace.StartSpan(m.ctx, traceMemcacheAdd)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, item.Key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	mItem := item.toMemcacheItem()
	mItem.Key = m.encodedNamespacedKey(item.Key)
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
	mItem.Key = m.encodedNamespacedKey(item.Key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, mItem.Key))

	return m.client.CompareAndSwap(mItem)
}

func (m memcached) Delete(key string) error {
	_, span := trace.StartSpan(m.ctx, traceMemcacheDelete)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	key = m.encodedNamespacedKey(key)
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

	nsKey := m.encodedNamespacedKey(key)
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
		nsKey := m.encodedNamespacedKey(key)
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

	key = m.encodedNamespacedKey(key)
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

	key = m.encodedNamespacedKey(key)
	span.AddAttributes(trace.StringAttribute(traceLabelFullKey, key))

	return m.client.Increment(key, uint64(amount))
}

func (m memcached) Namespace(ns string) Memcache {
	return memcached{
		ctx:    m.ctx,
		client: m.client,
		ns:     ns,
	}
}

func (m memcached) Set(item *CacheItem) error {
	_, span := trace.StartSpan(m.ctx, traceMemcacheSet)
	defer span.End()

	span.AddAttributes(trace.StringAttribute(traceLabelKey, item.Key))
	span.AddAttributes(trace.StringAttribute(traceLabelNamespace, m.ns))

	mItem := item.toMemcacheItem()
	mItem.Key = m.encodedNamespacedKey(item.Key)
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

const numEntriesPerServer = 1000

type hashedServer struct {
	hash uint64
	addr net.Addr
}

// see 	"github.com/google/gomemcache/memcache" in selector.go
// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw, str string
}

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func (s *staticAddr) Network() string { return s.ntw }
func (s *staticAddr) String() string  { return s.str }

type serverList []hashedServer

func (s serverList) Less(i, j int) bool {
	return s[i].hash < s[j].hash
}

func (s serverList) Len() int {
	return len(s)
}

func (s serverList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s serverList) String() string {
	b := strings.Builder{}
	b.WriteString("[")
	for _, entry := range s {
		b.WriteString(" ")
		b.WriteString("{" + strconv.FormatInt(int64(entry.hash), 10) + ": " + entry.addr.String() + "}")
	}
	b.WriteString("]")
	return b.String()
}

type consistentHashServerSelector struct {
	// pointers to allow us atomic access without requiring a mutex
	// anything that reads any list should first make a copy of the dereferenced pointer
	// anything that sets any list should create a NEW list and assign the new address
	//
	// do NOT dereference these pointers multiple times in the same function.
	// you're asking for out-of-bounds errors
	list      *serverList
	uniqAddrs *[]net.Addr
}

func (s *consistentHashServerSelector) PickServer(key string) (net.Addr, error) {
	if s.list == nil {
		return nil, memcache.ErrNoServers
	}

	list := *s.list

	keyHash := xxhash.Sum64String(key)

	idx := sort.Search(len(list), func(i int) bool {
		return list[i].hash > keyHash
	})

	if idx == len(list) {
		idx = 0
	}

	return list[idx].addr, nil
}

func (s *consistentHashServerSelector) PickAnyServer() (net.Addr, error) {
	if s.list == nil {
		return nil, memcache.ErrNoServers
	}

	list := *s.list

	idx := rand.Intn(len(list))
	return list[idx].addr, nil
}

func (s *consistentHashServerSelector) Each(f func(net.Addr) error) error {
	if s.list == nil {
		return memcache.ErrNoServers
	}

	list := *s.uniqAddrs

	for _, addr := range list {
		if err := f(addr); err != nil {
			return err
		}
	}

	return nil
}

func (s *consistentHashServerSelector) SetServers(servers ...string) error {
	if len(servers) == 0 {
		return nil
	}

	list := make(serverList, 0, len(servers)*numEntriesPerServer)

	uniqAddrs := make([]net.Addr, 0, len(servers))
	for _, server := range servers {
		tcpaddr, err := net.ResolveTCPAddr("tcp", server)
		if err != nil {
			return err
		}

		staticAddr := newStaticAddr(tcpaddr)
		uniqAddrs = append(uniqAddrs, staticAddr)

		for i := 0; i < numEntriesPerServer; i++ {
			list = append(list, hashedServer{
				hash: xxhash.Sum64String(server + strconv.FormatInt(int64(i), 10)),
				addr: staticAddr,
			})
		}
	}

	sort.Sort(list)

	s.uniqAddrs = &uniqAddrs

	// set list last - it's what we use to make sure we're initialized
	s.list = &list
	return nil
}

const (
	envMemcachedOperationTimeoutMs = "memcached_operation_timeout_ms"
	envMemcachedPoolSize           = "memcached_pool_size"
	envMemcachedPoolTimeoutMs      = "memcached_pool_timeout_ms"
)

var (
	memcachedOperationTimeout time.Duration
	memcachedPoolSize         int = 10
	memcachedPoolTimeout      time.Duration
)

func init() {
	timeoutMsStr := os.Getenv(envMemcachedOperationTimeoutMs)
	if timeoutMsStr != "" {
		timeoutMs, err := strconv.ParseInt(timeoutMsStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				envMemcachedOperationTimeoutMs, timeoutMsStr, err)
		} else if timeoutMs < 1 {
			fmt.Fprintf(os.Stderr, "'%s' must be a non-zero non-negative integer\n",
				envMemcachedOperationTimeoutMs)
		} else {
			memcachedOperationTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	poolSizeStr := os.Getenv(envMemcachedPoolSize)
	if poolSizeStr != "" {
		poolSize, err := strconv.ParseInt(poolSizeStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				envMemcachedPoolSize, poolSizeStr, err)
		} else if poolSize < 1 {
			fmt.Fprintf(os.Stderr, "'%s' must be a non-zero non-negative integer\n",
				envMemcachedPoolSize)
		} else {
			memcachedPoolSize = int(poolSize)
		}
	}

	poolTimeoutStr := os.Getenv(envMemcachedPoolTimeoutMs)
	if poolTimeoutStr != "" {
		timeoutMs, err := strconv.ParseInt(poolTimeoutStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				envMemcachedPoolTimeoutMs, poolTimeoutStr, err)
		} else if timeoutMs < 1 {
			fmt.Fprintf(os.Stderr, "'%s' must be a non-zero non-negative integer\n",
				envMemcachedPoolTimeoutMs)
		} else {
			memcachedPoolTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	if os.Getenv("LOCAL_DEBUG") == "true" {
		globalMemcacheService.client = memcache.New("127.0.0.1:11211")
	}
}
