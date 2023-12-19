package appwrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/pendo-io/gomemcache/memcache"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	. "gopkg.in/check.v1"
)

type memcachedMock struct {
	mock.Mock
}

func (m *memcachedMock) Add(item *memcache.Item) error {
	args := m.Called(item)
	return args.Error(0)
}

func (m *memcachedMock) CompareAndSwap(item *memcache.Item) error {
	args := m.Called(item)
	return args.Error(0)
}

func (m *memcachedMock) Delete(key string) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *memcachedMock) FlushAll() error {
	args := m.Called()
	return args.Error(0)
}

func (m *memcachedMock) Get(key string) (item *memcache.Item, err error) {
	args := m.Called(key)
	return args.Get(0).(*memcache.Item), args.Error(1)
}

func (m *memcachedMock) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	args := m.Called(keys)
	return args.Get(0).(map[string]*memcache.Item), args.Error(1)
}

func (m *memcachedMock) Increment(key string, delta uint64) (newValue uint64, err error) {
	args := m.Called(key, delta)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *memcachedMock) Set(item *memcache.Item) error {
	args := m.Called(item)
	return args.Error(0)
}

type MemcachedTest struct{}

var _ = Suite(&MemcachedTest{})

type memcachedTestFixture struct {
	client        memcached
	fatalErr      error
	memcachedMock *memcachedMock
	fullKey       func(key string) string
}

func (f *memcachedTestFixture) assertExpectations(c *C) {
	f.memcachedMock.AssertExpectations(c)
}

func (s *MemcachedTest) newFixture() memcachedTestFixture {
	mm := &memcachedMock{}
	m := memcached{
		ctx:    context.Background(),
		client: mm,
		ns:     "test-ns",
		tracer: otel.Tracer("memcachedTest"),
	}

	return memcachedTestFixture{
		client:        m,
		fatalErr:      errors.New("fatal-err"),
		memcachedMock: mm,
		fullKey:       m.encodedNamespacedKey,
	}
}

func (s *MemcachedTest) TestCacheItemToMemcacheItem(c *C) {
	item := &CacheItem{
		Key:        "key",
		Value:      []byte("aaaah"),
		Flags:      3,
		Expiration: 5 * time.Second,
	}
	c.Assert(item.toMemcacheItem(), DeepEquals, &memcache.Item{
		Key:        "key",
		Value:      []byte("aaaah"),
		Flags:      3,
		Expiration: 5,
	})

	// generally, floor expiration to the nearest second
	item.Expiration = 5*time.Second + 500*time.Millisecond
	c.Assert(item.toMemcacheItem(), DeepEquals, &memcache.Item{
		Key:        "key",
		Value:      []byte("aaaah"),
		Flags:      3,
		Expiration: 5,
	})

	// if 0 < expiration < 1 second, then set to 1 second
	item.Expiration = 5 * time.Millisecond
	c.Assert(item.toMemcacheItem(), DeepEquals, &memcache.Item{
		Key:        "key",
		Value:      []byte("aaaah"),
		Flags:      3,
		Expiration: 1,
	})
}

func (s *MemcachedTest) TestMemcacheItemToCacheItem(c *C) {
	item := &memcache.Item{
		Key:        "key",
		Value:      []byte("aaaah"),
		Flags:      3,
		Expiration: 5,
	}
	c.Assert(memcacheItemToCacheItem(item), DeepEquals, &CacheItem{
		Key:        "key",
		Value:      []byte("aaaah"),
		Flags:      3,
		Expiration: 5 * time.Second,
	})
}

func (s *MemcachedTest) TestNamespacedKey(c *C) {
	f := s.newFixture()
	regularKey := "key1"
	nsKey := f.client.encodedNamespacedKey(regularKey)
	c.Assert(nsKey, Equals, "dGVzdC1uczprZXkx")

	longKey := "reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-" +
		"reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-" +
		"reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-" +
		"reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey"

	nsLongKey := f.client.encodedNamespacedKey(longKey)
	c.Assert(len(nsLongKey) < 250, IsTrue)
	c.Assert(nsLongKey, Equals, "dGVzdC1uczpyZWFsbHlsb25na2V5LXJlYWxseWxvbmdrZXktcmVhbGx5bG9uZ2tleS1yZWFsbHlsb25na2V5LXJlYWxseWxvbmdrZXktcmVhbGx5bG9uZ2tleS1yZWFsbHlsb25na2V5LXJlYWxseWxvbmdrZXktcmVhbGx5bG9uZ2tleS1yZWFsbHlsb25na2V5LXJli1UTTI9tJkwI1Pz3hxWXlnfHc43z6R4dHz5Y7M80b0o=")
}

func (s *MemcachedTest) TestAdd(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Add", &memcache.Item{
		Key:        f.fullKey("key"),
		Value:      []byte("aaaah"),
		Expiration: 5,
	}).Return(nil).Once()

	err := f.client.Add(&CacheItem{
		Key:        "key",
		Value:      []byte("aaaah"),
		Expiration: 5 * time.Second,
	})

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestAddMulti(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Add", &memcache.Item{
		Key:        f.fullKey("key1"),
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Add", &memcache.Item{
		Key:        f.fullKey("key2"),
		Value:      []byte("aaaah2"),
		Expiration: 5,
	}).Return(nil).Once()

	err := f.client.AddMulti([]*CacheItem{
		{
			Key:        "key1",
			Value:      []byte("aaaah1"),
			Expiration: 3 * time.Second,
		},
		{
			Key:        "key2",
			Value:      []byte("aaaah2"),
			Expiration: 5 * time.Second,
		},
	})

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestAddMultiError(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Add", &memcache.Item{
		Key:        f.fullKey("key1"),
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Add", &memcache.Item{
		Key:        f.fullKey("key2"),
		Value:      []byte("aaaah2"),
		Expiration: 5,
	}).Return(f.fatalErr).Once()

	err := f.client.AddMulti([]*CacheItem{
		{
			Key:        "key1",
			Value:      []byte("aaaah1"),
			Expiration: 3 * time.Second,
		},
		{
			Key:        "key2",
			Value:      []byte("aaaah2"),
			Expiration: 5 * time.Second,
		},
	})

	c.Assert(err, DeepEquals, MultiError{nil, f.fatalErr})
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestCompareAndSwap(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("CompareAndSwap", &memcache.Item{
		Key:        f.fullKey("key"),
		Value:      []byte("aaaah"),
		Expiration: 5,
	}).Return(nil).Once()

	err := f.client.CompareAndSwap(&CacheItem{
		Key:        "key",
		Value:      []byte("aaaah"),
		Expiration: 5 * time.Second,
	})

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestDelete(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Delete", f.fullKey("key")).Return(nil).Once()

	err := f.client.Delete("key")

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestDeleteMulti(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Delete", f.fullKey("key1")).Return(nil).Once()
	f.memcachedMock.On("Delete", f.fullKey("key2")).Return(nil).Once()

	err := f.client.DeleteMulti([]string{"key1", "key2"})

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestDeleteMultiError(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Delete", f.fullKey("key1")).Return(f.fatalErr).Once()
	f.memcachedMock.On("Delete", f.fullKey("key2")).Return(nil).Once()

	err := f.client.DeleteMulti([]string{"key1", "key2"})

	c.Assert(err, DeepEquals, MultiError{f.fatalErr, nil})
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestFlush(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("FlushAll").Return(nil).Once()

	err := f.client.Flush()

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestFlushShard(c *C) {
	f := s.newFixture()

	shouldPanic := func() {
		_ = f.client.FlushShard(0)
	}

	c.Assert(shouldPanic, PanicMatches, "not implemented")
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestGet(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Get", f.fullKey("key")).Return(&memcache.Item{
		Key:        f.fullKey("key"),
		Value:      []byte("aaaah"),
		Expiration: 5,
	}, nil).Once()

	item, err := f.client.Get("key")
	c.Assert(err, IsNil)
	c.Assert(item, DeepEquals, &CacheItem{
		Key:        "key",
		Value:      []byte("aaaah"),
		Expiration: 5 * time.Second,
	})
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestGetMulti(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("GetMulti", []string{
		f.fullKey("key1"),
		f.fullKey("key2"),
	}).Return(map[string]*memcache.Item{
		f.fullKey("key1"): {
			Key:        f.fullKey("key1"),
			Value:      []byte("aaaah1"),
			Expiration: 5,
		},
		f.fullKey("key2"): {
			Key:        f.fullKey("key2"),
			Value:      []byte("aaaah2"),
			Expiration: 3,
		},
	}, nil).Once()

	items, err := f.client.GetMulti([]string{"key1", "key2"})
	c.Assert(err, IsNil)
	c.Assert(items, DeepEquals, map[string]*CacheItem{
		"key1": {
			Key:        "key1",
			Value:      []byte("aaaah1"),
			Expiration: 5 * time.Second,
		},
		"key2": {
			Key:        "key2",
			Value:      []byte("aaaah2"),
			Expiration: 3 * time.Second,
		},
	})
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestIncrement_AlreadyInCache(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Increment", f.fullKey("key"), uint64(5)).Return(uint64(12), nil).Once()

	amt, err := f.client.Increment("key", 5, 10, 0)
	c.Assert(err, IsNil)
	c.Assert(amt, Equals, uint64(12))
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestIncrement_NotInCache(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Increment", f.fullKey("key"), uint64(5)).Return(uint64(0), ErrCacheMiss).Once()
	f.memcachedMock.On("Add", &memcache.Item{
		Key:        f.fullKey("key"),
		Value:      []byte("10"),
		Expiration: int32(5 * time.Minute / time.Second),
	}).Return(nil).Once()
	f.memcachedMock.On("Increment", f.fullKey("key"), uint64(5)).Return(uint64(15), nil).Once()

	amt, err := f.client.Increment("key", 5, 10, 5*time.Minute)
	c.Assert(err, IsNil)
	c.Assert(amt, Equals, uint64(15))
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestIncrementExisting(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Increment", f.fullKey("key"), uint64(5)).Return(uint64(12), nil).Once()

	amt, err := f.client.IncrementExisting("key", 5)
	c.Assert(err, IsNil)
	c.Assert(amt, Equals, uint64(12))
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestNamespace(c *C) {
	f := s.newFixture()
	nsCache := f.client.Namespace("new-ns").(memcached)

	// rest of the fields should be unchanged
	c.Assert(nsCache.ns, Equals, "new-ns")
	nsCache.ns = f.client.ns
	c.Assert(nsCache, DeepEquals, f.client)
}

func (s *MemcachedTest) TestSet(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Set", &memcache.Item{
		Key:        f.fullKey("key"),
		Value:      []byte("aaaah"),
		Expiration: 5,
	}).Return(nil).Once()

	err := f.client.Set(&CacheItem{
		Key:        "key",
		Value:      []byte("aaaah"),
		Expiration: 5 * time.Second,
	})

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestSetMulti(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Set", &memcache.Item{
		Key:        f.fullKey("key1"),
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Set", &memcache.Item{
		Key:        f.fullKey("key2"),
		Value:      []byte("aaaah2"),
		Expiration: 5,
	}).Return(nil).Once()

	err := f.client.SetMulti([]*CacheItem{
		{
			Key:        "key1",
			Value:      []byte("aaaah1"),
			Expiration: 3 * time.Second,
		},
		{
			Key:        "key2",
			Value:      []byte("aaaah2"),
			Expiration: 5 * time.Second,
		},
	})

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestSetMultiError(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Set", &memcache.Item{
		Key:        f.fullKey("key1"),
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Set", &memcache.Item{
		Key:        f.fullKey("key2"),
		Value:      []byte("aaaah2"),
		Expiration: 5,
	}).Return(f.fatalErr).Once()

	err := f.client.SetMulti([]*CacheItem{
		{
			Key:        "key1",
			Value:      []byte("aaaah1"),
			Expiration: 3 * time.Second,
		},
		{
			Key:        "key2",
			Value:      []byte("aaaah2"),
			Expiration: 5 * time.Second,
		},
	})

	c.Assert(err, DeepEquals, MultiError{nil, f.fatalErr})
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestConsistentHashServerSelector(c *C) {
	sel := &consistentHashServerSelector{}

	addr, err := sel.PickServer("k1")
	c.Assert(err, Equals, memcache.ErrNoServers)
	c.Assert(addr, IsNil)
	addr, err = sel.PickAnyServer()
	c.Assert(err, Equals, memcache.ErrNoServers)
	c.Assert(addr, IsNil)
	err = sel.Each(nil)
	c.Assert(err, Equals, memcache.ErrNoServers)
	c.Assert(addr, IsNil)

	err = sel.SetServers("1.1.1.1:11211")
	c.Assert(err, IsNil)
	c.Assert(*sel.list, HasLen, numEntriesPerServer)
	c.Assert(*sel.uniqAddrs, HasLen, 1)

	addr, err = sel.PickServer("k1")
	c.Assert(err, IsNil)
	c.Assert(addr.String(), Equals, "1.1.1.1:11211")

	addr, err = sel.PickAnyServer()
	c.Assert(err, IsNil)
	c.Assert(addr.String(), Equals, "1.1.1.1:11211")

	err = sel.SetServers("1.1.1.1:11211", "2.2.2.2:11211")
	c.Assert(err, IsNil)
	c.Assert(*sel.list, HasLen, 2*numEntriesPerServer)
	c.Assert(*sel.uniqAddrs, HasLen, 2)

	seenServers := make(map[string]bool, 2)

	err = sel.Each(func(addr net.Addr) error {
		str := addr.String()
		if _, ok := seenServers[str]; ok {
			// seen twice
			return errors.New("aaaah")
		}
		seenServers[str] = true
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(seenServers, DeepEquals, map[string]bool{
		"1.1.1.1:11211": true,
		"2.2.2.2:11211": true,
	})
}

func (s *MemcachedTest) TestConsistenHashServerSelectorHashing(c *C) {
	keys := []string{
		"key-aaaaaaaa",
		"key-bbbbbbbb",
		"key-cccccccc",
		"key-dddddddd",
		"key-eeeeeeee",
		"key-ffffffff",
		"key-gggggggg",
		"key-hhhhhhhh",
		"key-iiiiiiii",
		"key-jjjjjjjj",
	}

	testCases := []struct {
		servers  []string
		expected []string
	}{
		{
			servers:  []string{"1.1.1.1:1"},
			expected: []string{"1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1"},
		},
		{ // around 5 keys should change from above result
			servers:  []string{"1.1.1.1:1", "2.2.2.2:2"},
			expected: []string{"1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "2.2.2.2:2", "2.2.2.2:2", "2.2.2.2:2", "2.2.2.2:2", "2.2.2.2:2", "2.2.2.2:2"},
		},
		{ // around 3 keys should change from above result
			servers:  []string{"1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3"},
			expected: []string{"1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3", "2.2.2.2:2", "3.3.3.3:3", "2.2.2.2:2", "2.2.2.2:2"},
		},
		{ // around 3 keys should change from above result
			servers:  []string{"1.1.1.1:1", "4.4.4.4:4", "2.2.2.2:2", "3.3.3.3:3"},
			expected: []string{"1.1.1.1:1", "4.4.4.4:4", "4.4.4.4:4", "1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3", "2.2.2.2:2", "4.4.4.4:4", "2.2.2.2:2", "4.4.4.4:4"},
		},
		{ // server order should not matter, same as previous result
			servers:  []string{"2.2.2.2:2", "1.1.1.1:1", "3.3.3.3:3", "4.4.4.4:4"},
			expected: []string{"1.1.1.1:1", "4.4.4.4:4", "4.4.4.4:4", "1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3", "2.2.2.2:2", "4.4.4.4:4", "2.2.2.2:2", "4.4.4.4:4"},
		},
		{ // more servers
			servers:  []string{"5.5.5.5:5", "2.2.2.2:2", "1.1.1.1:1", "3.3.3.3:3", "4.4.4.4:4"},
			expected: []string{"5.5.5.5:5", "4.4.4.4:4", "4.4.4.4:4", "1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3", "5.5.5.5:5", "5.5.5.5:5", "2.2.2.2:2", "5.5.5.5:5"},
		},
		{ // more servers
			servers:  []string{"5.5.5.5:5", "2.2.2.2:2", "1.1.1.1:1", "3.3.3.3:3", "4.4.4.4:4", "6.6.6.6:6"},
			expected: []string{"5.5.5.5:5", "4.4.4.4:4", "4.4.4.4:4", "1.1.1.1:1", "2.2.2.2:2", "6.6.6.6:6", "5.5.5.5:5", "5.5.5.5:5", "2.2.2.2:2", "5.5.5.5:5"},
		},
		{ // more servers
			servers:  []string{"5.5.5.5:5", "2.2.2.2:2", "1.1.1.1:1", "3.3.3.3:3", "7.7.7.7:7", "4.4.4.4:4", "6.6.6.6:6"},
			expected: []string{"7.7.7.7:7", "4.4.4.4:4", "4.4.4.4:4", "1.1.1.1:1", "2.2.2.2:2", "7.7.7.7:7", "5.5.5.5:5", "5.5.5.5:5", "2.2.2.2:2", "5.5.5.5:5"},
		},
		{ // since none of the keys are on 3 at the moment, removing 3 should not change anything
			servers:  []string{"5.5.5.5:5", "2.2.2.2:2", "1.1.1.1:1", "7.7.7.7:7", "4.4.4.4:4", "6.6.6.6:6"},
			expected: []string{"7.7.7.7:7", "4.4.4.4:4", "4.4.4.4:4", "1.1.1.1:1", "2.2.2.2:2", "7.7.7.7:7", "5.5.5.5:5", "5.5.5.5:5", "2.2.2.2:2", "5.5.5.5:5"},
		},
	}

	for i, tc := range testCases {
		fmt.Printf("case %d\n", i)
		actual := make([]string, len(keys))
		sel := consistentHashServerSelector{}
		err := sel.SetServers(tc.servers...)
		c.Assert(err, IsNil)

		for i, k := range keys {
			server, err := sel.PickServer(k)
			c.Assert(err, IsNil)
			actual[i] = server.String()
		}

		c.Assert(actual, DeepEquals, tc.expected)
	}
}
