package appwrap

import (
	"context"
	"errors"
	"time"

	"github.com/pendo-io/gomemcache/memcache"
	"github.com/stretchr/testify/mock"
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
}

func (f *memcachedTestFixture) assertExpectations(c *C) {
	f.memcachedMock.AssertExpectations(c)
}

func (s *MemcachedTest) newFixture() memcachedTestFixture {
	m := &memcachedMock{}
	return memcachedTestFixture{
		client: memcached{
			ctx:    context.Background(),
			client: m,
			ns:     "test-ns",
		},
		fatalErr:      errors.New("fatal-err"),
		memcachedMock: m,
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
	nsKey := f.client.namespacedKey(regularKey)
	c.Assert(nsKey, Equals, "test-ns:key1")

	longKey := "reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-" +
		"reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-" +
		"reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-" +
		"reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey"

	nsLongKey := f.client.namespacedKey(longKey)
	c.Assert(len(nsLongKey) < 250, IsTrue)
	c.Assert(nsLongKey, Equals, "test-ns:reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-"+
		"reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-reallylongkey-"+
		"reallylongkey-reallylongDGy/3SH4iRDYHL7m7ASbK/WsCAkBQ4YBHdu4F1Yl0a8=")
}

func (s *MemcachedTest) TestAdd(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Add", &memcache.Item{
		Key:        "test-ns:key",
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
		Key:        "test-ns:key1",
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Add", &memcache.Item{
		Key:        "test-ns:key2",
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
		Key:        "test-ns:key1",
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Add", &memcache.Item{
		Key:        "test-ns:key2",
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
		Key:        "test-ns:key",
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

	f.memcachedMock.On("Delete", "test-ns:key").Return(nil).Once()

	err := f.client.Delete("key")

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestDeleteMulti(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Delete", "test-ns:key1").Return(nil).Once()
	f.memcachedMock.On("Delete", "test-ns:key2").Return(nil).Once()

	err := f.client.DeleteMulti([]string{"key1", "key2"})

	c.Assert(err, IsNil)
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestDeleteMultiError(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Delete", "test-ns:key1").Return(f.fatalErr).Once()
	f.memcachedMock.On("Delete", "test-ns:key2").Return(nil).Once()

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

	f.memcachedMock.On("Get", "test-ns:key").Return(&memcache.Item{
		Key:        "test-ns:key",
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
		"test-ns:key1",
		"test-ns:key2",
	}).Return(map[string]*memcache.Item{
		"test-ns:key1": {
			Key:        "test-ns:key1",
			Value:      []byte("aaaah1"),
			Expiration: 5,
		},
		"test-ns:key2": {
			Key:        "test-ns:key2",
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

	f.memcachedMock.On("Increment", "test-ns:key", uint64(5)).Return(uint64(12), nil).Once()

	amt, err := f.client.Increment("key", 5, 10)
	c.Assert(err, IsNil)
	c.Assert(amt, Equals, uint64(12))
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestIncrement_NotInCache(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Increment", "test-ns:key", uint64(5)).Return(uint64(0), ErrCacheMiss).Once()
	f.memcachedMock.On("Add", &memcache.Item{
		Key:   "test-ns:key",
		Value: []byte("10"),
	}).Return(nil).Once()
	f.memcachedMock.On("Increment", "test-ns:key", uint64(5)).Return(uint64(15), nil).Once()

	amt, err := f.client.Increment("key", 5, 10)
	c.Assert(err, IsNil)
	c.Assert(amt, Equals, uint64(15))
	f.assertExpectations(c)
}

func (s *MemcachedTest) TestIncrementExisting(c *C) {
	f := s.newFixture()

	f.memcachedMock.On("Increment", "test-ns:key", uint64(5)).Return(uint64(12), nil).Once()

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
		Key:        "test-ns:key",
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
		Key:        "test-ns:key1",
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Set", &memcache.Item{
		Key:        "test-ns:key2",
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
		Key:        "test-ns:key1",
		Value:      []byte("aaaah1"),
		Expiration: 3,
	}).Return(nil).Once()
	f.memcachedMock.On("Set", &memcache.Item{
		Key:        "test-ns:key2",
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
