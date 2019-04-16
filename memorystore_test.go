//+build memorystore

package appwrap

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/mock"
	"google.golang.org/appengine"
	. "gopkg.in/check.v1"
	"pendo.io/brace"
)

type redisClientMock struct {
	mock.Mock
}

func (m *redisClientMock) Del(keys ...string) error {
	args := m.Called(keys)
	return args.Error(0)
}

func (m *redisClientMock) Exists(keys ...string) (int64, error) {
	args := m.Called(keys)
	return args.Get(0).(int64), args.Error(1)
}

func (m *redisClientMock) FlushAll() error {
	args := m.Called()
	return args.Error(0)
}

func (m *redisClientMock) Get(key string) ([]byte, error) {
	args := m.Called(key)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *redisClientMock) IncrBy(key string, value int64) (int64, error) {
	args := m.Called(key, value)
	return args.Get(0).(int64), args.Error(1)
}

func (m *redisClientMock) MGet(keys ...string) ([]interface{}, error) {
	args := m.Called(keys)
	return args.Get(0).([]interface{}), args.Error(1)
}

func (m *redisClientMock) Set(key string, value interface{}, expiration time.Duration) error {
	args := m.Called(key, value, expiration)
	return args.Error(0)
}

func (m *redisClientMock) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	args := m.Called(key, value, expiration)
	return args.Get(0).(bool), args.Error(1)
}

func (m *redisClientMock) TxPipeline() redisPipelineInterface {
	args := m.Called()
	return args.Get(0).(redisPipelineInterface)
}

func (m *redisClientMock) Watch(fn func(*redis.Tx) error, keys ...string) error {
	args := m.Called(fn, keys)
	return args.Error(0)
}

type redisPipelineMock struct {
	mock.Mock
}

func (m *redisPipelineMock) Exec() ([]redis.Cmder, error) {
	args := m.Called()
	return args.Get(0).([]redis.Cmder), args.Error(1)
}

func (m *redisPipelineMock) IncrBy(key string, value int64) {
	m.Called(key, value)
}

func (m *redisPipelineMock) Set(key string, value interface{}, expiration time.Duration) {
	m.Called(key, value, expiration)
}

func (m *redisPipelineMock) SetNX(key string, value interface{}, expiration time.Duration) {
	m.Called(key, value, expiration)
}

type intCmdMock struct {
	mock.Mock
	redis.Cmder
}

func (m *intCmdMock) Val() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

type boolCmdMock struct {
	mock.Mock
	redis.Cmder
}

func (m *boolCmdMock) Result() (bool, error) {
	args := m.Called()
	return args.Get(0).(bool), args.Error(1)
}

type MemorystoreTest struct{}

var _ = Suite(&MemorystoreTest{})

func (s *MemorystoreTest) SetUpTest(c *C) {
	redisClient = &redisClientMock{}
}

func (s *MemorystoreTest) TestNewAppengineMemcacheThreadSafety(c *C) {
	// for just this test, we want redisClient to be nil
	redisClient = nil

	// don't want to instantiate GCP object, so set redisAddr
	redisAddr = "1.2.3.4:1234"

	wg := &sync.WaitGroup{}
	startingLine := make(chan struct{})
	numGoroutines := 20000
	clients := make([]Memorystore, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			// Start all goroutines at once
			<-startingLine
			clients[i] = NewAppengineMemcache(context.Background(), "", "").(Memorystore)
		}()
	}
	close(startingLine)
	wg.Wait()

	authoritativeClient := clients[0].client
	for i := 0; i < numGoroutines; i++ {
		// should be the exact same pointer
		c.Assert(clients[i].client, Equals, authoritativeClient)
	}
}

func (s *MemorystoreTest) TestBuildNamespacedKey(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	fullKey := ms.buildNamespacedKey("banana")
	c.Assert(fullKey, Equals, "test-ns:banana")
}

func (s *MemorystoreTest) TestAdd(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	item := &CacheItem{
		Key:        "banana",
		Value:      []byte("apple"),
		Expiration: time.Duration(0),
	}
	fullKey := ms.buildNamespacedKey(item.Key)
	clientMock.On("SetNX", fullKey, item.Value, item.Expiration).Return(true, nil).Once()
	err := ms.Add(item)
	c.Assert(err, IsNil)
	checkMocks()

	// not added because it already exists
	clientMock.On("SetNX", fullKey, item.Value, item.Expiration).Return(false, nil).Once()
	err = ms.Add(item)
	c.Assert(err, Equals, CacheErrNotStored)
	checkMocks()

	// other error
	fatalErr := errors.New("aaaah!")
	clientMock.On("SetNX", fullKey, item.Value, item.Expiration).Return(false, fatalErr).Once()
	err = ms.Add(item)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestAddMulti(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)
	pipeMock := &redisPipelineMock{}
	resultMock0 := &boolCmdMock{}
	resultMock1 := &boolCmdMock{}
	checkMocks := func() {
		clientMock.AssertExpectations(c)
		pipeMock.AssertExpectations(c)
		resultMock0.AssertExpectations(c)
		resultMock1.AssertExpectations(c)
	}

	// success case
	items := []*CacheItem{
		{
			Key:        "banana",
			Value:      []byte("apple"),
			Expiration: time.Duration(0),
		},
		{
			Key:        "pear",
			Value:      []byte("blueberry"),
			Expiration: time.Duration(0),
		},
	}
	fullKey0 := ms.buildNamespacedKey(items[0].Key)
	fullKey1 := ms.buildNamespacedKey(items[1].Key)
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock.On("Exec").Return([]redis.Cmder{resultMock0, resultMock1}, nil).Once()
	resultMock0.On("Result").Return(true, nil).Once()
	resultMock1.On("Result").Return(true, nil).Once()
	err := ms.AddMulti(items)
	c.Assert(err, IsNil)
	checkMocks()

	// error storing second item, already exists
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock.On("Exec").Return([]redis.Cmder{resultMock0, resultMock1}, nil).Once()
	resultMock0.On("Result").Return(true, nil).Once()
	resultMock1.On("Result").Return(false, nil).Once()
	err = ms.AddMulti(items)
	c.Assert(err, DeepEquals, appengine.MultiError{nil, CacheErrNotStored})
	checkMocks()

	// fatal error storing first item
	fatalErr := errors.New("aaaah")
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock.On("Exec").Return([]redis.Cmder{resultMock0, resultMock1}, nil).Once()
	resultMock0.On("Result").Return(false, fatalErr).Once()
	resultMock1.On("Result").Return(true, nil).Once()
	err = ms.AddMulti(items)
	c.Assert(err, DeepEquals, appengine.MultiError{fatalErr, nil})
	checkMocks()

	// error on exec

	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock.On("Exec").Return([]redis.Cmder{resultMock0, resultMock1}, fatalErr).Once()
	err = ms.AddMulti(items)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestCompareAndSwap(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)
	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	item := &CacheItem{
		Key:        "banana",
		Value:      []byte("apple"),
		Expiration: time.Duration(0),
	}
	fullKey := ms.buildNamespacedKey(item.Key)
	clientMock.On("Watch", mock.Anything, []string{fullKey}).Return(nil).Once()
	err := ms.CompareAndSwap(item)
	c.Assert(err, IsNil)
	checkMocks()

	// TxFailedErr should become CASConflict
	clientMock.On("Watch", mock.Anything, []string{fullKey}).Return(redis.TxFailedErr).Once()
	err = ms.CompareAndSwap(item)
	c.Assert(err, Equals, CacheErrCASConflict)
	checkMocks()

	// other error
	fatalErr := errors.New("aaaah")
	clientMock.On("Watch", mock.Anything, []string{fullKey}).Return(fatalErr).Once()
	err = ms.CompareAndSwap(item)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestDoCompareAndSwap(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)
	pipeMock := &redisPipelineMock{}
	checkMocks := func() {
		clientMock.AssertExpectations(c)
		pipeMock.AssertExpectations(c)
	}

	// success case
	item := &CacheItem{
		Key:            "banana",
		Value:          []byte("apple"),
		valueOnLastGet: []byte("banana"),
		Expiration:     time.Duration(0),
	}
	fullKey := ms.buildNamespacedKey(item.Key)
	clientMock.On("Get", fullKey).Return([]byte("banana"), nil).Once()
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("Set", fullKey, item.Value, item.Expiration).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), nil).Once()
	err := ms.doCompareAndSwap(item, clientMock, fullKey)
	c.Assert(err, IsNil)
	checkMocks()

	// Item does not exist
	clientMock.On("Get", fullKey).Return(([]byte)(nil), ErrCacheMiss).Once()
	err = ms.doCompareAndSwap(item, clientMock, fullKey)
	c.Assert(err, Equals, CacheErrNotStored)
	checkMocks()

	// Other error on Get
	fatalErr := errors.New("aaaah")
	clientMock.On("Get", fullKey).Return(([]byte)(nil), fatalErr).Once()
	err = ms.doCompareAndSwap(item, clientMock, fullKey)
	c.Assert(err, Equals, fatalErr)
	checkMocks()

	// Value modified outside of transaction, cannot CAS
	clientMock.On("Get", fullKey).Return([]byte("not-a-banana"), nil).Once()
	err = ms.doCompareAndSwap(item, clientMock, fullKey)
	c.Assert(err, Equals, CacheErrCASConflict)
	checkMocks()

	// Exec error, should be returned
	clientMock.On("Get", fullKey).Return([]byte("banana"), nil).Once()
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("Set", fullKey, item.Value, item.Expiration).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), fatalErr).Once()
	err = ms.doCompareAndSwap(item, clientMock, fullKey)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestDelete(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	key := "banana"
	fullKey := ms.buildNamespacedKey(key)
	clientMock.On("Del", []string{fullKey}).Return(nil).Once()
	err := ms.Delete(key)
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("Del", []string{fullKey}).Return(fatalErr).Once()
	err = ms.Delete(key)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestDeleteMulti(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	key0 := "banana"
	fullKey0 := ms.buildNamespacedKey(key0)
	key1 := "apple"
	fullKey1 := ms.buildNamespacedKey(key1)
	clientMock.On("Del", []string{fullKey0, fullKey1}).Return(nil).Once()
	err := ms.DeleteMulti([]string{key0, key1})
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("Del", []string{fullKey0, fullKey1}).Return(fatalErr).Once()
	err = ms.DeleteMulti([]string{key0, key1})
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestFlush(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	clientMock.On("FlushAll").Return(nil).Once()
	err := ms.Flush()
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("FlushAll").Return(fatalErr).Once()
	err = ms.Flush()
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestGet(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	key := "banana"
	val := []byte("woohoo")
	fullKey := ms.buildNamespacedKey(key)
	clientMock.On("Get", fullKey).Return(val, nil).Once()
	item, err := ms.Get(key)
	c.Assert(err, IsNil)
	c.Assert(item, DeepEquals, &CacheItem{
		Key:            key,
		Value:          val,
		valueOnLastGet: val,
	})
	c.Assert(item.Value, Not(brace.SameMemoryAs), item.valueOnLastGet)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("Get", fullKey).Return(val, fatalErr).Once()
	item, err = ms.Get(key)
	c.Assert(err, Equals, fatalErr)
	c.Assert(item, IsNil)
	checkMocks()
}

func (s *MemorystoreTest) TestGetMulti(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	keys := []string{"apple", "banana", "pear"}
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = ms.buildNamespacedKey(key)
	}
	vals := []interface{}{
		[]byte("apple tree"),
		nil, // indicates not found
		[]byte("pear tree"),
	}
	clientMock.On("MGet", fullKeys).Return(vals, nil).Once()
	results, err := ms.GetMulti(keys)
	c.Assert(err, IsNil)
	c.Assert(results, DeepEquals, map[string]*CacheItem{
		"apple": {
			Key:            keys[0],
			Value:          vals[0].([]byte),
			valueOnLastGet: vals[0].([]byte),
		},
		"pear": {
			Key:            keys[2],
			Value:          vals[2].([]byte),
			valueOnLastGet: vals[2].([]byte),
		},
	})
	c.Assert(results["apple"].Value, Not(brace.SameMemoryAs), results["apple"].valueOnLastGet)
	c.Assert(results["pear"].Value, Not(brace.SameMemoryAs), results["pear"].valueOnLastGet)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("MGet", fullKeys).Return(([]interface{})(nil), fatalErr).Once()
	results, err = ms.GetMulti(keys)
	c.Assert(err, Equals, fatalErr)
	c.Assert(results, IsNil)
	checkMocks()
}

func (s *MemorystoreTest) TestIncrement(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	calledResultMock := &intCmdMock{}
	clientMock := ms.client.(*redisClientMock)
	pipeMock := &redisPipelineMock{}
	uncalledResultMock := &intCmdMock{}

	checkMocks := func() {
		calledResultMock.AssertExpectations(c)
		clientMock.AssertExpectations(c)
		pipeMock.AssertExpectations(c)
		uncalledResultMock.AssertExpectations(c)
	}

	// success case
	key := "banana"
	fullKey := ms.buildNamespacedKey(key)
	initialValue := uint64(1)
	amount := int64(5)
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("SetNX", fullKey, initialValue, time.Duration(0)).Once()
	pipeMock.On("IncrBy", fullKey, amount).Once()
	pipeMock.On("Exec").Return([]redis.Cmder{
		uncalledResultMock,
		calledResultMock,
	}, nil).Once()
	calledResultMock.On("Val").Return(int64(40))
	incr, err := ms.Increment(key, amount, initialValue)
	c.Assert(err, IsNil)
	c.Assert(incr, Equals, uint64(40))
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("SetNX", fullKey, initialValue, time.Duration(0)).Once()
	pipeMock.On("IncrBy", fullKey, amount).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), fatalErr).Once()
	incr, err = ms.Increment(key, amount, initialValue)
	c.Assert(err, Equals, fatalErr)
	c.Assert(incr, Equals, uint64(0))
	checkMocks()
}

func (s *MemorystoreTest) TestIncrementExisting(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// exists case, no error
	key := "banana"
	fullKey := ms.buildNamespacedKey(key)
	amount := int64(10)
	clientMock.On("Exists", []string{fullKey}).Return(int64(1), nil).Once()
	clientMock.On("IncrBy", fullKey, amount).Return(int64(10), nil).Once()
	val, err := ms.IncrementExisting(key, amount)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, uint64(10))
	checkMocks()

	// exists case, error on increment
	fatalErr := errors.New("aaaah")
	clientMock.On("Exists", []string{fullKey}).Return(int64(1), nil).Once()
	clientMock.On("IncrBy", fullKey, amount).Return(int64(0), fatalErr).Once()
	val, err = ms.IncrementExisting(key, amount)
	c.Assert(err, Equals, fatalErr)
	c.Assert(val, Equals, uint64(0))
	checkMocks()

	// does not exist
	clientMock.On("Exists", []string{fullKey}).Return(int64(0), nil).Once()
	val, err = ms.IncrementExisting(key, amount)
	c.Assert(err, Equals, ErrCacheMiss)
	c.Assert(val, Equals, uint64(0))
	checkMocks()

	// fatal error
	clientMock.On("Exists", []string{fullKey}).Return(int64(0), fatalErr).Once()
	val, err = ms.IncrementExisting(key, amount)
	c.Assert(err, Equals, fatalErr)
	c.Assert(val, Equals, uint64(0))
	checkMocks()
}

func (s *MemorystoreTest) TestSet(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	// success case
	key := "banana"
	fullKey := ms.buildNamespacedKey(key)
	value := []byte("banana tree")
	item := &CacheItem{
		Key:        key,
		Value:      value,
		Expiration: time.Duration(0),
	}
	clientMock.On("Set", fullKey, value, time.Duration(0)).Return(nil).Once()
	err := ms.Set(item)
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("Set", fullKey, value, time.Duration(0)).Return(fatalErr).Once()
	err = ms.Set(item)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestSetMulti(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").Namespace("test-ns").(Memorystore)
	clientMock := ms.client.(*redisClientMock)
	pipeMock := &redisPipelineMock{}

	checkMocks := func() {
		clientMock.AssertExpectations(c)
		pipeMock.AssertExpectations(c)
	}

	// success case
	key0 := "banana"
	key1 := "apple"
	fullKey0 := ms.buildNamespacedKey(key0)
	fullKey1 := ms.buildNamespacedKey(key1)
	value0 := []byte("banana tree")
	value1 := []byte("apple tree")
	items := []*CacheItem{
		{
			Key:        key0,
			Value:      value0,
			Expiration: time.Duration(0),
		},
		{
			Key:        key1,
			Value:      value1,
			Expiration: time.Duration(1),
		},
	}
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("Set", fullKey0, value0, time.Duration(0)).Once()
	pipeMock.On("Set", fullKey1, value1, time.Duration(1)).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), nil).Once()
	err := ms.SetMulti(items)
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMock.On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("Set", fullKey0, value0, time.Duration(0)).Once()
	pipeMock.On("Set", fullKey1, value1, time.Duration(1)).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), fatalErr).Once()
	err = ms.SetMulti(items)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestNamespace(c *C) {
	ms := NewAppengineMemcache(context.Background(), "", "").(Memorystore)
	msNewNamespace := ms.Namespace("test-ns").(Memorystore)
	// original ns not modified
	c.Assert(ms.namespace, Equals, "")
	// new ns check
	c.Assert(msNewNamespace.namespace, Equals, "test-ns")
	// want the exact same pointers in other fields
	c.Assert(ms.c, Equals, msNewNamespace.c)
	c.Assert(ms.client, Equals, msNewNamespace.client)
}
