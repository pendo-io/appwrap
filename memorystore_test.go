package appwrap

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/mock"
	. "gopkg.in/check.v1"
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
	redisClients = &[]redisClientInterface{&redisClientMock{}, &redisClientMock{}}
}

func (s *MemorystoreTest) newMemstore() (Memorystore, []*redisClientMock) {
	mocks := []*redisClientMock{(*redisClients)[0].(*redisClientMock), (*redisClients)[1].(*redisClientMock)}
	return NewAppengineMemcache(context.Background(), []string{"1.2.3.4", "5.6.7.8"}).Namespace("test-ns").(Memorystore), mocks
}

func (s *MemorystoreTest) TestNewAppengineMemcacheThreadSafety(c *C) {
	// for just this test, we want redisClient to be nil
	redisClients = nil

	// don't want to instantiate GCP object, so set redisAddrs
	redisAddrs = []string{"1.2.3.4:1234"}

	wg := &sync.WaitGroup{}
	startingLine := make(chan struct{})
	numGoroutines := 20000
	msClients := make([]Memorystore, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			// Start all goroutines at once
			<-startingLine
			msClients[i] = NewAppengineMemcache(context.Background(), []string{"1.2.3.4"}).(Memorystore)
		}()
	}
	close(startingLine)
	wg.Wait()

	authoritativeClients := msClients[0].clients
	c.Assert(len(authoritativeClients), Equals, 1)
	for i := 0; i < numGoroutines; i++ {
		c.Assert(len(msClients[i].clients), Equals, 1)
		// should be the exact same pointer
		c.Assert(msClients[i].clients[0], Equals, authoritativeClients[0])
	}
}

func (s *MemorystoreTest) TestNamespacedKeyAndShard(c *C) {
	ms, _ := s.newMemstore()

	fullKey, shard := ms.namespacedKeyAndShard("banana1")
	c.Assert(fullKey, Equals, "test-ns:banana1")
	c.Assert(shard, Equals, 0)

	fullKey, shard = ms.namespacedKeyAndShard("banana")
	c.Assert(fullKey, Equals, "test-ns:banana")
	c.Assert(shard, Equals, 1)

	// this value tests that our sharding algorithm connects for negative number results from the mod operator.
	_, shard = ms.namespacedKeyAndShard(":asdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdfasdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdf:asdfasdfasdfasdf")
	c.Assert(shard, Equals, 1)
}

func (s *MemorystoreTest) TestAdd(c *C) {
	ms, clientMocks := s.newMemstore()

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// success case
	item := &CacheItem{
		Key:        "banana",
		Value:      []byte("apple"),
		Expiration: time.Duration(0),
	}
	fullKey, _ := ms.namespacedKeyAndShard(item.Key)
	clientMocks[1].On("SetNX", fullKey, item.Value, item.Expiration).Return(true, nil).Once()
	err := ms.Add(item)
	c.Assert(err, IsNil)
	checkMocks()

	// not added because it already exists
	clientMocks[1].On("SetNX", fullKey, item.Value, item.Expiration).Return(false, nil).Once()
	err = ms.Add(item)
	c.Assert(err, Equals, CacheErrNotStored)
	checkMocks()

	// other error
	fatalErr := errors.New("aaaah!")
	clientMocks[1].On("SetNX", fullKey, item.Value, item.Expiration).Return(false, fatalErr).Once()
	err = ms.Add(item)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestAddMulti(c *C) {
	ms, clientMocks := s.newMemstore()
	pipeMock0 := &redisPipelineMock{}
	pipeMock1 := &redisPipelineMock{}
	resultMock0 := &boolCmdMock{}
	resultMock1 := &boolCmdMock{}
	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
		pipeMock0.AssertExpectations(c)
		pipeMock1.AssertExpectations(c)
		resultMock0.AssertExpectations(c)
		resultMock1.AssertExpectations(c)
	}

	// success case
	items := []*CacheItem{
		{
			Key:        "banana1",
			Value:      []byte("apple"),
			Expiration: time.Duration(0),
		},
		{
			Key:        "pear1",
			Value:      []byte("blueberry"),
			Expiration: time.Duration(0),
		},
	}
	fullKey0, _ := ms.namespacedKeyAndShard(items[0].Key)
	fullKey1, _ := ms.namespacedKeyAndShard(items[1].Key)
	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	clientMocks[1].On("TxPipeline").Return(pipeMock1).Once()
	pipeMock0.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock1.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock0.On("Exec").Return([]redis.Cmder{resultMock0}, nil).Once()
	pipeMock1.On("Exec").Return([]redis.Cmder{resultMock1}, nil).Once()
	resultMock0.On("Result").Return(true, nil).Once()
	resultMock1.On("Result").Return(true, nil).Once()
	err := ms.AddMulti(items)
	c.Assert(err, IsNil)
	checkMocks()

	// only storing items in one shard, test boundary condition
	items = []*CacheItem{
		{
			Key:        "banana1",
			Value:      []byte("apple"),
			Expiration: time.Duration(0),
		},
	}
	fullKey0, _ = ms.namespacedKeyAndShard(items[0].Key)
	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	pipeMock0.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock0.On("Exec").Return([]redis.Cmder{resultMock0}, nil).Once()
	resultMock0.On("Result").Return(true, nil).Once()
	err = ms.AddMulti(items)
	c.Assert(err, IsNil)
	checkMocks()

	// error storing second item, already exists
	items = []*CacheItem{
		{
			Key:        "banana1",
			Value:      []byte("apple"),
			Expiration: time.Duration(0),
		},
		{
			Key:        "pear1",
			Value:      []byte("blueberry"),
			Expiration: time.Duration(0),
		},
	}
	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	clientMocks[1].On("TxPipeline").Return(pipeMock1).Once()
	pipeMock0.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock1.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock0.On("Exec").Return([]redis.Cmder{resultMock0}, nil).Once()
	pipeMock1.On("Exec").Return([]redis.Cmder{resultMock1}, nil).Once()
	resultMock0.On("Result").Return(true, nil).Once()
	resultMock1.On("Result").Return(false, nil).Once()
	err = ms.AddMulti(items)
	c.Assert(err, DeepEquals, MultiError{nil, CacheErrNotStored})
	checkMocks()

	// fatal error storing first item
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	clientMocks[1].On("TxPipeline").Return(pipeMock1).Once()
	pipeMock0.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock1.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock0.On("Exec").Return([]redis.Cmder{resultMock0}, nil).Once()
	pipeMock1.On("Exec").Return([]redis.Cmder{resultMock1}, nil).Once()
	resultMock0.On("Result").Return(false, fatalErr).Once()
	resultMock1.On("Result").Return(true, nil).Once()
	err = ms.AddMulti(items)
	c.Assert(err, DeepEquals, MultiError{fatalErr, nil})
	checkMocks()

	// error on exec

	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	clientMocks[1].On("TxPipeline").Return(pipeMock1).Once()
	pipeMock0.On("SetNX", fullKey0, items[0].Value, items[0].Expiration).Once()
	pipeMock1.On("SetNX", fullKey1, items[1].Value, items[1].Expiration).Once()
	pipeMock0.On("Exec").Return([]redis.Cmder{resultMock0}, fatalErr).Once()
	pipeMock1.On("Exec").Return([]redis.Cmder{resultMock1}, nil).Once()
	err = ms.AddMulti(items)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestCompareAndSwap(c *C) {
	ms, clientMocks := s.newMemstore()
	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// success case
	item := &CacheItem{
		Key:        "banana1",
		Value:      []byte("apple"),
		Expiration: time.Duration(0),
	}
	fullKey, _ := ms.namespacedKeyAndShard(item.Key)
	clientMocks[0].On("Watch", mock.Anything, []string{fullKey}).Return(nil).Once()
	err := ms.CompareAndSwap(item)
	c.Assert(err, IsNil)
	checkMocks()

	// TxFailedErr should become CASConflict
	clientMocks[0].On("Watch", mock.Anything, []string{fullKey}).Return(redis.TxFailedErr).Once()
	err = ms.CompareAndSwap(item)
	c.Assert(err, Equals, CacheErrCASConflict)
	checkMocks()

	// other error
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("Watch", mock.Anything, []string{fullKey}).Return(fatalErr).Once()
	err = ms.CompareAndSwap(item)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestDoCompareAndSwap(c *C) {
	ms, clientMocks := s.newMemstore()
	pipeMock := &redisPipelineMock{}
	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
		pipeMock.AssertExpectations(c)
	}

	// success case
	item := &CacheItem{
		Key:            "banana1",
		Value:          []byte("apple"),
		valueOnLastGet: []byte("banana"),
		Expiration:     time.Duration(0),
	}
	fullKey, _ := ms.namespacedKeyAndShard(item.Key)
	clientMocks[0].On("Get", fullKey).Return([]byte("banana"), nil).Once()
	clientMocks[0].On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("Set", fullKey, item.Value, item.Expiration).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), nil).Once()
	err := ms.doCompareAndSwap(item, clientMocks[0], fullKey)
	c.Assert(err, IsNil)
	checkMocks()

	// Item does not exist
	clientMocks[0].On("Get", fullKey).Return(([]byte)(nil), ErrCacheMiss).Once()
	err = ms.doCompareAndSwap(item, clientMocks[0], fullKey)
	c.Assert(err, Equals, CacheErrNotStored)
	checkMocks()

	// Other error on Get
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("Get", fullKey).Return(([]byte)(nil), fatalErr).Once()
	err = ms.doCompareAndSwap(item, clientMocks[0], fullKey)
	c.Assert(err, Equals, fatalErr)
	checkMocks()

	// Value modified outside of transaction, cannot CAS
	clientMocks[0].On("Get", fullKey).Return([]byte("not-a-banana"), nil).Once()
	err = ms.doCompareAndSwap(item, clientMocks[0], fullKey)
	c.Assert(err, Equals, CacheErrCASConflict)
	checkMocks()

	// Exec error, should be returned
	clientMocks[0].On("Get", fullKey).Return([]byte("banana"), nil).Once()
	clientMocks[0].On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("Set", fullKey, item.Value, item.Expiration).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), fatalErr).Once()
	err = ms.doCompareAndSwap(item, clientMocks[0], fullKey)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestDelete(c *C) {
	ms, clientMocks := s.newMemstore()

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// success case
	key := "banana1"
	fullKey, _ := ms.namespacedKeyAndShard(key)
	clientMocks[0].On("Del", []string{fullKey}).Return(nil).Once()
	err := ms.Delete(key)
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("Del", []string{fullKey}).Return(fatalErr).Once()
	err = ms.Delete(key)
	c.Assert(err, DeepEquals, MultiError{fatalErr})
	checkMocks()
}

func (s *MemorystoreTest) TestDeleteMulti(c *C) {
	ms, clientMocks := s.newMemstore()

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// success case
	key0 := "banana1"
	fullKey0, _ := ms.namespacedKeyAndShard(key0)
	key1 := "pear1"
	fullKey1, _ := ms.namespacedKeyAndShard(key1)
	clientMocks[0].On("Del", []string{fullKey0}).Return(nil).Once()
	clientMocks[1].On("Del", []string{fullKey1}).Return(nil).Once()
	err := ms.DeleteMulti([]string{key0, key1})
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("Del", []string{fullKey0}).Return(fatalErr).Once()
	clientMocks[1].On("Del", []string{fullKey1}).Return(nil).Once()
	err = ms.DeleteMulti([]string{key0, key1})
	c.Assert(err, DeepEquals, MultiError{fatalErr})
	checkMocks()
}

func (s *MemorystoreTest) TestFlush(c *C) {
	ms, _ := s.newMemstore()
	fatalErr := errors.New("please don't call this on memorystore")
	err := ms.Flush()
	c.Assert(err, DeepEquals, fatalErr)

	/*
		Leaving this here to show how you test flush. It is currently disabled because flush brings down memorystore for the duration of this operation.

		ms, clientMocks := s.newMemstore()

		checkMocks := func() {
			clientMocks[0].AssertExpectations(c)
			clientMocks[1].AssertExpectations(c)
		}

		// success case
		clientMocks[0].On("FlushAll").Return(nil).Once()
		clientMocks[1].On("FlushAll").Return(nil).Once()
		err := ms.Flush()
		c.Assert(err, IsNil)
		checkMocks()

		// error case
		fatalErr := errors.New("aaaah")
		clientMocks[0].On("FlushAll").Return(fatalErr).Once()
		clientMocks[1].On("FlushAll").Return(nil).Once()
		err = ms.Flush()
		c.Assert(err, DeepEquals, MultiError{fatalErr})
		checkMocks()
	*/
}

func (s *MemorystoreTest) TestGet(c *C) {
	ms, clientMocks := s.newMemstore()

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// success case
	key := "pear1"
	val := []byte("woohoo")
	fullKey, _ := ms.namespacedKeyAndShard(key)
	clientMocks[1].On("Get", fullKey).Return(val, nil).Once()
	item, err := ms.Get(key)
	c.Assert(err, IsNil)
	c.Assert(item, DeepEquals, &CacheItem{
		Key:            key,
		Value:          val,
		valueOnLastGet: val,
	})
	c.Assert(item.Value, Not(Equals), item.valueOnLastGet)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMocks[1].On("Get", fullKey).Return(val, fatalErr).Once()
	item, err = ms.Get(key)
	c.Assert(err, Equals, fatalErr)
	c.Assert(item, IsNil)
	checkMocks()
}

func (s *MemorystoreTest) TestGetMulti(c *C) {
	ms, clientMocks := s.newMemstore()

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// success case
	keys := []string{"apple", "banana", "pear", "pineapple"}
	nsKey0, _ := ms.namespacedKeyAndShard(keys[0])
	nsKey1, _ := ms.namespacedKeyAndShard(keys[1])
	nsKey2, _ := ms.namespacedKeyAndShard(keys[2])
	nsKey3, _ := ms.namespacedKeyAndShard(keys[3])
	shard0Keys := []string{nsKey2, nsKey3}
	shard1Keys := []string{nsKey0, nsKey1}

	shard0Vals := []interface{}{
		[]byte("pear tree"),
		"pineapple shrub thing", // getMulti can return strings instead of byte slices
	}
	shard1Vals := []interface{}{
		[]byte("apple tree"),
		nil, // indicates not found
	}
	clientMocks[0].On("MGet", shard0Keys).Return(shard0Vals, nil).Once()
	clientMocks[1].On("MGet", shard1Keys).Return(shard1Vals, nil).Once()
	results, err := ms.GetMulti(keys)
	c.Assert(err, IsNil)
	c.Assert(results, DeepEquals, map[string]*CacheItem{
		"apple": {
			Key:            keys[0],
			Value:          shard1Vals[0].([]byte),
			valueOnLastGet: shard1Vals[0].([]byte),
		},
		"pear": {
			Key:            keys[2],
			Value:          shard0Vals[0].([]byte),
			valueOnLastGet: shard0Vals[0].([]byte),
		},
		"pineapple": {
			Key:            keys[3],
			Value:          []byte(shard0Vals[1].(string)),
			valueOnLastGet: []byte(shard0Vals[1].(string)),
		},
	})
	c.Assert(results["apple"].Value, Not(Equals), results["apple"].valueOnLastGet)
	c.Assert(results["pear"].Value, Not(Equals), results["pear"].valueOnLastGet)
	checkMocks()

	// test getting keys all in same shard
	keys = []string{"pear", "pineapple"}
	nsKey0, _ = ms.namespacedKeyAndShard(keys[0])
	nsKey1, _ = ms.namespacedKeyAndShard(keys[1])
	shard0Keys = []string{nsKey0, nsKey1}

	shard0Vals = []interface{}{
		nil,
		"pineapple shrub thing", // getMulti can return strings instead of byte slices
	}
	clientMocks[0].On("MGet", shard0Keys).Return(shard0Vals, nil).Once()
	results, err = ms.GetMulti(keys)
	c.Assert(err, IsNil)
	c.Assert(results, DeepEquals, map[string]*CacheItem{
		"pineapple": {
			Key:            keys[1],
			Value:          []byte(shard0Vals[1].(string)),
			valueOnLastGet: []byte(shard0Vals[1].(string)),
		},
	})
	c.Assert(results["pineapple"].Value, Not(Equals), results["pineapple"].valueOnLastGet)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("MGet", shard0Keys).Return(([]interface{})(nil), fatalErr).Once()
	results, err = ms.GetMulti(keys)
	c.Assert(err, Equals, fatalErr)
	c.Assert(results, IsNil)
	checkMocks()
}

func (s *MemorystoreTest) TestIncrement(c *C) {
	ms, clientMocks := s.newMemstore()
	calledResultMock := &intCmdMock{}
	pipeMock := &redisPipelineMock{}
	uncalledResultMock := &intCmdMock{}

	checkMocks := func() {
		calledResultMock.AssertExpectations(c)
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
		pipeMock.AssertExpectations(c)
		uncalledResultMock.AssertExpectations(c)
	}

	// success case
	key := "banana1"
	fullKey, _ := ms.namespacedKeyAndShard(key)
	initialValue := uint64(1)
	amount := int64(5)
	clientMocks[0].On("TxPipeline").Return(pipeMock).Once()
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
	clientMocks[0].On("TxPipeline").Return(pipeMock).Once()
	pipeMock.On("SetNX", fullKey, initialValue, time.Duration(0)).Once()
	pipeMock.On("IncrBy", fullKey, amount).Once()
	pipeMock.On("Exec").Return(([]redis.Cmder)(nil), fatalErr).Once()
	incr, err = ms.Increment(key, amount, initialValue)
	c.Assert(err, Equals, fatalErr)
	c.Assert(incr, Equals, uint64(0))
	checkMocks()
}

func (s *MemorystoreTest) TestIncrementExisting(c *C) {
	ms, clientMocks := s.newMemstore()

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// exists case, no error
	key := "banana"
	fullKey, _ := ms.namespacedKeyAndShard(key)
	amount := int64(10)
	clientMocks[1].On("Exists", []string{fullKey}).Return(int64(1), nil).Once()
	clientMocks[1].On("IncrBy", fullKey, amount).Return(int64(10), nil).Once()
	val, err := ms.IncrementExisting(key, amount)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, uint64(10))
	checkMocks()

	// exists case, error on increment
	fatalErr := errors.New("aaaah")
	clientMocks[1].On("Exists", []string{fullKey}).Return(int64(1), nil).Once()
	clientMocks[1].On("IncrBy", fullKey, amount).Return(int64(0), fatalErr).Once()
	val, err = ms.IncrementExisting(key, amount)
	c.Assert(err, Equals, fatalErr)
	c.Assert(val, Equals, uint64(0))
	checkMocks()

	// does not exist
	clientMocks[1].On("Exists", []string{fullKey}).Return(int64(0), nil).Once()
	val, err = ms.IncrementExisting(key, amount)
	c.Assert(err, Equals, ErrCacheMiss)
	c.Assert(val, Equals, uint64(0))
	checkMocks()

	// fatal error
	clientMocks[1].On("Exists", []string{fullKey}).Return(int64(0), fatalErr).Once()
	val, err = ms.IncrementExisting(key, amount)
	c.Assert(err, Equals, fatalErr)
	c.Assert(val, Equals, uint64(0))
	checkMocks()
}

func (s *MemorystoreTest) TestSet(c *C) {
	ms, clientMocks := s.newMemstore()

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
	}

	// success case
	key := "banana1"
	fullKey, _ := ms.namespacedKeyAndShard(key)
	value := []byte("banana tree")
	item := &CacheItem{
		Key:        key,
		Value:      value,
		Expiration: time.Duration(0),
	}
	clientMocks[0].On("Set", fullKey, value, time.Duration(0)).Return(nil).Once()
	err := ms.Set(item)
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("Set", fullKey, value, time.Duration(0)).Return(fatalErr).Once()
	err = ms.Set(item)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestSetMulti(c *C) {
	ms, clientMocks := s.newMemstore()
	pipeMock0 := &redisPipelineMock{}
	pipeMock1 := &redisPipelineMock{}

	checkMocks := func() {
		clientMocks[0].AssertExpectations(c)
		clientMocks[1].AssertExpectations(c)
		pipeMock0.AssertExpectations(c)
		pipeMock1.AssertExpectations(c)
	}

	// success case
	key0 := "banana1"
	key1 := "apple"
	fullKey0, _ := ms.namespacedKeyAndShard(key0)
	fullKey1, _ := ms.namespacedKeyAndShard(key1)
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
	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	clientMocks[1].On("TxPipeline").Return(pipeMock1).Once()
	pipeMock0.On("Set", fullKey0, value0, time.Duration(0)).Once()
	pipeMock1.On("Set", fullKey1, value1, time.Duration(1)).Once()
	pipeMock0.On("Exec").Return(([]redis.Cmder)(nil), nil).Once()
	pipeMock1.On("Exec").Return(([]redis.Cmder)(nil), nil).Once()
	err := ms.SetMulti(items)
	c.Assert(err, IsNil)
	checkMocks()

	// only set keys in one shard
	items = []*CacheItem{
		{
			Key:        key0,
			Value:      value0,
			Expiration: time.Duration(0),
		},
	}
	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	pipeMock0.On("Set", fullKey0, value0, time.Duration(0)).Once()
	pipeMock0.On("Exec").Return(([]redis.Cmder)(nil), nil).Once()
	err = ms.SetMulti(items)
	c.Assert(err, IsNil)
	checkMocks()

	// error case
	fatalErr := errors.New("aaaah")
	clientMocks[0].On("TxPipeline").Return(pipeMock0).Once()
	pipeMock0.On("Set", fullKey0, value0, time.Duration(0)).Once()
	pipeMock0.On("Exec").Return(([]redis.Cmder)(nil), fatalErr).Once()
	err = ms.SetMulti(items)
	c.Assert(err, Equals, fatalErr)
	checkMocks()
}

func (s *MemorystoreTest) TestNamespace(c *C) {
	ms := NewAppengineMemcache(context.Background(), []string{"1.2.3.4"}).(Memorystore)
	msNewNamespace := ms.Namespace("test-ns").(Memorystore)
	// original ns not modified
	c.Assert(ms.namespace, Equals, "")
	// new ns check
	c.Assert(msNewNamespace.namespace, Equals, "test-ns")
	// want the exact same pointers in other fields
	c.Assert(ms.c, Equals, msNewNamespace.c)
	c.Assert(ms.clients[0], Equals, msNewNamespace.clients[0])
}
