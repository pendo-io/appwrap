//+build memorystore

package appwrap

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	cloudms "cloud.google.com/go/redis/apiv1"
	"github.com/go-redis/redis"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	redispb "google.golang.org/genproto/googleapis/cloud/redis/v1"
)

var ErrCacheMiss = redis.Nil
var CacheErrNotStored = errors.New("item not stored")
var CacheErrCASConflict = errors.New("compare-and-swap conflict")
var CacheErrServerError = errors.New("redis: server error")

type CacheItem struct {
	Key        string
	Value      []byte
	Object     interface{}
	Flags      uint32
	Expiration time.Duration
	// Used for CompareAndSwap, invisible to client
	valueOnLastGet []byte
}

// Implements needed redis methods for mocking purposes.  See *redis.Client for a full list of available methods
// Implementations of these methods convert the returned redis Cmd objects into mockable data by calling
// Err(), Result(), etc.
type redisCommonInterface interface {
	Del(keys ...string) error
	Exists(keys ...string) (int64, error)
	FlushAll() error
	Get(key string) ([]byte, error)
	IncrBy(key string, value int64) (int64, error)
	MGet(keys ...string) ([]interface{}, error)
	Set(key string, value interface{}, expiration time.Duration) error
	SetNX(key string, value interface{}, expiration time.Duration) (bool, error)
	TxPipeline() redisPipelineInterface
}

// Additionally implements Watch for transactions
type redisClientInterface interface {
	redisCommonInterface
	Watch(fn func(*redis.Tx) error, keys ...string) error
}

type redisClientImplementation struct {
	// common is used for all methods defined on redisCommonInterface
	common redis.Cmdable
	// client is used for the redisClientInterface-specific methods
	client *redis.Client
}

func (rci *redisClientImplementation) Del(keys ...string) error {
	return rci.common.Del(keys...).Err()
}

func (rci *redisClientImplementation) Exists(keys ...string) (int64, error) {
	return rci.common.Exists(keys...).Result()
}

func (rci *redisClientImplementation) FlushAll() error {
	return rci.common.FlushAll().Err()
}

func (rci *redisClientImplementation) Get(key string) ([]byte, error) {
	return rci.common.Get(key).Bytes()
}

func (rci *redisClientImplementation) IncrBy(key string, value int64) (int64, error) {
	return rci.common.IncrBy(key, value).Result()
}

func (rci *redisClientImplementation) MGet(keys ...string) ([]interface{}, error) {
	return rci.common.MGet(keys...).Result()
}

func (rci *redisClientImplementation) Set(key string, value interface{}, expiration time.Duration) error {
	return rci.common.Set(key, value, expiration).Err()
}

func (rci *redisClientImplementation) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	return rci.common.SetNX(key, value, expiration).Result()
}

func (rci *redisClientImplementation) TxPipeline() redisPipelineInterface {
	return &redisPipelineImplementation{rci.common.TxPipeline()}
}

// Watch can only be called by the top-level redis Client.  In particular, this means that
// *redis.TX cannot call Watch again - it only implements redis.Cmdable.
func (rci *redisClientImplementation) Watch(fn func(*redis.Tx) error, keys ...string) error {
	return rci.client.Watch(fn, keys...)
}

// Implements needed redis pipeline methods for mocking purposes.  See redis.Pipeliner for all available methods.
type redisPipelineInterface interface {
	Exec() ([]redis.Cmder, error)
	IncrBy(key string, value int64)
	Set(key string, value interface{}, expiration time.Duration)
	SetNX(key string, value interface{}, expiration time.Duration)
}

type redisPipelineImplementation struct {
	pipeline redis.Pipeliner
}

func (rpi *redisPipelineImplementation) Exec() ([]redis.Cmder, error) {
	return rpi.pipeline.Exec()
}

func (rpi *redisPipelineImplementation) IncrBy(key string, value int64) {
	rpi.pipeline.IncrBy(key, value)
}

func (rpi *redisPipelineImplementation) Set(key string, value interface{}, expiration time.Duration) {
	rpi.pipeline.Set(key, value, expiration)
}

func (rpi *redisPipelineImplementation) SetNX(key string, value interface{}, expiration time.Duration) {
	rpi.pipeline.SetNX(key, value, expiration)
}

// Allows mocking of IntCmds returned by redis calls
type intCmdInterface interface {
	Val() int64
}

type boolCmdInterface interface {
	Result() (bool, error)
}

type Memorystore struct {
	c         context.Context
	client    redisClientInterface
	namespace string
}

var (
	redisClientMtx = &sync.Mutex{}
	redisClient    redisClientInterface
	redisAddr      = ""
)

func getRedisAddr(c context.Context) string {
	if redisAddr != "" {
		return redisAddr
	}
	appInfo := NewAppengineInfoFromContext(c)
	client, err := cloudms.NewCloudRedisClient(context.Background())
	if err != nil {
		panic(err)
	}
	defer client.Close()

	projectId := appInfo.AppID()
	locationId := c.Value(KeyCacheLocation)
	instanceId := c.Value(KeyCacheName)

	instance, err := client.GetInstance(c, &redispb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/instances/%s", projectId, locationId, instanceId),
	})
	if err != nil {
		panic(err)
	}

	redisAddr = fmt.Sprintf("%s:%d", instance.Host, instance.Port)
	return redisAddr
}

func NewAppengineMemcache(c context.Context) Memcache {
	if redisClient == nil {
		redisClientMtx.Lock()
		defer redisClientMtx.Unlock()

		// Check again, because another goroutine could have beaten us here while we were checking the first time
		if redisClient == nil {
			client := redis.NewClient(&redis.Options{
				Addr:     getRedisAddr(c),
				Password: "",
				DB:       0,
			}).WithContext(c)
			redisClient = &redisClientImplementation{client, client}
		}
	}
	return Memorystore{c, redisClient, ""}
}

func (ms Memorystore) buildNamespacedKey(key string) string {
	if key == "" {
		panic("redis: blank key")
	}
	return ms.namespace + ":" + key
}

func (ms Memorystore) Add(item *CacheItem) error {
	fullKey := ms.buildNamespacedKey(item.Key)
	if added, err := ms.client.SetNX(fullKey, item.Value, item.Expiration); err != nil {
		return err
	} else if !added {
		return CacheErrNotStored
	}
	return nil

}

func (ms Memorystore) AddMulti(items []*CacheItem) error {
	errList := make(appengine.MultiError, len(items))
	haveErrors := false

	pipe := ms.client.TxPipeline()
	for _, item := range items {
		fullKey := ms.buildNamespacedKey(item.Key)
		pipe.SetNX(fullKey, item.Value, item.Expiration)
	}

	results, err := pipe.Exec()
	if err != nil {
		return err
	}

	for i, result := range results {
		if added, err := result.(boolCmdInterface).Result(); err != nil {
			errList[i] = err
			haveErrors = true
		} else if !added {
			errList[i] = CacheErrNotStored
			haveErrors = true
		}
	}

	if haveErrors {
		return errList
	}

	return nil
}

func (ms Memorystore) CompareAndSwap(item *CacheItem) error {
	fullKey := ms.buildNamespacedKey(item.Key)
	if err := ms.client.Watch(func(tx *redis.Tx) error {
		// Watch is an optimistic lock
		txClient := &redisClientImplementation{tx, nil}
		return ms.doCompareAndSwap(item, txClient, fullKey)
	}, fullKey); err == redis.TxFailedErr {
		return CacheErrCASConflict
	} else {
		return err
	}
}

func (ms Memorystore) doCompareAndSwap(item *CacheItem, tx redisCommonInterface, fullKey string) error {
	val, err := tx.Get(fullKey)
	if err == ErrCacheMiss {
		// Does item exist?  If not, can't swap it
		return CacheErrNotStored
	} else if err != nil {
		return err
	} else if !bytes.Equal(val, item.valueOnLastGet) {
		// Did something change before we entered the transaction?
		// If so, already too late to swap
		return CacheErrCASConflict
	}

	// Finally, attempt the swap.  This will fail if something else beats us there, since we're in a transaction
	// This extends the TTL of the item
	// The set will succeed even if the item has expired since we entered WATCH
	pipe := tx.TxPipeline()
	pipe.Set(fullKey, item.Value, item.Expiration)
	_, err = pipe.Exec()
	return err
}

// This (and DeleteMulti) doesn't return ErrCacheMiss if the key doesn't exist
// However, every caller of this never used that error for anything useful
func (ms Memorystore) Delete(key string) error {
	return ms.DeleteMulti([]string{key})
}

func (ms Memorystore) DeleteMulti(keys []string) error {
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = ms.buildNamespacedKey(key)
	}
	return ms.client.Del(fullKeys...)
}

func (ms Memorystore) Flush() error {
	return ms.client.FlushAll()
}

func (ms Memorystore) Get(key string) (*CacheItem, error) {
	fullKey := ms.buildNamespacedKey(key)
	if val, err := ms.client.Get(fullKey); err != nil {
		// redis.Nil (ErrCacheMiss) will be returned if they key doesn't exist
		return nil, err
	} else {
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		return &CacheItem{
			Key:            key,
			Value:          val,
			valueOnLastGet: valCopy,
		}, nil
	}
}

func (ms Memorystore) GetMulti(keys []string) (map[string]*CacheItem, error) {
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = ms.buildNamespacedKey(key)
	}
	vals, err := ms.client.MGet(fullKeys...)
	if err != nil {
		return nil, err
	}

	results := make(map[string]*CacheItem, len(keys))

	for i, val := range vals {
		if val == nil {
			// Not found
			continue
		}
		valBytes := val.([]byte)
		valCopy := make([]byte, len(valBytes))
		copy(valCopy, valBytes)
		results[keys[i]] = &CacheItem{
			Key:            keys[i],
			Value:          valBytes,
			valueOnLastGet: valCopy,
		}
	}

	return results, nil
}

func (ms Memorystore) Increment(key string, amount int64, initialValue uint64) (incr uint64, err error) {
	fullKey := ms.buildNamespacedKey(key)
	pipe := ms.client.TxPipeline()
	pipe.SetNX(fullKey, initialValue, time.Duration(0))
	pipe.IncrBy(fullKey, amount)

	var res []redis.Cmder
	if res, err = pipe.Exec(); err == nil {
		incr = uint64(res[1].(intCmdInterface).Val())
	}
	return incr, err
}

func (ms Memorystore) IncrementExisting(key string, amount int64) (uint64, error) {
	fullKey := ms.buildNamespacedKey(key)
	if res, err := ms.client.Exists(fullKey); err == nil && res == 1 {
		val, err := ms.client.IncrBy(fullKey, amount)
		return uint64(val), err
	} else if err != nil {
		return 0, err
	} else {
		return 0, ErrCacheMiss
	}
}

func (ms Memorystore) Set(item *CacheItem) error {
	fullKey := ms.buildNamespacedKey(item.Key)
	return ms.client.Set(fullKey, item.Value, item.Expiration)
}

func (ms Memorystore) SetMulti(items []*CacheItem) error {
	pipe := ms.client.TxPipeline()
	for _, item := range items {
		fullKey := ms.buildNamespacedKey(item.Key)
		pipe.Set(fullKey, item.Value, item.Expiration)
	}
	_, err := pipe.Exec()
	return err
}

func (ms Memorystore) Namespace(ns string) Memcache {
	return Memorystore{ms.c, ms.client, ns}
}
