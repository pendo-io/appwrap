package appwrap

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"time"

	cloudms "cloud.google.com/go/redis/apiv1"
	xxhash "github.com/cespare/xxhash/v2"
	"github.com/go-redis/redis"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/pendo-io/appwrap/internal/metrics"
	"go.opencensus.io/tag"
	"golang.org/x/net/context"
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

type redisAPIConnectorFn func(ctx context.Context) (redisAPIService, error)

func NewRedisAPIService(ctx context.Context) (redisAPIService, error) {
	return cloudms.NewCloudRedisClient(ctx)
}

// redisAPIService captures the behavior of *redispb.CloudRedisClient, to make it mockable to testing.
type redisAPIService interface {
	io.Closer
	GetInstance(context.Context, *redispb.GetInstanceRequest, ...gax.CallOption) (*redispb.Instance, error)
}

// Implements needed redis methods for mocking purposes.  See *redis.Client for a full list of available methods
// Implementations of these methods convert the returned redis Cmd objects into mockable data by calling
// Err(), Result(), etc.
type redisCommonInterface interface {
	Del(keys ...string) error
	Exists(keys ...string) (int64, error)
	FlushAll() error
	FlushAllAsync() error
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
	PoolStats() *redis.PoolStats
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

func (rci *redisClientImplementation) FlushAllAsync() error {
	return rci.common.FlushAllAsync().Err()
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

func (rci *redisClientImplementation) PoolStats() *redis.PoolStats {
	return rci.client.PoolStats()
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
	clients   []redisClientInterface
	namespace string
}

type memorystoreService struct {
	connectFn redisAPIConnectorFn // if nil, use "real" implementation NewRedisAPIService; non-nil used for testing

	mtx sync.Mutex

	clients            *[]redisClientInterface
	addrs              []string
	addrLastErr        error
	addrDontRetryUntil time.Time

	statReporterOnce sync.Once
}

var GlobalService memorystoreService

func InitializeRedisAddrs(addrs []string) {
	if len(addrs) == 0 {
		return
	}
	GlobalService.mtx.Lock()
	defer GlobalService.mtx.Unlock()
	GlobalService.addrs = addrs
}

const redisErrorDontRetryInterval = 5 * time.Second

func (ms *memorystoreService) getRedisAddr(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards) (_ []string, finalErr error) {
	if ms.addrs != nil && ms.addrLastErr == nil {
		return ms.addrs, nil
	}

	// Handle don't-retry interval: repeat prior error if too soon after failure
	now := time.Now()
	if ms.addrLastErr != nil && now.Before(ms.addrDontRetryUntil) {
		return nil, fmt.Errorf("cached error (no retry for %s): %s", ms.addrDontRetryUntil.Sub(now), ms.addrLastErr)
	}
	defer func() {
		if finalErr != nil {
			ms.addrLastErr, ms.addrDontRetryUntil = finalErr, now.Add(redisErrorDontRetryInterval)
		}
	}()

	connectFn := ms.connectFn
	if connectFn == nil {
		connectFn = NewRedisAPIService
	}
	client, err := connectFn(context.Background())
	if err != nil {
		return nil, err
	}
	defer client.Close()

	projectId := appInfo.AppID()

	if ms.addrs == nil {
		ms.addrs = make([]string, shards)
	}

	for shard, existingAddr := range ms.addrs {
		if existingAddr != "" {
			continue // skip already-successful addresses
		}
		instance, err := client.GetInstance(c, &redispb.GetInstanceRequest{
			Name: fmt.Sprintf("projects/%s/locations/%s/instances/%s-%d", projectId, loc, name, shard),
		})
		if err != nil {
			finalErr = err
			continue // skip failed address gets and keep trying to cache others (but consider the overall lookup failed)
		}

		ms.addrs[shard] = fmt.Sprintf("%s:%d", instance.Host, instance.Port)
	}

	if finalErr != nil {
		return nil, finalErr
	}

	return ms.addrs, nil
}

func NewAppengineMemcache(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards) (Memcache, error) {
	return GlobalService.NewMemcache(c, appInfo, loc, name, shards)
}

func (ms *memorystoreService) NewMemcache(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards) (Memcache, error) {
	// We don't use sync.Once here because we do actually want to execute the long path again in case of failures to initialize.
	if ms.clients == nil {
		ms.mtx.Lock()
		defer ms.mtx.Unlock()

		// Check again, because another goroutine could have beaten us here while we were checking the first time
		if ms.clients == nil {
			if shards == 0 {
				panic("cannot use Memorystore with zero shards")
			}

			clients := make([]redisClientInterface, shards)
			addrs, err := ms.getRedisAddr(c, appInfo, loc, name, shards)
			if err != nil {
				return nil, err
			}
			for i := range addrs {
				client := redis.NewClient(&redis.Options{
					Addr:     addrs[i],
					Password: "",
					DB:       0,
					PoolSize: 4 * runtime.GOMAXPROCS(0),
				}).WithContext(c)
				clients[i] = &redisClientImplementation{client, client}
			}

			ms.clients = &clients
		}
	}

	statInterval := metrics.GetMetricsRecordingInterval()
	if statInterval > 0 {
		ms.statReporterOnce.Do(func() {
			fmt.Fprintf(os.Stderr, "[memorystoreService] stat reporter starting (reporting every %s)\n", statInterval)
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt)
			go func() {
				ticker := time.NewTicker(statInterval)
				defer func() {
					ticker.Stop()
				}()
				for {
					select {
					case <-ticker.C:
						ms.logPoolStats()
					case <-sigCh:
						fmt.Fprintln(os.Stderr, "[memorystoreService] interrupt received, stopping stat reporter")
						return
					}
				}
			}()
		})
	}
	return Memorystore{c, *ms.clients, ""}, nil
}

func (ms *memorystoreService) logPoolStats() {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()
	if ms.clients == nil {
		return
	}

	for i, client := range *ms.clients {
		pstats := client.PoolStats()

		// These metrics are all for the same connection shard
		mctx, err := tag.New(context.Background(), tag.Insert(metrics.KeyConnectionShard, strconv.Itoa(i)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create context with tag: "+err.Error())
			continue
		}

		// Pool usage stats
		metrics.RecordWithTagName(mctx, metrics.MMemoryStoreConnectionPoolUsage.M(int64(pstats.Hits)),
			metrics.KeyPoolUsageResult, metrics.ConnectionPoolUsageResultHit)
		metrics.RecordWithTagName(mctx, metrics.MMemoryStoreConnectionPoolUsage.M(int64(pstats.Misses)),
			metrics.KeyPoolUsageResult, metrics.ConnectionPoolUsageResultMiss)
		metrics.RecordWithTagName(mctx, metrics.MMemoryStoreConnectionPoolUsage.M(int64(pstats.Timeouts)),
			metrics.KeyPoolUsageResult, metrics.ConnectionPoolUsageResultTimeout)

		// Connection state stats
		metrics.RecordWithTagName(mctx, metrics.MMemoryStoreConnectionPoolConnections.M(int64(pstats.TotalConns-pstats.IdleConns)),
			metrics.KeyPoolConnState, metrics.ConnectionPoolConnectionStateActive)
		metrics.RecordWithTagName(mctx, metrics.MMemoryStoreConnectionPoolConnections.M(int64(pstats.IdleConns)),
			metrics.KeyPoolConnState, metrics.ConnectionPoolConnectionStateIdle)
	}
}

func (ms Memorystore) shardedNamespacedKeysForItems(items []*CacheItem) (namespacedKeys [][]string, originalPositions map[string]int) {
	keys := make([]string, len(items))

	for i, item := range items {
		keys[i] = item.Key
	}

	return ms.shardedNamespacedKeys(keys)
}

func (ms Memorystore) shardedNamespacedKeys(keys []string) (namespacedKeys [][]string, originalPositions map[string]int) {
	namespacedKeys = make([][]string, len(ms.clients))
	originalPositions = make(map[string]int, len(keys))
	for i, key := range keys {
		namespacedKey, shard := ms.namespacedKeyAndShard(key)
		namespacedKeys[shard] = append(namespacedKeys[shard], namespacedKey)
		originalPositions[namespacedKey] = i
	}
	return namespacedKeys, originalPositions
}

func (ms Memorystore) namespacedKeyAndShard(key string) (string, int) {
	if key == "" {
		panic("redis: blank key")
	}
	namespacedKey := ms.namespace + ":" + key
	shard := int(xxhash.Sum64String(namespacedKey) % uint64(len(ms.clients)))
	return namespacedKey, shard
}

func (ms Memorystore) Add(item *CacheItem) error {
	fullKey, shard := ms.namespacedKeyAndShard(item.Key)
	if added, err := ms.clients[shard].SetNX(fullKey, item.Value, item.Expiration); err != nil {
		return err
	} else if !added {
		return CacheErrNotStored
	}
	return nil

}

func (ms Memorystore) AddMulti(items []*CacheItem) error {
	namespacedKeys, itemIndices := ms.shardedNamespacedKeysForItems(items)

	results := make([][]redis.Cmder, len(ms.clients))
	wg := sync.WaitGroup{}
	errs := make(chan error, len(ms.clients))
	for shard := 0; shard < len(ms.clients); shard++ {
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			shardKeys := namespacedKeys[shard]
			if len(shardKeys) == 0 {
				return
			}
			pipe := ms.clients[shard].TxPipeline()
			for _, key := range shardKeys {
				item := items[itemIndices[key]]
				pipe.SetNX(key, item.Value, item.Expiration)
			}
			res, err := pipe.Exec()
			if err != nil {
				errs <- err
			}
			results[shard] = res
		}()
	}

	wg.Wait()

	select {
	case err := <-errs:
		return err
	default:
	}

	haveErrors := false
	errList := make(MultiError, len(items))

	for shard, shardResults := range results {
		for i, result := range shardResults {
			if added, err := result.(boolCmdInterface).Result(); err != nil {
				errList[itemIndices[namespacedKeys[shard][i]]] = err
				haveErrors = true
			} else if !added {
				errList[itemIndices[namespacedKeys[shard][i]]] = CacheErrNotStored
				haveErrors = true
			}
		}
	}

	if haveErrors {
		return errList
	}

	return nil
}

func (ms Memorystore) CompareAndSwap(item *CacheItem) error {
	fullKey, shard := ms.namespacedKeyAndShard(item.Key)
	if err := ms.clients[shard].Watch(func(tx *redis.Tx) error {
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
	namespacedKeys, _ := ms.shardedNamespacedKeys(keys)
	errList := make(MultiError, 0, len(ms.clients))

	haveErrors := false
	for i, client := range ms.clients {
		shardKeys := namespacedKeys[i]
		if len(shardKeys) == 0 {
			continue
		}
		if err := client.Del(shardKeys...); err != nil {
			errList = append(errList, err)
			haveErrors = true
		}

	}

	if haveErrors {
		return errList
	}

	return nil
}

func (ms Memorystore) Flush() error {
	return errors.New("please don't call this on memorystore")
	/*
		Leaving this here to show how you implement flush. It is currently disabled because flush brings down memorystore for the duration of this operation.

			errs := make([]error, 0, len(ms.clients))
			for _, client := range ms.clients {
				if err := client.FlushAll(); err != nil {
					errs = append(errs, err)
				}
			}
			if len(errs) == 0 {
				return nil
			} else {
				return MultiError(errs)
			}
	*/
}

func (ms Memorystore) FlushShard(shard int) error {
	if shard < 0 || shard >= len(ms.clients) {
		return fmt.Errorf("shard must be in range [0, %d), got %d", len(ms.clients), shard)
	}
	return ms.clients[shard].FlushAllAsync()
}

func (ms Memorystore) Get(key string) (*CacheItem, error) {
	fullKey, shard := ms.namespacedKeyAndShard(key)
	if val, err := ms.clients[shard].Get(fullKey); err != nil {
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
	results := make(map[string]*CacheItem, len(keys))

	namespacedKeys, keyIndices := ms.shardedNamespacedKeys(keys)
	returnVals := make([][]interface{}, len(ms.clients))
	wg := sync.WaitGroup{}
	haveErrors := false
	finalErr := make(MultiError, len(ms.clients))
	for shard := 0; shard < len(ms.clients); shard++ {
		shardKeys := namespacedKeys[shard]
		if len(shardKeys) == 0 {
			continue
		}
		wg.Add(1)
		shard := shard
		go func() {
			defer wg.Done()
			vals, err := ms.clients[shard].MGet(shardKeys...)
			returnVals[shard] = vals
			if err != nil {
				finalErr[shard] = err
				haveErrors = true
			}
		}()
	}

	wg.Wait()

	for shard, shardVals := range returnVals {
		for i, val := range shardVals {
			if val == nil {
				// Not found
				continue
			}
			valBytes := ms.convertToByteSlice(val)
			valCopy := make([]byte, len(valBytes))
			copy(valCopy, valBytes)
			key := keys[keyIndices[namespacedKeys[shard][i]]]
			results[key] = &CacheItem{
				Key:            key,
				Value:          valBytes,
				valueOnLastGet: valCopy,
			}
		}
	}

	if haveErrors {
		return results, finalErr
	} else {
		return results, nil
	}
}

func (ms Memorystore) convertToByteSlice(v interface{}) []byte {
	switch v.(type) {
	case string:
		return []byte(v.(string))
	case []byte:
		return v.([]byte)
	default:
		panic(fmt.Sprintf("unsupported type for convert: %T, %+v", v, v))
	}
}

func (ms Memorystore) Increment(key string, amount int64, initialValue uint64) (incr uint64, err error) {
	fullKey, shard := ms.namespacedKeyAndShard(key)
	pipe := ms.clients[shard].TxPipeline()
	pipe.SetNX(fullKey, initialValue, time.Duration(0))
	pipe.IncrBy(fullKey, amount)

	var res []redis.Cmder
	if res, err = pipe.Exec(); err == nil {
		incr = uint64(res[1].(intCmdInterface).Val())
	}
	return incr, err
}

func (ms Memorystore) IncrementExisting(key string, amount int64) (uint64, error) {
	fullKey, shard := ms.namespacedKeyAndShard(key)
	if res, err := ms.clients[shard].Exists(fullKey); err == nil && res == 1 {
		val, err := ms.clients[shard].IncrBy(fullKey, amount)
		return uint64(val), err
	} else if err != nil {
		return 0, err
	} else {
		return 0, ErrCacheMiss
	}
}

func (ms Memorystore) Set(item *CacheItem) error {
	fullKey, shard := ms.namespacedKeyAndShard(item.Key)
	return ms.clients[shard].Set(fullKey, item.Value, item.Expiration)
}

func (ms Memorystore) SetMulti(items []*CacheItem) error {
	namespacedKeys, itemIndices := ms.shardedNamespacedKeysForItems(items)
	errs := make(chan error, len(ms.clients))
	wg := sync.WaitGroup{}
	for shard := 0; shard < len(ms.clients); shard++ {
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			shardKeys := namespacedKeys[shard]
			if len(shardKeys) == 0 {
				return
			}
			pipe := ms.clients[shard].TxPipeline()
			for i, key := range shardKeys {
				item := items[itemIndices[shardKeys[i]]]
				pipe.Set(key, item.Value, item.Expiration)
			}
			_, err := pipe.Exec()
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func (ms Memorystore) Namespace(ns string) Memcache {
	return Memorystore{ms.c, ms.clients, ns}
}
