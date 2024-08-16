package appwrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"io"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"

	cloudms "cloud.google.com/go/redis/apiv1"
	"github.com/cespare/xxhash/v2"
	"github.com/go-redis/redis/v8"
	"github.com/googleapis/gax-go/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	redispb "google.golang.org/genproto/googleapis/cloud/redis/v1"
)

type redisAPIConnectorFn func(ctx context.Context) (redisAPIService, error)

func NewRedisAPIService(ctx context.Context) (redisAPIService, error) {
	return cloudms.NewCloudRedisClient(ctx)
}

// redisAPIService captures the behavior of *redispb.CloudRedisClient, to make it mockable to testing.
type redisAPIService interface {
	io.Closer
	FailoverInstance(ctx context.Context, req *redispb.FailoverInstanceRequest, opts ...gax.CallOption) (*cloudms.FailoverInstanceOperation, error)
	GetInstance(context.Context, *redispb.GetInstanceRequest, ...gax.CallOption) (*redispb.Instance, error)
}

// Implements needed redis methods for mocking purposes.  See *redis.Client for a full list of available methods
// Implementations of these methods convert the returned redis Cmd objects into mockable data by calling
// Err(), Result(), etc.
type redisCommonInterface interface {
	Del(ctx context.Context, keys ...string) error
	Exists(ctx context.Context, keys ...string) (int64, error)
	FlushAll(ctx context.Context) error
	FlushAllAsync(ctx context.Context) error
	Get(ctx context.Context, key string) ([]byte, error)
	IncrBy(ctx context.Context, key string, value int64) (int64, error)
	MGet(ctx context.Context, keys ...string) ([]interface{}, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)
	TxPipeline() redisPipelineInterface
}

// Additionally implements Watch for transactions
type redisClientInterface interface {
	redisCommonInterface
	PoolStats() *redis.PoolStats
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
}

type redisClientImplementation struct {
	// common is used for all methods defined on redisCommonInterface
	common redis.Cmdable
	// client is used for the redisClientInterface-specific methods
	client *redis.Client
}

func (rci *redisClientImplementation) Del(ctx context.Context, keys ...string) error {
	return rci.common.Del(ctx, keys...).Err()
}

func (rci *redisClientImplementation) Exists(ctx context.Context, keys ...string) (int64, error) {
	return rci.common.Exists(ctx, keys...).Result()
}

func (rci *redisClientImplementation) FlushAll(ctx context.Context) error {
	return rci.common.FlushAll(ctx).Err()
}

func (rci *redisClientImplementation) FlushAllAsync(ctx context.Context) error {
	return rci.common.FlushAllAsync(ctx).Err()
}

func (rci *redisClientImplementation) Get(ctx context.Context, key string) ([]byte, error) {
	return rci.common.Get(ctx, key).Bytes()
}

func (rci *redisClientImplementation) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return rci.common.IncrBy(ctx, key, value).Result()
}

func (rci *redisClientImplementation) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	return rci.common.MGet(ctx, keys...).Result()
}

func (rci *redisClientImplementation) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return rci.common.Set(ctx, key, value, expiration).Err()
}

func (rci *redisClientImplementation) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	return rci.common.SetNX(ctx, key, value, expiration).Result()
}

func (rci *redisClientImplementation) TxPipeline() redisPipelineInterface {
	return &redisPipelineImplementation{rci.common.TxPipeline()}
}

func (rci *redisClientImplementation) PoolStats() *redis.PoolStats {
	return rci.client.PoolStats()
}

// Watch can only be called by the top-level redis Client.  In particular, this means that
// *redis.TX cannot call Watch again - it only implements redis.Cmdable.
func (rci *redisClientImplementation) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return rci.client.Watch(ctx, fn, keys...)
}

// Implements needed redis pipeline methods for mocking purposes.  See redis.Pipeliner for all available methods.
type redisPipelineInterface interface {
	Exec(ctx context.Context) ([]redis.Cmder, error)
	IncrBy(ctx context.Context, key string, value int64)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration)
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration)
}

type redisPipelineImplementation struct {
	pipeline redis.Pipeliner
}

func (rpi *redisPipelineImplementation) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return rpi.pipeline.Exec(ctx)
}

func (rpi *redisPipelineImplementation) IncrBy(ctx context.Context, key string, value int64) {
	rpi.pipeline.IncrBy(ctx, key, value)
}

func (rpi *redisPipelineImplementation) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) {
	rpi.pipeline.Set(ctx, key, value, expiration)
}

func (rpi *redisPipelineImplementation) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) {
	rpi.pipeline.SetNX(ctx, key, value, expiration)
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
	tracer    trace.Tracer
	namespace string
	keyHashFn func(key string, shardCount int) int
}

type memorystoreService struct {
	connectFn redisAPIConnectorFn // if nil, use "real" implementation NewRedisAPIService; non-nil used for testing

	mtx sync.Mutex

	clients            *[]redisClientInterface
	addrs              []string
	addrLastErr        error
	addrDontRetryUntil time.Time
}

var GlobalService memorystoreService

func InitializeRedisAddrs(addrs []string) {
	if len(addrs) == 0 || LocalDebug {
		return
	}
	GlobalService.mtx.Lock()
	defer GlobalService.mtx.Unlock()

	if !reflect.DeepEqual(GlobalService.addrs, addrs) {
		GlobalService.addrs = addrs
		GlobalService.clients = nil
	}
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

	projectId := appInfo.NativeProjectID()

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

func NewMemorystore(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards) (Memcache, error) {
	return GlobalService.NewMemorystore(c, appInfo, loc, name, shards)
}

func NewRateLimitedMemorystore(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards, log Logging, createLimiters func(shard int, log Logging) redis.Limiter) (Memcache, error) {
	return GlobalService.NewRateLimitedMemorystore(c, appInfo, loc, name, shards, log, createLimiters)
}

func (ms *memorystoreService) NewMemorystore(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards) (Memcache, error) {
	return ms.NewRateLimitedMemorystore(c, appInfo, loc, name, shards, nil, nil)
}

func (ms *memorystoreService) NewRateLimitedMemorystore(c context.Context, appInfo AppengineInfo, loc CacheLocation, name CacheName, shards CacheShards, log Logging, createLimiter func(shard int, log Logging) redis.Limiter) (Memcache, error) {
	// We don't use sync.Once here because we do actually want to execute the long path again in case of failures to initialize.
	ourClients := ms.clients

	if ourClients == nil {
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

			rateLimitersProvided := createLimiter != nil && log != nil

			for i := range addrs {
				ops := &redis.Options{
					Addr:     addrs[i],
					Password: "",
					DB:       0,

					// Do not ever use internal retries; let the user of this
					// library deal with retrying themselves if they see fit.
					MaxRetries: -1,

					// These are set by environment variable; see the init() function.
					IdleTimeout: memorystoreIdleTimeout,
					PoolSize:    memorystorePoolSize,
					PoolTimeout: memorystorePoolTimeout,
					ReadTimeout: memorystoreReadTimeout,
				}

				if ops.PoolSize == 0 {
					ops.PoolSize = 4 * runtime.GOMAXPROCS(0)
				}

				if rateLimitersProvided {
					ops.Limiter = createLimiter(i, log)
				}

				shard := i
				ipaddr := addrs[i]
				ops.OnConnect = func(ctx context.Context, cn *redis.Conn) error {
					log := NewStackdriverLogging(ctx)
					log.Debugf("memorystore: created new connection to shard %d (%s)", shard, ipaddr)
					return nil
				}

				client := redis.NewClient(ops)
				if err := addMetrics(client, shard); err != nil && log != nil {
					log.Warningf("failed to set up metrics for redis client %s, shard %d: %s", ipaddr, shard, err)
				}
				clients[i] = &redisClientImplementation{client, client}
			}

			ms.clients = &clients
		}

		ourClients = ms.clients
	}

	return Memorystore{c, *ourClients, otel.GetTracerProvider().Tracer(OtelScopeMemorystore), "", defaultKeyHashFn}, nil
}

const (
	memorystoreShardKey       = attribute.Key("io.pendo.db.client.connection.pool.shard")
	memorystorePoolResultsKey = attribute.Key("io.pendo.db.client.connection.pool.result")
)

func addMetrics(rdb *redis.Client, shard int) error {
	msMeter := otel.Meter(OtelScopeMemorystore)
	config := rdb.Options()

	baseAttrs := []attribute.KeyValue{
		semconv.DBSystemRedis,
		semconv.DBClientConnectionsPoolName(config.Addr),
		memorystoreShardKey.Int(shard),
	}

	idleMin, err := msMeter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionIdleMinName,
		metric.WithDescription(semconv.DBClientConnectionIdleMinDescription),
		metric.WithUnit(semconv.DBClientConnectionIdleMinUnit),
	)
	if err != nil {
		return err
	}

	connsMax, err := msMeter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionMaxName,
		metric.WithDescription(semconv.DBClientConnectionMaxDescription),
		metric.WithUnit(semconv.DBClientConnectionMaxUnit),
	)
	if err != nil {
		return err
	}

	idleAttrs := append(baseAttrs, semconv.DBClientConnectionsStateIdle)
	usedAttrs := append(baseAttrs, semconv.DBClientConnectionsStateUsed)
	usage, err := msMeter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionCountName,
		metric.WithDescription(semconv.DBClientConnectionCountDescription),
		metric.WithUnit(semconv.DBClientConnectionCountUnit),
	)
	if err != nil {
		return err
	}

	// io.pendo.db.client.connection.pool.timeouts is defined upstream as "db.client.connection.timeouts"
	// but according to the semantic conventions, it should use an up/down counter. This doesn't make sense as
	// the number of timeouts are monotonically increasing here, so we'll use our own name.
	// If we upgrade to Redis v9 and the default metrics are sufficient, then we can transition to the standard
	// metric and stop tracking this one.
	timeouts, err := msMeter.Int64ObservableCounter(
		"io.pendo.db.client.connection.pool.timeouts",
		metric.WithDescription(semconv.DBClientConnectionTimeoutsDescription),
		metric.WithUnit(semconv.DBClientConnectionTimeoutsUnit),
	)
	if err != nil {
		return err
	}

	// io.pendo.db.client.connection.pool.reuse is non-standard, and obviated by the standard metric
	// db.client.connection.create_time, which is supported in go-redis v9.
	hitAttrs := append(baseAttrs, memorystorePoolResultsKey.String("reused"))
	missAttrs := append(baseAttrs, memorystorePoolResultsKey.String("created"))
	poolReuse, err := msMeter.Int64ObservableCounter("io.pendo.db.client.connection.pool.reuse",
		metric.WithDescription("The number of hits and misses when attempting to obtain a connection from the pool."),
	)
	if err != nil {
		return err
	}

	_, err = msMeter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			stats := rdb.PoolStats()

			o.ObserveInt64(idleMin, int64(config.MinIdleConns), metric.WithAttributes(baseAttrs...))
			o.ObserveInt64(connsMax, int64(config.PoolSize), metric.WithAttributes(baseAttrs...))

			o.ObserveInt64(usage, int64(stats.TotalConns-stats.IdleConns), metric.WithAttributes(usedAttrs...))
			o.ObserveInt64(usage, int64(stats.IdleConns), metric.WithAttributes(idleAttrs...))

			o.ObserveInt64(timeouts, int64(stats.Timeouts), metric.WithAttributes(baseAttrs...))

			o.ObserveInt64(poolReuse, int64(stats.Hits), metric.WithAttributes(hitAttrs...))
			o.ObserveInt64(poolReuse, int64(stats.Misses), metric.WithAttributes(missAttrs...))

			return nil
		},
		idleMin,
		connsMax,
		usage,
		timeouts,
		poolReuse,
	)

	return err
}

func (ms Memorystore) shardedNamespacedKeysForItems(items []*CacheItem) (namespacedKeys [][]string, originalPositions map[string]int, singleShard int) {
	keys := make([]string, len(items))

	for i, item := range items {
		keys[i] = item.Key
	}

	return ms.shardedNamespacedKeys(keys)
}

func (ms Memorystore) shardedNamespacedKeys(keys []string) (namespacedKeys [][]string, originalPositions map[string]int, singleShard int) {
	namespacedKeys = make([][]string, len(ms.clients))
	originalPositions = make(map[string]int, len(keys))

	singleShard = -1
	for i, key := range keys {
		namespacedKey, shard := ms.namespacedKeyAndShard(key)
		if i > 0 && singleShard != shard {
			singleShard = -1
		} else {
			singleShard = shard
		}

		namespacedKeys[shard] = append(namespacedKeys[shard], namespacedKey)
		originalPositions[namespacedKey] = i
	}
	return namespacedKeys, originalPositions, singleShard
}

func (ms Memorystore) namespacedKeyAndShard(key string) (string, int) {
	if key == "" {
		panic("redis: blank key")
	}
	namespacedKey := ms.namespace + ":" + key
	shard := ms.keyHashFn(namespacedKey, len(ms.clients))
	return namespacedKey, shard
}

func (ms Memorystore) Add(item *CacheItem) error {
	fullKey, shard := ms.namespacedKeyAndShard(item.Key)

	c, span := ms.tracer.Start(ms.c, traceMemorystoreAdd)
	defer span.End()

	span.SetAttributes(labelKey(fullKey))
	span.SetAttributes(labelShard(int64(shard)))

	if added, err := ms.clients[shard].SetNX(c, fullKey, item.Value, item.Expiration); err != nil {
		return err
	} else if !added {
		return CacheErrNotStored
	}
	return nil
}

func (ms Memorystore) AddMulti(items []*CacheItem) error {
	c, span := ms.tracer.Start(ms.c, traceMemorystoreAddMulti)
	defer span.End()

	span.SetAttributes(labelNumKeys(int64(len(items))))

	addMultiForShard := func(shard int, itemIndices map[string]int, shardKeys []string) ([]redis.Cmder, error) {
		if len(shardKeys) == 0 {
			return nil, nil
		}

		c, span := ms.tracer.Start(c, traceMemorystoreAddMultiShard)
		defer span.End()

		span.SetAttributes(labelFirstKey(shardKeys[0]))
		span.SetAttributes(labelNumKeys(int64(len(shardKeys))))
		span.SetAttributes(labelShard(int64(shard)))

		pipe := ms.clients[shard].TxPipeline()
		for _, key := range shardKeys {
			item := items[itemIndices[key]]
			pipe.SetNX(c, key, item.Value, item.Expiration)
		}
		return pipe.Exec(c)
	}

	handleReturn := func(shard int, itemIndices map[string]int, shardKeys []string, shardResults []redis.Cmder, errList []error) bool {
		haveErrors := false
		for i, result := range shardResults {
			if err := result.Err(); err != nil {
				errList[itemIndices[shardKeys[i]]] = err
				haveErrors = true
			} else if added, _ := result.(boolCmdInterface).Result(); !added {
				errList[itemIndices[shardKeys[i]]] = CacheErrNotStored
				haveErrors = true
			}
		}
		return haveErrors
	}

	namespacedKeys, itemIndices, singleShard := ms.shardedNamespacedKeysForItems(items)
	errList := make(MultiError, len(items))

	if singleShard >= 0 {
		results, err := addMultiForShard(singleShard, itemIndices, namespacedKeys[singleShard])
		if err != nil {
			return err
		}
		if handleReturn(singleShard, itemIndices, namespacedKeys[singleShard], results, errList) {
			return errList
		}
		return nil
	}

	results := make([][]redis.Cmder, len(ms.clients))
	wg := sync.WaitGroup{}
	errs := make(chan error, len(ms.clients))
	for shard := 0; shard < len(ms.clients); shard++ {
		if len(namespacedKeys[shard]) == 0 {
			continue
		}
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			shardKeys := namespacedKeys[shard]
			res, err := addMultiForShard(shard, itemIndices, shardKeys)
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
	for shard, shardResults := range results {
		newErrors := handleReturn(shard, itemIndices, namespacedKeys[shard], shardResults, errList)
		haveErrors = haveErrors || newErrors
	}

	if haveErrors {
		return errList
	}

	return nil
}

func (ms Memorystore) CompareAndSwap(item *CacheItem) error {
	fullKey, shard := ms.namespacedKeyAndShard(item.Key)

	c, span := ms.tracer.Start(ms.c, traceMemorystoreCAS)
	defer span.End()

	span.SetAttributes(labelFirstKey(fullKey))
	span.SetAttributes(labelShard(int64(shard)))

	if err := ms.clients[shard].Watch(c, func(tx *redis.Tx) error {
		// Watch is an optimistic lock
		txClient := &redisClientImplementation{tx, nil}
		return ms.doCompareAndSwap(c, item, txClient, fullKey)
	}, fullKey); err == redis.TxFailedErr {
		return CacheErrCASConflict
	} else {
		return err
	}
}

func (ms Memorystore) doCompareAndSwap(c context.Context, item *CacheItem, tx redisCommonInterface, fullKey string) error {
	val, err := tx.Get(c, fullKey)
	if err == redis.Nil {
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
	pipe.Set(c, fullKey, item.Value, item.Expiration)
	_, err = pipe.Exec(c)
	return err
}

// This (and DeleteMulti) doesn't return ErrCacheMiss if the key doesn't exist
// However, every caller of this never used that error for anything useful
func (ms Memorystore) Delete(key string) error {
	return ms.DeleteMulti([]string{key})
}

func (ms Memorystore) DeleteMulti(keys []string) error {
	c, span := ms.tracer.Start(ms.c, traceMemorystoreDeleteMulti)
	defer span.End()

	span.SetAttributes(labelNumKeys(int64(len(keys))))

	namespacedKeys, _, _ := ms.shardedNamespacedKeys(keys)
	errList := make(MultiError, 0, len(ms.clients))

	haveErrors := false
	for i, client := range ms.clients {
		shardKeys := namespacedKeys[i]
		if len(shardKeys) == 0 {
			continue
		}

		func() {
			c, span := ms.tracer.Start(c, traceMemorystoreDeleteMultiShard)
			defer span.End()

			span.SetAttributes(labelFirstKey(shardKeys[0]))
			span.SetAttributes(labelNumKeys(int64(len(shardKeys))))
			span.SetAttributes(labelShard(int64(i)))
			if err := client.Del(c, shardKeys...); err != nil {
				errList = append(errList, err)
				haveErrors = true
			}
		}()

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
	return ms.clients[shard].FlushAllAsync(ms.c)
}

func (ms Memorystore) Get(key string) (*CacheItem, error) {
	c, span := ms.tracer.Start(ms.c, traceMemorystoreGet)
	defer span.End()

	fullKey, shard := ms.namespacedKeyAndShard(key)

	span.SetAttributes(labelKey(fullKey))
	span.SetAttributes(labelShard(int64(shard)))

	if val, err := ms.clients[shard].Get(c, fullKey); err == redis.Nil {
		return nil, ErrCacheMiss
	} else if err != nil {
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
	c, span := ms.tracer.Start(ms.c, traceMemorystoreGetMulti)
	defer span.End()

	span.SetAttributes(labelNumKeys(int64(len(keys))))

	getMultiForShard := func(shard int, itemIndices map[string]int, shardKeys []string) ([]interface{}, error) {
		if len(shardKeys) == 0 {
			return nil, nil
		}
		c, span := ms.tracer.Start(c, traceMemorystoreGetMultiShard)
		defer span.End()

		span.SetAttributes(labelFirstKey(shardKeys[0]))
		span.SetAttributes(labelNumKeys(int64(len(shardKeys))))
		span.SetAttributes(labelShard(int64(shard)))
		return ms.clients[shard].MGet(c, shardKeys...)
	}

	handleReturn := func(shard int, itemIndices map[string]int, shardKeys []string, shardVals []interface{}, results map[string]*CacheItem) {
		for i, val := range shardVals {
			if val == nil {
				// Not found
				continue
			}
			valBytes := ms.convertToByteSlice(val)
			valCopy := make([]byte, len(valBytes))
			copy(valCopy, valBytes)
			key := keys[itemIndices[shardKeys[i]]]
			results[key] = &CacheItem{
				Key:            key,
				Value:          valBytes,
				valueOnLastGet: valCopy,
			}
		}
	}

	namespacedKeys, keyIndices, singleShard := ms.shardedNamespacedKeys(keys)

	// Fast path (no goroutine) if only one shard is involved
	if singleShard >= 0 {

		results := make(map[string]*CacheItem, len(keys))

		vals, err := getMultiForShard(singleShard, keyIndices, namespacedKeys[singleShard])
		if err != nil {
			return nil, err
		}

		handleReturn(singleShard, keyIndices, namespacedKeys[singleShard], vals, results)
		return results, nil
	}

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
			vals, err := getMultiForShard(shard, keyIndices, shardKeys)
			returnVals[shard] = vals
			if err != nil {
				finalErr[shard] = err
				haveErrors = true
			}
		}()
	}

	wg.Wait()

	results := make(map[string]*CacheItem, len(keys))
	for shard, shardVals := range returnVals {
		handleReturn(shard, keyIndices, namespacedKeys[shard], shardVals, results)
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

func (ms Memorystore) Increment(key string, amount int64, initialValue uint64, initialExpires time.Duration) (incr uint64, err error) {
	fullKey, shard := ms.namespacedKeyAndShard(key)
	c, span := ms.tracer.Start(ms.c, traceMemorystoreIncr)
	defer span.End()

	span.SetAttributes(labelKey(fullKey))
	span.SetAttributes(labelShard(int64(shard)))

	pipe := ms.clients[shard].TxPipeline()
	pipe.SetNX(c, fullKey, initialValue, initialExpires)
	pipe.IncrBy(c, fullKey, amount)

	var res []redis.Cmder
	if res, err = pipe.Exec(c); err == nil {
		incr = uint64(res[1].(intCmdInterface).Val())
	}
	return incr, err
}

func (ms Memorystore) IncrementExisting(key string, amount int64) (uint64, error) {
	fullKey, shard := ms.namespacedKeyAndShard(key)
	c, span := ms.tracer.Start(ms.c, traceMemorystoreIncrExisting)
	defer span.End()

	span.SetAttributes(labelKey(fullKey))
	span.SetAttributes(labelShard(int64(shard)))

	if res, err := ms.clients[shard].Exists(c, fullKey); err == nil && res == 1 {
		val, err := ms.clients[shard].IncrBy(c, fullKey, amount)
		return uint64(val), err
	} else if err != nil {
		return 0, err
	} else {
		return 0, ErrCacheMiss
	}
}

func (ms Memorystore) Set(item *CacheItem) error {
	fullKey, shard := ms.namespacedKeyAndShard(item.Key)
	c, span := ms.tracer.Start(ms.c, traceMemorystoreSet)
	defer span.End()

	span.SetAttributes(labelKey(fullKey))
	span.SetAttributes(labelShard(int64(shard)))

	return ms.clients[shard].Set(c, fullKey, item.Value, item.Expiration)
}

func (ms Memorystore) SetMulti(items []*CacheItem) error {
	c, span := ms.tracer.Start(ms.c, traceMemorystoreSetMulti)
	defer span.End()

	span.SetAttributes(labelNumKeys(int64(len(items))))

	setMultiForShard := func(shard int, itemIndices map[string]int, shardKeys []string) error {
		if len(shardKeys) == 0 {
			return nil
		}

		c, span := ms.tracer.Start(c, traceMemorystoreSetMultiShard)
		defer span.End()

		span.SetAttributes(labelFirstKey(shardKeys[0]))
		span.SetAttributes(labelNumKeys(int64(len(shardKeys))))
		span.SetAttributes(labelShard(int64(shard)))

		pipe := ms.clients[shard].TxPipeline()
		for i, key := range shardKeys {
			item := items[itemIndices[shardKeys[i]]]
			pipe.Set(c, key, item.Value, item.Expiration)
		}
		_, err := pipe.Exec(c)
		if err != nil {
			return err
		}
		return nil
	}

	namespacedKeys, itemIndices, singleShard := ms.shardedNamespacedKeysForItems(items)

	if singleShard >= 0 {
		if err := setMultiForShard(singleShard, itemIndices, namespacedKeys[singleShard]); err != nil {
			return err
		}
		return nil
	}

	errs := make(chan error, len(ms.clients))
	wg := sync.WaitGroup{}

	for shard := 0; shard < len(ms.clients); shard++ {
		if len(namespacedKeys[shard]) == 0 {
			continue
		}
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			shardKeys := namespacedKeys[shard]
			if err := setMultiForShard(shard, itemIndices, shardKeys); err != nil {
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
	return Memorystore{ms.c, ms.clients, ms.tracer, ns, ms.keyHashFn}
}

func defaultKeyHashFn(key string, shardCount int) int {
	return int(xxhash.Sum64String(key) % uint64(shardCount))
}

const (
	envMemorystoreIdleTimeoutMs = "memorystore_idle_timeout_ms"
	envMemorystorePoolSize      = "memorystore_pool_size"
	envMemorystorePoolTimeoutMs = "memorystore_pool_timeout_ms"
	envMemorystoreReadTimeoutMs = "memorystore_read_timeout_ms"
)

var (
	memorystoreIdleTimeout time.Duration
	memorystorePoolSize    int
	memorystorePoolTimeout time.Duration
	memorystoreReadTimeout time.Duration
)

func init() {
	readTimeoutMsStr := os.Getenv(envMemorystoreReadTimeoutMs)
	if readTimeoutMsStr != "" {
		timeoutMs, err := strconv.ParseInt(readTimeoutMsStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				envMemorystoreReadTimeoutMs, readTimeoutMsStr, err)
		} else if timeoutMs < 1 {
			fmt.Fprintf(os.Stderr, "'%s' must be a non-zero non-negative integer\n",
				envMemorystorePoolSize)
		} else {
			memorystoreReadTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	memorystorePoolSizeStr := os.Getenv(envMemorystorePoolSize)
	if memorystorePoolSizeStr != "" {
		poolSize, err := strconv.ParseInt(memorystorePoolSizeStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				envMemorystorePoolSize, memorystorePoolSizeStr, err)
		} else if poolSize < 1 {
			fmt.Fprintf(os.Stderr, "'%s' must be a non-zero non-negative integer\n",
				envMemorystorePoolSize)
		} else {
			memorystorePoolSize = int(poolSize)
		}
	}

	poolTimeoutStr := os.Getenv(envMemorystorePoolTimeoutMs)
	if poolTimeoutStr != "" {
		timeoutMs, err := strconv.ParseInt(poolTimeoutStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				envMemorystorePoolTimeoutMs, poolTimeoutStr, err)
		} else if timeoutMs < 1 {
			fmt.Fprintf(os.Stderr, "'%s' must be a non-zero non-negative integer\n",
				envMemorystorePoolTimeoutMs)
		} else {
			memorystorePoolTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	// From https://cloud.google.com/memorystore/docs/redis/redis-configs:
	// The default idle timeout on the managed Redis servers used by Memorystore
	// is 0, which is to say the connections are _never_ disconnected by the server.
	// The go-redis documentation says that any client-specified value should always
	// be less than the Redis server's value, or disabled.
	//
	// To disable idle connection reaping, specify -1.
	idleTimeoutStr := os.Getenv(envMemorystoreIdleTimeoutMs)
	if idleTimeoutStr != "" {
		timeoutMs, err := strconv.ParseInt(idleTimeoutStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				envMemorystoreIdleTimeoutMs, idleTimeoutStr, err)
		} else if timeoutMs == -1 {
			memorystoreIdleTimeout = -1
		} else if timeoutMs < 1 {
			fmt.Fprintf(os.Stderr, "'%s' must be either a non-zero non-negative integer, or -1 to disable idle timeout\n",
				envMemorystoreIdleTimeoutMs)
		} else {
			memorystoreIdleTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	if LocalDebug {
		log.Println("Connecting redis to localhost")

		clients := make([]redisClientInterface, 1)

		ops := &redis.Options{
			Addr:     "127.0.0.1:6379",
			Password: "",
			DB:       0,

			// Do not ever use internal retries; let the user of this
			// library deal with retrying themselves if they see fit.
			MaxRetries: -1,

			IdleTimeout: memorystoreIdleTimeout,
			PoolSize:    memorystorePoolSize,
			PoolTimeout: memorystorePoolTimeout,
			ReadTimeout: memorystoreReadTimeout,
		}

		if ops.PoolSize == 0 {
			ops.PoolSize = 4 * runtime.GOMAXPROCS(0)
		}

		ops.OnConnect = func(ctx context.Context, cn *redis.Conn) error {
			log := NewStackdriverLogging(ctx)
			log.Debugf("memorystore: created new connection to shard %d (%s)", 0, "127.0.0.1:6379")
			return nil
		}

		client := redis.NewClient(ops)
		clients[0] = &redisClientImplementation{client, client}

		GlobalService.clients = &clients
	}
}
