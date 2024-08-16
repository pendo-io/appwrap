package appwrap

import "go.opentelemetry.io/otel/attribute"

const (
	otelScopeBase = "github.com/pendo-io/appwrap"

	OtelScopeDatastore   = otelScopeBase + "/datastore"
	OtelScopeMemcache    = otelScopeBase + "/memcache"
	OtelScopeMemorystore = otelScopeBase + "/memorystore"

	traceDatastoreAllocateIds      = "pendo.io/datastore/allocate-id-set"
	traceDatastoreDeleteMulti      = "pendo.io/datastore/delete-multi"
	traceDatastoreGet              = "pendo.io/datastore/get"
	traceDatastoreGetMulti         = "pendo.io/datastore/get-multi"
	traceDatastoreKinds            = "pendo.io/datastore/kinds"
	traceDatastoreNewQuery         = "pendo.io/datastore/new-query"
	traceDatastorePut              = "pendo.io/datastore/put"
	traceDatastorePutMulti         = "pendo.io/datastore/put-multi"
	traceDatastoreRunInTransaction = "pendo.io/datastore/run-in-transaction"

	traceDatastoreQueryGetAll = "pendo.io/datastore/query/get-all"

	traceDatastoreTransactionDeleteMulti = "pendo.io/datastore/transaction/delete-multi"
	traceDatastoreTransactionGet         = "pendo.io/datastore/transaction/get"
	traceDatastoreTransactionGetMulti    = "pendo.io/datastore/transaction/get-multi"
	traceDatastoreTransactionNewQuery    = "pendo.io/datastore/transaction/new-query"
	traceDatastoreTransactionPut         = "pendo.io/datastore/transaction/put"
	traceDatastoreTransactionPutMulti    = "pendo.io/datastore/transaction/put-multi"

	traceMemorystoreAdd              = "pendo.io/memorystore/add"
	traceMemorystoreAddMulti         = "pendo.io/memorystore/add-multi"
	traceMemorystoreAddMultiShard    = "pendo.io/memorystore/add-multi-shard"
	traceMemorystoreCAS              = "pendo.io/memorystore/compare-and-swap"
	traceMemorystoreDeleteMulti      = "pendo.io/memorystore/delete-multi"
	traceMemorystoreDeleteMultiShard = "pendo.io/memorystore/delete-multi-shard"
	traceMemorystoreGet              = "pendo.io/memorystore/get"
	traceMemorystoreGetMulti         = "pendo.io/memorystore/get-multi"
	traceMemorystoreGetMultiShard    = "pendo.io/memorystore/get-multi-shard"
	traceMemorystoreIncr             = "pendo.io/memorystore/increment"
	traceMemorystoreIncrExisting     = "pendo.io/memorystore/increment-existing"
	traceMemorystoreSet              = "pendo.io/memorystore/set"
	traceMemorystoreSetMulti         = "pendo.io/memorystore/set-multi"
	traceMemorystoreSetMultiShard    = "pendo.io/memorystore/set-multi-shard"

	traceMemcacheAdd          = "pendo.io/memcache/add"
	traceMemcacheCAS          = "pendo.io/memcache/cas"
	traceMemcacheDelete       = "pendo.io/memcache/delete"
	traceMemcacheGet          = "pendo.io/memcache/get"
	traceMemcacheGetMulti     = "pendo.io/memcache/get-multi"
	traceMemcacheIncr         = "pendo.io/memcache/increment"
	traceMemcacheIncrExisting = "pendo.io/memcache/increment-existing"
	traceMemcacheSet          = "pendo.io/memcache/set"

	traceLabelFirstKey  = attribute.Key("first-key")
	traceLabelKey       = attribute.Key("key")
	traceLabelFullKey   = attribute.Key("full-key")
	traceLabelKind      = attribute.Key("kind")
	traceLabelNamespace = attribute.Key("namespace")
	traceLabelNumKeys   = attribute.Key("num-keys")
	traceLabelShard     = attribute.Key("shard")
)

func labelFirstKey(val string) attribute.KeyValue { return traceLabelFirstKey.String(val) }

func labelKey(val string) attribute.KeyValue { return traceLabelKey.String(val) }

func labelFullKey(val string) attribute.KeyValue { return traceLabelFullKey.String(val) }

func labelKind(val string) attribute.KeyValue { return traceLabelKind.String(val) }

func labelNamespace(val string) attribute.KeyValue { return traceLabelNamespace.String(val) }

func labelNumKeys(val int64) attribute.KeyValue { return traceLabelNumKeys.Int64(val) }

func labelShard(val int64) attribute.KeyValue { return traceLabelShard.Int64(val) }
