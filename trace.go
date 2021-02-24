package appwrap

const (
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

	traceLabelFirstKey = "first-key"
	traceLabelKey      = "key"
	traceLabelKind     = "kind"
	traceLabelNumKeys  = "num-keys"
	traceLabelShard    = "shard"
)
