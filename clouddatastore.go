package appwrap

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type DatastoreEntity = datastore.Entity
type DatastoreKey = datastore.Key
type DatastoreProperty = datastore.Property
type DatastorePropertyList = datastore.PropertyList
type DatastorePropertyLoadSaver = datastore.PropertyLoadSaver
type DatastoreTransactionOptions datastore.TransactionOption
type GeoPoint = datastore.GeoPoint
type PendingKey = datastore.PendingKey

var DatastoreDone = iterator.Done
var ErrConcurrentTransaction = datastore.ErrConcurrentTransaction
var ErrInvalidEntityType = datastore.ErrInvalidEntityType
var ErrInvalidKey = datastore.ErrInvalidKey
var ErrNoSuchEntity = datastore.ErrNoSuchEntity

func KeyKind(key *DatastoreKey) string {
	return key.Kind
}

func KeyParent(key *DatastoreKey) *DatastoreKey {
	return key.Parent
}

func KeyIntID(key *DatastoreKey) int64 {
	return key.ID
}

func KeyStringID(key *DatastoreKey) string {
	return key.Name
}

func KeyNamespace(key *DatastoreKey) string {
	return key.Namespace
}

// SetKeyNamespace recursively sets the namespace for a key and its parents.
func SetKeyNamespace(key *DatastoreKey, ns string) *DatastoreKey {
	for k := key; k != nil; k = k.Parent {
		k.Namespace = ns
	}
	return key
}

func LoadStruct(dest interface{}, props DatastorePropertyList) error {
	return datastore.LoadStruct(dest, props)
}

func SaveStruct(src interface{}) (DatastorePropertyList, error) {
	return datastore.SaveStruct(src)
}

func DecodeKey(encoded string) (*datastore.Key, error) {
	return datastore.DecodeKey(encoded)
}

func newKey(ctx context.Context, kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	key := &datastore.Key{
		Kind:   kind,
		ID:     iId,
		Name:   sId,
		Parent: parent,
	}

	return key
}

// Since appengine.MultiError is used by the calling code to apply to memcache, datastore, and cloud tasks,
// we have to convert datastore.MultiError into appengine.MultiError.
func convertIfMultiError(err error) error {
	if mErr, ok := err.(datastore.MultiError); ok {
		return MultiError(mErr)
	}
	return err
}

type CloudDatastore struct {
	ctx       context.Context
	client    *datastore.Client
	namespace string
	timeout   time.Duration
}

var NewDatastore = NewCloudDatastore

var (
	dsClientMtx sync.Mutex
	dsClient    *datastore.Client

	dsClientTimeout time.Duration = 0 // default: no timeout
)

func newCloudDatastore(c context.Context, client *datastore.Client, namespace string, timeout time.Duration) Datastore {
	return CloudDatastore{client: client, ctx: c, namespace: namespace, timeout: timeout}
}

func NewCloudDatastore(c context.Context) (Datastore, error) {
	var err error
	if dsClient == nil {
		dsClientMtx.Lock()
		defer dsClientMtx.Unlock()
		if dsClient == nil {
			aeInfo := NewAppengineInfoFromContext(c)
			o := []option.ClientOption{
				// Options borrowed from construction of the pubsub client
				option.WithGRPCConnectionPool(2 * runtime.GOMAXPROCS(0)),
				option.WithGRPCDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time: 5 * time.Minute,
				})),
			}
			if dsClient, err = datastore.NewClient(context.Background(), aeInfo.DataProjectID(), o...); err != nil {
				return nil, convertIfMultiError(err)
			}

			if timeoutStr := os.Getenv("cloud_datastore_timeout_ms"); timeoutStr != "" {
				if d, err := strconv.ParseUint(timeoutStr, 10, 64); err != nil {
					panic(fmt.Errorf("invalid timeout %s in cloud_datastore_timeoutms: %s", timeoutStr, err))
				} else {
					dsClientTimeout = time.Duration(d) * time.Millisecond
				}
			}
		}
	}

	return newCloudDatastore(c, dsClient, "", dsClientTimeout), nil
}

var ErrDatastoreDoNotRetry = status.Error(codes.Canceled, "request context canceled")

func withDeadline(parent context.Context, time time.Time, f func(context.Context) error) error {
	tctx, cancel := context.WithDeadline(parent, time)
	defer cancel()
	err := f(tctx)
	if err != nil && tctx.Err() != nil && parent.Err() == nil {
		// The individual request timed out, but the parent context is still alive; return a retryable error to the caller.
		return status.Error(codes.DeadlineExceeded, "datastore timeout")
	} else if parent.Err() != nil {
		return ErrDatastoreDoNotRetry
	}
	return err // otherwise, just return whatever error we got
}

func withTimeout(parent context.Context, timeout time.Duration, f func(context.Context) error) error {
	if timeout == 0 {
		return f(parent)
	}
	return withDeadline(parent, time.Now().Add(timeout), f)
}

func (cds CloudDatastore) withDefaultTimeout(f func(context.Context) error) error {
	return withTimeout(cds.ctx, cds.timeout, f)
}

func (cds CloudDatastore) Deadline(t time.Time) Datastore {
	c, _ := context.WithDeadline(cds.ctx, t)
	return newCloudDatastore(c, cds.client, cds.namespace, cds.timeout)
}

func (cds CloudDatastore) Namespace(ns string) Datastore {
	newCds := cds
	newCds.namespace = ns
	return newCds
}

func (cds CloudDatastore) AllocateIDSet(incompleteKeys []*DatastoreKey) ([]*DatastoreKey, error) {
	var res []*DatastoreKey
	err := cds.withDefaultTimeout(func(tctx context.Context) error {
		var err error
		res, err = cds.client.AllocateIDs(tctx, incompleteKeys)
		return err
	})
	return res, convertIfMultiError(err)
}

func (cds CloudDatastore) DeleteMulti(keys []*DatastoreKey) error {
	err := cds.withDefaultTimeout(func(tctx context.Context) error {
		return cds.client.DeleteMulti(tctx, keys)
	})
	return convertIfMultiError(err)
}

func (cds CloudDatastore) Get(key *DatastoreKey, dst interface{}) error {
	err := cds.withDefaultTimeout(func(tctx context.Context) error {
		return cds.client.Get(tctx, key, dst)
	})
	return convertIfMultiError(err)
}

func (cds CloudDatastore) GetMulti(keys []*DatastoreKey, dst interface{}) error {
	err := cds.withDefaultTimeout(func(tctx context.Context) error {
		return cds.client.GetMulti(tctx, keys, dst)
	})
	return convertIfMultiError(err)
}

func (cds CloudDatastore) Kinds() (kinds []string, err error) {
	keys, err := cds.NewQuery("__kind__").KeysOnly().GetAll(nil)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		kinds = append(kinds, k.Name)
	}
	return kinds, nil
}

func (cds CloudDatastore) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {

	var namespace string
	if parent != nil {
		namespace = parent.Namespace
	} else {
		namespace = cds.namespace
	}

	key := newKey(nil, kind, sId, iId, parent)
	key.Namespace = namespace

	return key
}

func (cds CloudDatastore) Put(key *DatastoreKey, src interface{}) (*DatastoreKey, error) {
	var res *DatastoreKey
	err := cds.withDefaultTimeout(func(tctx context.Context) error {
		var err error
		res, err = cds.client.Put(tctx, key, src)
		return err
	})

	return res, convertIfMultiError(err)
}

func (cds CloudDatastore) PutMulti(keys []*DatastoreKey, src interface{}) ([]*DatastoreKey, error) {
	var res []*DatastoreKey
	err := cds.withDefaultTimeout(func(tctx context.Context) error {
		var err error
		res, err = cds.client.PutMulti(tctx, keys, src)
		return err
	})
	return res, convertIfMultiError(err)
}

func (cds CloudDatastore) RunInTransaction(f func(coreds DatastoreTransaction) error, opts *DatastoreTransactionOptions) (Commit, error) {
	if opts != nil {
		panic("transaction options not supported")
	}

	var commit *datastore.Commit
	err := cds.withDefaultTimeout(func(tctx context.Context) error {
		var err error
		commit, err = cds.client.RunInTransaction(tctx, func(transaction *datastore.Transaction) error {
			ct := CloudTransaction{
				client:      cds.client,
				ctx:         tctx,
				namespace:   cds.namespace,
				transaction: transaction,
			}
			return f(ct)
		}, datastore.MaxAttempts(1))
		return err
	})

	return CloudDatastoreCommit{ctx: cds.ctx, commit: commit}, convertIfMultiError(err)
}

func (cds CloudDatastore) NewQuery(kind string) DatastoreQuery {
	q := CloudDatastoreQuery{ctx: cds.ctx, client: cds.client, timeout: cds.timeout, q: datastore.NewQuery(kind)}
	if cds.namespace != "" {
		q.q = q.q.Namespace(cds.namespace)
	}

	return q
}

type CloudTransaction struct {
	ctx         context.Context
	client      *datastore.Client
	namespace   string
	transaction *datastore.Transaction
}

func (ct CloudTransaction) DeleteMulti(keys []*DatastoreKey) error {
	err := ct.transaction.DeleteMulti(keys)
	return convertIfMultiError(err)
}

func (ct CloudTransaction) Get(key *DatastoreKey, dst interface{}) error {
	err := ct.transaction.Get(key, dst)
	return convertIfMultiError(err)
}

func (ct CloudTransaction) GetMulti(keys []*DatastoreKey, dst interface{}) error {
	err := ct.transaction.GetMulti(keys, dst)
	return convertIfMultiError(err)
}

func (ct CloudTransaction) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	key := newKey(nil, kind, sId, iId, parent)
	key.Namespace = ct.namespace

	return key
}

func (ct CloudTransaction) NewQuery(kind string) DatastoreQuery {
	q := CloudDatastoreQuery{ctx: ct.ctx, client: ct.client, timeout: 0 /* no timeout since ct.ctx already has deadline */, q: datastore.NewQuery(kind)}
	q.q = q.q.Transaction(ct.transaction)
	if ct.namespace != "" {
		q.q = q.q.Namespace(ct.namespace)
	}

	return q
}

func (ct CloudTransaction) Put(key *DatastoreKey, src interface{}) (*PendingKey, error) {
	res, err := ct.transaction.Put(key, src)
	return res, convertIfMultiError(err)
}

func (ct CloudTransaction) PutMulti(keys []*DatastoreKey, src interface{}) ([]*PendingKey, error) {
	res, err := ct.transaction.PutMulti(keys, src)
	return res, convertIfMultiError(err)
}

type CloudDatastoreCommit struct {
	commit *datastore.Commit
	ctx    context.Context
}

func (cdc CloudDatastoreCommit) Key(pending *PendingKey) *DatastoreKey {
	return cdc.commit.Key(pending)
}

type CloudDatastoreQuery struct {
	ctx     context.Context
	client  *datastore.Client
	q       *datastore.Query
	timeout time.Duration
}

func (cdq CloudDatastoreQuery) Ancestor(ancestor *DatastoreKey) DatastoreQuery {
	q := cdq
	q.q = cdq.q.Ancestor(ancestor)
	return q
}

func (cdq CloudDatastoreQuery) Distinct() DatastoreQuery {
	q := cdq
	q.q = cdq.q.Distinct()
	return q
}

func (cdq CloudDatastoreQuery) Filter(how string, what interface{}) DatastoreQuery {
	q := cdq

	if reflect.ValueOf(what).Kind() == reflect.String {
		switch what.(type) {
		case string:
		default:
			what = reflect.ValueOf(what).String()
		}
	}
	q.q = cdq.q.Filter(how, what)
	return q
}

func (cdq CloudDatastoreQuery) KeysOnly() DatastoreQuery {
	q := cdq
	q.q = cdq.q.KeysOnly()
	return q
}

func (cdq CloudDatastoreQuery) Limit(i int) DatastoreQuery {
	q := cdq
	q.q = cdq.q.Limit(i)
	return q
}

func (cdq CloudDatastoreQuery) Offset(i int) DatastoreQuery {
	q := cdq
	q.q = cdq.q.Offset(i)
	return q
}

func (cdq CloudDatastoreQuery) Order(how string) DatastoreQuery {
	q := cdq
	q.q = cdq.q.Order(how)
	return q
}

func (cdq CloudDatastoreQuery) Project(fieldName ...string) DatastoreQuery {
	q := cdq
	q.q = cdq.q.Project(fieldName...)
	return q
}

func (cdq CloudDatastoreQuery) Start(c DatastoreCursor) DatastoreQuery {
	q := cdq
	q.q = cdq.q.Start(c.(datastore.Cursor))
	return q
}

func (cdq CloudDatastoreQuery) Run() DatastoreIterator {
	return cloudDatastoreIterator{iter: cdq.client.Run(cdq.ctx, cdq.q)}
}

func (cdq CloudDatastoreQuery) GetAll(dst interface{}) ([]*DatastoreKey, error) {
	var keys []*DatastoreKey
	err := withTimeout(cdq.ctx, cdq.timeout, func(tctx context.Context) error {
		var err error
		keys, err = cdq.client.GetAll(tctx, cdq.q, dst)
		return err
	})
	return keys, convertIfMultiError(err)
}

type cloudDatastoreIterator struct {
	iter *datastore.Iterator
}

func (i cloudDatastoreIterator) Next(dst interface{}) (*DatastoreKey, error) {
	key, err := i.iter.Next(dst) // TODO: find a way to put a timeout on Next (see https://github.com/googleapis/google-cloud-go/issues/1663 for support)
	return key, convertIfMultiError(err)
}

func (i cloudDatastoreIterator) Cursor() (DatastoreCursor, error) {
	res, err := i.iter.Cursor()
	return res, convertIfMultiError(err)
}

func ToAppwrapPropertyList(l []DatastoreProperty) []AppwrapProperty {
	awList := make([]AppwrapProperty, 0, len(l))
	for _, prop := range l {
		if intfList, isList := prop.Value.([]interface{}); isList {
			for _, val := range intfList {
				awList = append(awList, AppwrapProperty{
					Multiple: true,
					Name:     prop.Name,
					NoIndex:  prop.NoIndex,
					Value:    val,
				})
			}
		} else {
			awList = append(awList, AppwrapProperty{
				Multiple: false,
				Name:     prop.Name,
				NoIndex:  prop.NoIndex,
				Value:    prop.Value,
			})
		}
	}

	return awList
}

func ToDatastorePropertyList(l []AppwrapProperty) []DatastoreProperty {
	dsList := make([]DatastoreProperty, 0, len(l))
	multipleByName := map[string]int{}
	for _, prop := range l {
		if !prop.Multiple {
			dsList = append(dsList, DatastoreProperty{
				Name:    prop.Name,
				NoIndex: prop.NoIndex,
				Value:   prop.Value,
			})
		} else if index, exists := multipleByName[prop.Name]; exists {
			dsList[index].Value = append(dsList[index].Value.([]interface{}), prop.Value)
		} else {
			multipleByName[prop.Name] = len(dsList)
			dsList = append(dsList, DatastoreProperty{
				Name:    prop.Name,
				NoIndex: prop.NoIndex,
				Value:   []interface{}{prop.Value},
			})
		}
	}

	return dsList
}

// namespace handling for this is slightly different based on appengine datastore keys vs cloud datastore keys
func (ds *LocalDatastore) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	var namespace string
	if parent != nil {
		namespace = parent.Namespace
	} else {
		namespace = "s~memds" // this mirrors StubContext
	}

	key := newKey(ds.emptyContext, kind, sId, iId, parent)
	key.Namespace = namespace

	return key
}
