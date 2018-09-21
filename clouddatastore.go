// +build !appengine,!appenginevm

package appwrap

import (
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type DatastoreKey = keyWrapper
type DatastoreProperty = datastore.Property
type DatastorePropertyList = datastore.PropertyList
type DatastorePropertyLoadSaver = datastore.PropertyLoadSaver
type DatastoreTransactionOptions datastore.TransactionOption
type GeoPoint = datastore.GeoPoint
type MultiError = datastore.MultiError
type PendingKey = datastore.PendingKey

var DatastoreDone = iterator.Done
var ErrConcurrentTransaction = datastore.ErrConcurrentTransaction
var ErrNoSuchEntity = datastore.ErrNoSuchEntity

type keyWrapper struct {
	dsKey *datastore.Key
}

func (kw *keyWrapper) Kind() string {
	return kw.dsKey.Kind
}

func (kw *keyWrapper) Parent() *keyWrapper {
	if kw.dsKey.Parent == nil {
		return nil
	}

	return &keyWrapper{dsKey: kw.dsKey.Parent}
}

func (kw *keyWrapper) Equal(other *keyWrapper) bool {
	return kw.dsKey.Equal(other.dsKey)
}

func (kw *keyWrapper) IntID() int64 {
	return kw.dsKey.ID
}

func (kw *keyWrapper) StringID() string {
	return kw.dsKey.Name
}

func LoadStruct(dest interface{}, props DatastorePropertyList) error {
	return datastore.LoadStruct(dest, props)
}

func SaveStruct(src interface{}) (DatastorePropertyList, error) {
	return datastore.SaveStruct(src)
}

func toKeyWrapper(ctx context.Context, key *datastore.Key) *DatastoreKey {
	if key == nil {
		return nil
	}

	return &keyWrapper{dsKey: key}
}

func toDsKeyList(keyWrapperList []*keyWrapper) []*datastore.Key {
	keyList := make([]*datastore.Key, len(keyWrapperList))
	for i, key := range keyWrapperList {
		keyList[i] = key.dsKey
	}

	return keyList
}

func toKeyWrapperList(dsKeyList []*datastore.Key) []*keyWrapper {
	wrappedKeys := make([]*keyWrapper, len(dsKeyList))
	for i, key := range dsKeyList {
		wrappedKeys[i] = &keyWrapper{dsKey: key}
	}

	return wrappedKeys
}

func newKey(ctx context.Context, kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	key := &keyWrapper{
		dsKey: &datastore.Key{
			Kind: kind,
			ID:   iId,
			Name: sId,
		},
	}

	if parent != nil {
		key.dsKey.Parent = parent.dsKey
	}

	return key
}

type CloudDatastore struct {
	ctx       context.Context
	client    *datastore.Client
	namespace string
}

func NewCloudDatastore(c context.Context) (Datastore, error) {
	if client, err := datastore.NewClient(c, ""); err != nil {
		return nil, err
	} else {
		return CloudDatastore{
			client: client,
			ctx:    c,
		}, nil
	}
}

func (cds CloudDatastore) Deadline(t time.Time) Datastore {
	c, _ := context.WithDeadline(cds.ctx, t)
	return CloudDatastore{ctx: c}
}

func (cds CloudDatastore) Namespace(ns string) Datastore {
	newCds := cds
	newCds.namespace = ns
	return newCds
}

func (cds CloudDatastore) AllocateIDSet(incompleteKeys []*DatastoreKey) ([]*DatastoreKey, error) {
	if keys, err := cds.client.AllocateIDs(cds.ctx, toDsKeyList(incompleteKeys)); err != nil {
		return nil, err
	} else {
		return toKeyWrapperList(keys), nil
	}
}

func (cds CloudDatastore) DeleteMulti(keys []*DatastoreKey) error {
	return cds.client.DeleteMulti(cds.ctx, toDsKeyList(keys))
}

func (cds CloudDatastore) Get(key *DatastoreKey, dst interface{}) error {
	return cds.client.Get(cds.ctx, key.dsKey, dst)
}

func (cds CloudDatastore) GetMulti(keys []*DatastoreKey, dst interface{}) error {
	return cds.client.GetMulti(cds.ctx, toDsKeyList(keys), dst)
}

func (cds CloudDatastore) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	key := newKey(nil, kind, sId, iId, parent)
	if cds.namespace != "" {
		key.dsKey.Namespace = cds.namespace
	}

	return key
}

func (cds CloudDatastore) Put(key *DatastoreKey, src interface{}) (*DatastoreKey, error) {
	if k, err := cds.client.Put(cds.ctx, key.dsKey, src); err != nil {
		return nil, err
	} else {
		return toKeyWrapper(cds.ctx, k), nil
	}
}

func (cds CloudDatastore) PutMulti(keys []*DatastoreKey, src interface{}) ([]*DatastoreKey, error) {
	if putKeyList, err := cds.client.PutMulti(cds.ctx, toDsKeyList(keys), src); err != nil {
		return nil, err
	} else {
		return toKeyWrapperList(putKeyList), nil
	}
}

func (cds CloudDatastore) RunInTransaction(f func(coreds DatastoreTransaction) error, opts *DatastoreTransactionOptions) (Commit, error) {
	if opts != nil {
		panic("transaction options not supported")
	}

	commit, err := cds.client.RunInTransaction(cds.ctx, func(transaction *datastore.Transaction) error {
		ct := CloudTransaction{
			client:      cds.client,
			ctx:         cds.ctx,
			namespace:   cds.namespace,
			transaction: transaction,
		}
		return f(ct)
	})

	return CloudDatastoreCommit{ctx: cds.ctx, commit: commit}, err
}

func (cds CloudDatastore) NewQuery(kind string) DatastoreQuery {
	q := CloudDatastoreQuery{ctx: cds.ctx, client: cds.client, q: datastore.NewQuery(kind)}
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
	return ct.transaction.DeleteMulti(toDsKeyList(keys))
}

func (ct CloudTransaction) Get(key *DatastoreKey, dst interface{}) error {
	return ct.transaction.Get(key.dsKey, dst)
}

func (ct CloudTransaction) GetMulti(keys []*DatastoreKey, dst interface{}) error {
	return ct.transaction.GetMulti(toDsKeyList(keys), dst)
}

func (ct CloudTransaction) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	key := newKey(nil, kind, sId, iId, parent)
	if ct.namespace != "" {
		key.dsKey.Namespace = ct.namespace
	}

	return key
}

func (ct CloudTransaction) NewQuery(kind string) DatastoreQuery {
	q := CloudDatastoreQuery{ctx: ct.ctx, client: ct.client, q: datastore.NewQuery(kind)}
	q.q = q.q.Transaction(ct.transaction)
	if ct.namespace != "" {
		q.q = q.q.Namespace(ct.namespace)
	}

	return q
}

func (ct CloudTransaction) Put(key *DatastoreKey, src interface{}) (*PendingKey, error) {
	return ct.transaction.Put(key.dsKey, src)
}

func (ct CloudTransaction) PutMulti(keys []*DatastoreKey, src interface{}) ([]*PendingKey, error) {
	return ct.transaction.PutMulti(toDsKeyList(keys), src)
}

type CloudDatastoreCommit struct {
	commit *datastore.Commit
	ctx    context.Context
}

func (cdc CloudDatastoreCommit) Key(pending *PendingKey) *DatastoreKey {
	return toKeyWrapper(cdc.ctx, cdc.commit.Key(pending))
}

type CloudDatastoreQuery struct {
	ctx    context.Context
	client *datastore.Client
	q      *datastore.Query
}

func (cdq CloudDatastoreQuery) Ancestor(ancestor *DatastoreKey) DatastoreQuery {
	q := cdq
	q.q = cdq.q.Ancestor(ancestor.dsKey)
	return q
}

func (cdq CloudDatastoreQuery) Distinct() DatastoreQuery {
	q := cdq
	q.q = cdq.q.Distinct()
	return q
}

func (cdq CloudDatastoreQuery) Filter(how string, what interface{}) DatastoreQuery {
	q := cdq
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
	keys, err := cdq.client.GetAll(cdq.ctx, cdq.q, dst)
	return toKeyWrapperList(keys), err
}

type cloudDatastoreIterator struct {
	iter *datastore.Iterator
}

func (i cloudDatastoreIterator) Next(dst interface{}) (*DatastoreKey, error) {
	key, err := i.iter.Next(dst)
	return &keyWrapper{dsKey: key}, err
}

func (i cloudDatastoreIterator) Cursor() (DatastoreCursor, error) {
	return i.iter.Cursor()
}
