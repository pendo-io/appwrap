// +build !clouddatastore

package appwrap

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

type DatastoreKey = datastore.Key
type DatastoreProperty = datastore.Property
type DatastorePropertyList = datastore.PropertyList
type DatastorePropertyLoadSaver = datastore.PropertyLoadSaver
type DatastoreTransactionOptions = datastore.TransactionOptions
type GeoPoint = appengine.GeoPoint
type PendingKey struct{ key *datastore.Key }

var DatastoreDone = datastore.Done
var ErrConcurrentTransaction = datastore.ErrConcurrentTransaction
var ErrInvalidEntityType = datastore.ErrInvalidEntityType
var ErrInvalidKey = datastore.ErrInvalidKey
var ErrNoSuchEntity = datastore.ErrNoSuchEntity

func KeyKind(key *DatastoreKey) string {
	return key.Kind()
}

func KeyParent(key *DatastoreKey) *DatastoreKey {
	return key.Parent()
}

func KeyIntID(key *DatastoreKey) int64 {
	return key.IntID()
}

func KeyStringID(key *DatastoreKey) string {
	return key.StringID()
}

func KeyNamespace(key *DatastoreKey) string {
	return key.Namespace()
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
	return datastore.NewKey(ctx, kind, sId, iId, parent)
}

type AppengineDatastore struct {
	c context.Context
}

type AppengineDatastoreTransaction struct {
	c context.Context
}

func NewDatastore(c context.Context) (Datastore, error) {
	return AppengineDatastore{c}, nil
}

func NewAppengineDatastore(c context.Context) Datastore {
	return AppengineDatastore{c}
}

func (cds AppengineDatastore) Deadline(t time.Time) Datastore {
	c, _ := context.WithDeadline(cds.c, t)
	return AppengineDatastore{c}
}

func (cds AppengineDatastore) Namespace(ns string) Datastore {
	c, _ := appengine.Namespace(cds.c, ns)
	return AppengineDatastore{c}
}

func (cds AppengineDatastore) AllocateIDSet(incompleteKeys []*DatastoreKey) ([]*DatastoreKey, error) {
	return emulateAllocateIDSet(cds, incompleteKeys)
}

func (cds AppengineDatastore) AllocateIDs(kind string, parent *DatastoreKey, n int) (int64, int64, error) {
	return datastore.AllocateIDs(cds.c, kind, parent, n)
}

func (cds AppengineDatastore) DeleteMulti(keys []*DatastoreKey) error {
	longerTimeoutCtx, _ := context.WithTimeout(cds.c, time.Second*20)
	return datastore.DeleteMulti(longerTimeoutCtx, keys)
}

func (cds AppengineDatastore) Get(key *DatastoreKey, dst interface{}) error {
	return datastore.Get(cds.c, key, dst)
}

func (cds AppengineDatastore) GetMulti(keys []*DatastoreKey, dst interface{}) error {
	return datastore.GetMulti(cds.c, keys, dst)
}

func (cds AppengineDatastore) Kinds() ([]string, error) {
	return datastore.Kinds(cds.c)
}

func (cds AppengineDatastore) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	return datastore.NewKey(cds.c, kind, sId, iId, parent)
}

func (cds AppengineDatastore) Put(key *DatastoreKey, src interface{}) (*DatastoreKey, error) {
	return datastore.Put(cds.c, key, src)
}

func (cds AppengineDatastore) PutMulti(keys []*DatastoreKey, src interface{}) ([]*DatastoreKey, error) {
	return datastore.PutMulti(cds.c, keys, src)
}

func (cds AppengineDatastore) RunInTransaction(f func(coreds DatastoreTransaction) error, opts *DatastoreTransactionOptions) (Commit, error) {
	return unmappedDatastoreCommit{}, datastore.RunInTransaction(cds.c, func(c context.Context) error {
		return f(AppengineDatastoreTransaction{c: c})
	}, opts)
}

func (cds AppengineDatastore) NewQuery(kind string) DatastoreQuery {
	return &appengineDatastoreQuery{Query: datastore.NewQuery(kind), context: cds.c}
}

type appengineDatastoreQuery struct {
	*datastore.Query
	context context.Context
}

func (q *appengineDatastoreQuery) nest(newQ *datastore.Query) DatastoreQuery {
	return &appengineDatastoreQuery{Query: newQ, context: q.context}
}

func (q *appengineDatastoreQuery) KeysOnly() DatastoreQuery {
	return q.nest(q.Query.KeysOnly())
}

func (q *appengineDatastoreQuery) Filter(how string, what interface{}) DatastoreQuery {
	return q.nest(q.Query.Filter(how, what))
}

func (q *appengineDatastoreQuery) Limit(i int) DatastoreQuery {
	return q.nest(q.Query.Limit(i))
}

func (q *appengineDatastoreQuery) Offset(i int) DatastoreQuery {
	return q.nest(q.Query.Offset(i))
}

func (q *appengineDatastoreQuery) Run() DatastoreIterator {
	return &appengineDatastoreIterator{q.Query.Run(q.context)}
}

func (q *appengineDatastoreQuery) GetAll(dst interface{}) ([]*DatastoreKey, error) {
	return q.Query.GetAll(q.context, dst)
}

func (q *appengineDatastoreQuery) Order(how string) DatastoreQuery {
	q2 := q.nest(q.Query.Order(how))
	return q2
}

func (q *appengineDatastoreQuery) Ancestor(ancestor *DatastoreKey) DatastoreQuery {
	return q.nest(q.Query.Ancestor(ancestor))
}

func (q *appengineDatastoreQuery) Start(c DatastoreCursor) DatastoreQuery {
	return q.nest(q.Query.Start(c.(datastore.Cursor)))
}

func (q *appengineDatastoreQuery) Project(fieldName ...string) DatastoreQuery {
	return q.nest(q.Query.Project(fieldName...))
}

func (q *appengineDatastoreQuery) Distinct() DatastoreQuery {
	return q.nest(q.Query.Distinct())
}

type appengineDatastoreIterator struct {
	iter *datastore.Iterator
}

func (i *appengineDatastoreIterator) Next(dst interface{}) (*DatastoreKey, error) {
	return i.iter.Next(dst)
}

func (i *appengineDatastoreIterator) Cursor() (DatastoreCursor, error) {
	return i.iter.Cursor()
}

func (dt AppengineDatastoreTransaction) DeleteMulti(keys []*DatastoreKey) error {
	longerTimeoutCtx, _ := context.WithTimeout(dt.c, time.Second*20)
	return datastore.DeleteMulti(longerTimeoutCtx, keys)
}

func (dt AppengineDatastoreTransaction) Get(key *DatastoreKey, dst interface{}) error {
	return datastore.Get(dt.c, key, dst)
}

func (dt AppengineDatastoreTransaction) GetMulti(keys []*DatastoreKey, dst interface{}) error {
	return datastore.GetMulti(dt.c, keys, dst)
}

func (dt AppengineDatastoreTransaction) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	return datastore.NewKey(dt.c, kind, sId, iId, parent)
}

func (dt AppengineDatastoreTransaction) NewQuery(kind string) DatastoreQuery {
	return &appengineDatastoreQuery{Query: datastore.NewQuery(kind), context: dt.c}
}

func (dt AppengineDatastoreTransaction) Put(key *DatastoreKey, src interface{}) (*PendingKey, error) {
	key, err := datastore.Put(dt.c, key, src)
	return &PendingKey{key: key}, err
}

func (dt AppengineDatastoreTransaction) PutMulti(keys []*DatastoreKey, src interface{}) ([]*PendingKey, error) {
	resultKeys, err := datastore.PutMulti(dt.c, keys, src)
	if err != nil {
		return nil, err
	}

	pendingKeys := make([]*PendingKey, len(resultKeys))
	for i := range resultKeys {
		pendingKeys[i] = &PendingKey{key: resultKeys[i]}
	}

	return pendingKeys, err
}

type unmappedDatastoreCommit struct{}

func (unmappedDatastoreCommit) Key(pending *PendingKey) *DatastoreKey {
	return pending.key
}

func ToAppwrapPropertyList(l []DatastoreProperty) []AppwrapProperty {
	awList := make([]AppwrapProperty, len(l))
	for i, p := range l {
		awList[i] = AppwrapProperty{
			Multiple: p.Multiple,
			Name:     p.Name,
			NoIndex:  p.NoIndex,
			Value:    p.Value,
		}
	}

	return awList
}

func ToDatastorePropertyList(l []AppwrapProperty) []DatastoreProperty {
	dsList := make([]DatastoreProperty, len(l))
	for i, p := range l {
		dsList[i] = DatastoreProperty{
			Multiple: p.Multiple,
			Name:     p.Name,
			NoIndex:  p.NoIndex,
			Value:    p.Value,
		}
	}

	return dsList
}

// namespace handling for this is slightly different based on appengine datastore keys vs cloud datastore keys
func (ds *LocalDatastore) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	return newKey(ds.emptyContext, kind, sId, iId, parent)
}
