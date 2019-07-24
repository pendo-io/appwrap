// +build clouddatastore

package appwrap

import (
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
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

// SetKeyNamespace recursively sets the namespace for a key and its parents, returning a new key.
func SetKeyNamespace(key *DatastoreKey, ns string) *DatastoreKey {
	for ; key != nil; key = key.Parent {
		key.Namespace = ns
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
		return appengine.MultiError(mErr)
	}
	return err
}

type CloudDatastore struct {
	ctx       context.Context
	client    *datastore.Client
	namespace string
}

var NewDatastore = NewCloudDatastore

var dsClient *datastore.Client = nil
var clientMtx = &sync.Mutex{}

func NewCloudDatastore(c context.Context) (Datastore, error) {
	var err error
	if dsClient == nil {
		clientMtx.Lock()
		defer clientMtx.Unlock()
		if dsClient == nil {
			aeInfo := NewAppengineInfoFromContext(c)
			if dsClient, err = datastore.NewClient(c, aeInfo.AppID()); err != nil {
				return nil, convertIfMultiError(err)
			}
		}
	}

	return CloudDatastore{
		client: dsClient,
		ctx:    c,
	}, nil

}

func (cds CloudDatastore) Deadline(t time.Time) Datastore {
	c, _ := context.WithDeadline(cds.ctx, t)
	return CloudDatastore{client: cds.client, ctx: c}
}

func (cds CloudDatastore) Namespace(ns string) Datastore {
	newCds := cds
	newCds.namespace = ns
	return newCds
}

func (cds CloudDatastore) AllocateIDSet(incompleteKeys []*DatastoreKey) ([]*DatastoreKey, error) {
	res, err := cds.client.AllocateIDs(cds.ctx, incompleteKeys)
	return res, convertIfMultiError(err)
}

func (cds CloudDatastore) DeleteMulti(keys []*DatastoreKey) error {
	err := cds.client.DeleteMulti(cds.ctx, keys)
	return convertIfMultiError(err)
}

func (cds CloudDatastore) Get(key *DatastoreKey, dst interface{}) error {
	err := cds.client.Get(cds.ctx, key, dst)
	return convertIfMultiError(err)
}

func (cds CloudDatastore) GetMulti(keys []*DatastoreKey, dst interface{}) error {
	err := cds.client.GetMulti(cds.ctx, keys, dst)
	return convertIfMultiError(err)
}

func (cds CloudDatastore) NewKey(kind string, sId string, iId int64, parent *DatastoreKey) *DatastoreKey {
	key := newKey(nil, kind, sId, iId, parent)
	key.Namespace = cds.namespace

	return key
}

func (cds CloudDatastore) Put(key *DatastoreKey, src interface{}) (*DatastoreKey, error) {
	res, err := cds.client.Put(cds.ctx, key, src)
	return res, convertIfMultiError(err)
}

func (cds CloudDatastore) PutMulti(keys []*DatastoreKey, src interface{}) ([]*DatastoreKey, error) {
	res, err := cds.client.PutMulti(cds.ctx, keys, src)
	return res, convertIfMultiError(err)
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

	return CloudDatastoreCommit{ctx: cds.ctx, commit: commit}, convertIfMultiError(err)
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
	q := CloudDatastoreQuery{ctx: ct.ctx, client: ct.client, q: datastore.NewQuery(kind)}
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
	ctx    context.Context
	client *datastore.Client
	q      *datastore.Query
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
	keys, err := cdq.client.GetAll(cdq.ctx, cdq.q, dst)
	return keys, convertIfMultiError(err)
}

type cloudDatastoreIterator struct {
	iter *datastore.Iterator
}

func (i cloudDatastoreIterator) Next(dst interface{}) (*DatastoreKey, error) {
	key, err := i.iter.Next(dst)
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
	key := newKey(ds.emptyContext, kind, sId, iId, parent)
	key.Namespace = "s~memds" // this mirrors StubContext
	return key
}
