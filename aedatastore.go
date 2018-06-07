// +build appengine appenginevm

package appwrap

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

type AppengineDatastore struct {
	c context.Context
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

func (cds AppengineDatastore) AllocateIDs(kind string, parent *datastore.Key, n int) (int64, int64, error) {
	return datastore.AllocateIDs(cds.c, kind, parent, n)
}

func (cds AppengineDatastore) DeleteMulti(keys []*datastore.Key) error {
	longerTimeoutCtx, _ := context.WithTimeout(cds.c, time.Second*20)
	return datastore.DeleteMulti(longerTimeoutCtx, keys)
}

func (cds AppengineDatastore) Get(key *datastore.Key, dst interface{}) error {
	return datastore.Get(cds.c, key, dst)
}

func (cds AppengineDatastore) GetMulti(keys []*datastore.Key, dst interface{}) error {
	return datastore.GetMulti(cds.c, keys, dst)
}

func (cds AppengineDatastore) Kinds() ([]string, error) {
	return datastore.Kinds(cds.c)
}

func (cds AppengineDatastore) NewKey(kind string, sId string, iId int64, parent *datastore.Key) *datastore.Key {
	return datastore.NewKey(cds.c, kind, sId, iId, parent)
}

func (cds AppengineDatastore) Put(key *datastore.Key, src interface{}) (*datastore.Key, error) {
	return datastore.Put(cds.c, key, src)
}

func (cds AppengineDatastore) PutMulti(keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	return datastore.PutMulti(cds.c, keys, src)
}

func (cds AppengineDatastore) GetContext() context.Context {
	// this is not part of the core datastore interface. use with great care.
	return cds.c
}

func (cds AppengineDatastore) RunInTransaction(f func(coreds Datastore) error, opts *datastore.TransactionOptions) error {
	return datastore.RunInTransaction(cds.c, func(c context.Context) error {
		return f(AppengineDatastore{c})
	}, opts)
}

func (cds AppengineDatastore) NewQuery(kind string) DatastoreQuery {
	return &appengineDatastoreQuery{datastore.NewQuery(kind), cds}
}

type appengineDatastoreQuery struct {
	*datastore.Query
	cds AppengineDatastore
}

func (q *appengineDatastoreQuery) nest(newQ *datastore.Query) DatastoreQuery {
	return &appengineDatastoreQuery{newQ, q.cds}
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
	return &appengineDatastoreIterator{q.Query.Run(q.cds.c)}
}

func (q *appengineDatastoreQuery) GetAll(dst interface{}) ([]*datastore.Key, error) {
	return q.Query.GetAll(q.cds.c, dst)
}

func (q *appengineDatastoreQuery) Order(how string) DatastoreQuery {
	q2 := q.nest(q.Query.Order(how))
	return q2
}

func (q *appengineDatastoreQuery) Ancestor(ancestor *datastore.Key) DatastoreQuery {
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

func (i *appengineDatastoreIterator) Next(dst interface{}) (*datastore.Key, error) {
	return i.iter.Next(dst)
}

func (i *appengineDatastoreIterator) Cursor() (DatastoreCursor, error) {
	return i.iter.Cursor()
}
