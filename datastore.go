// +build appengine appenginevm

package appwrap

import (
	"time"

	"google.golang.org/appengine/datastore"
)

type DatastoreCursor interface{}

type DatastoreIterator interface {
	Next(itemPtr interface{}) (*datastore.Key, error)
	Cursor() (DatastoreCursor, error)
}

type DatastoreQuery interface {
	Ancestor(ancestor *datastore.Key) DatastoreQuery
	Distinct() DatastoreQuery
	Filter(how string, what interface{}) DatastoreQuery
	KeysOnly() DatastoreQuery
	Limit(i int) DatastoreQuery
	Offset(i int) DatastoreQuery
	Order(how string) DatastoreQuery
	Project(fieldName ...string) DatastoreQuery
	Start(c DatastoreCursor) DatastoreQuery
	Run() DatastoreIterator
	GetAll(dst interface{}) ([]*datastore.Key, error)
}

type Datastore interface {
	AllocateIDs(kind string, parent *datastore.Key, n int) (int64, int64, error)
	DeleteMulti(keys []*datastore.Key) error
	Get(keys *datastore.Key, dst interface{}) error
	GetMulti(keys []*datastore.Key, dst interface{}) error
	Kinds() ([]string, error)
	Namespace(ns string) Datastore
	NewKey(string, string, int64, *datastore.Key) *datastore.Key
	Put(key *datastore.Key, src interface{}) (*datastore.Key, error)
	PutMulti(keys []*datastore.Key, src interface{}) ([]*datastore.Key, error)
	RunInTransaction(f func(coreds Datastore) error, opts *datastore.TransactionOptions) error
	NewQuery(kind string) DatastoreQuery
	Deadline(t time.Time) Datastore
}
