package appwrap

import (
	"time"
)

type DatastoreCursor interface{}

type DatastoreIterator interface {
	Next(itemPtr interface{}) (*DatastoreKey, error)
	Cursor() (DatastoreCursor, error)
}

type DatastoreQuery interface {
	Ancestor(ancestor *DatastoreKey) DatastoreQuery
	Distinct() DatastoreQuery
	Filter(how string, what interface{}) DatastoreQuery
	KeysOnly() DatastoreQuery
	Limit(i int) DatastoreQuery
	Offset(i int) DatastoreQuery
	Order(how string) DatastoreQuery
	Project(fieldName ...string) DatastoreQuery
	Start(c DatastoreCursor) DatastoreQuery
	Run() DatastoreIterator
	GetAll(dst interface{}) ([]*DatastoreKey, error)
}

type DatastoreTransaction interface {
	DeleteMulti(keys []*DatastoreKey) error
	Get(keys *DatastoreKey, dst interface{}) error
	GetMulti(keys []*DatastoreKey, dst interface{}) error
	NewKey(string, string, int64, *DatastoreKey) *DatastoreKey
	NewQuery(kind string) DatastoreQuery
	Put(key *DatastoreKey, src interface{}) (*PendingKey, error)
	PutMulti(keys []*DatastoreKey, src interface{}) ([]*PendingKey, error)
}

type Datastore interface {
	AllocateIDSet(incompleteKeys []*DatastoreKey) ([]*DatastoreKey, error)
	Deadline(t time.Time) Datastore
	DeleteMulti(keys []*DatastoreKey) error
	Get(keys *DatastoreKey, dst interface{}) error
	GetMulti(keys []*DatastoreKey, dst interface{}) error
	Namespace(ns string) Datastore
	NewKey(string, string, int64, *DatastoreKey) *DatastoreKey
	NewQuery(kind string) DatastoreQuery
	Put(key *DatastoreKey, src interface{}) (*DatastoreKey, error)
	PutMulti(keys []*DatastoreKey, src interface{}) ([]*DatastoreKey, error)
	RunInTransaction(f func(coreds DatastoreTransaction) error, opts *DatastoreTransactionOptions) (Commit, error)
}

type Commit interface {
	Key(pending *PendingKey) *DatastoreKey
}

type LegacyDatastore interface {
	AllocateIDs(kind string, parent *DatastoreKey, n int) (int64, int64, error)
	Kinds() ([]string, error)
	NewKey(string, string, int64, *DatastoreKey) *DatastoreKey
}

type AppwrapProperty struct {
	Multiple bool
	Name     string
	NoIndex  bool
	Value    interface{}
}
