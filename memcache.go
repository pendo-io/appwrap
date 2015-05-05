// +build appengine appenginevm

package appwrap

import (
	"google.golang.org/appengine/memcache"
)

type Memcache interface {
	Add(item *memcache.Item) error
	AddMulti(item []*memcache.Item) error
	CompareAndSwap(item *memcache.Item) error
	Delete(key string) error
	DeleteMulti(keys []string) error
	Flush() error
	Get(key string) (*memcache.Item, error)
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Increment(key string, amount int64, initialValue uint64) (uint64, error)
	IncrementExisting(key string, amount int64) (uint64, error)
	Namespace(ns string) Memcache
	Set(item *memcache.Item) error
	SetMulti(item []*memcache.Item) error
}
