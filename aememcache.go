// +build appengine appenginevm

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"
)

type AppengineMemcache struct {
	c context.Context
}

func NewAppengineMemcache(c context.Context) Memcache {
	return AppengineMemcache{c}
}

func (mc AppengineMemcache) Add(item *memcache.Item) error {
	return memcache.Add(mc.c, item)
}

func (mc AppengineMemcache) AddMulti(item []*memcache.Item) error {
	return memcache.AddMulti(mc.c, item)
}

func (mc AppengineMemcache) CompareAndSwap(item *memcache.Item) error {
	return memcache.CompareAndSwap(mc.c, item)
}

func (mc AppengineMemcache) Delete(key string) error {
	return memcache.Delete(mc.c, key)
}

func (mc AppengineMemcache) DeleteMulti(keys []string) error {
	return memcache.DeleteMulti(mc.c, keys)
}

func (mc AppengineMemcache) Flush() error {
	return memcache.Flush(mc.c)
}

func (mc AppengineMemcache) Get(key string) (*memcache.Item, error) {
	return memcache.Get(mc.c, key)
}

func (mc AppengineMemcache) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	return memcache.GetMulti(mc.c, keys)
}

func (mc AppengineMemcache) Increment(key string, amount int64, initialValue uint64) (uint64, error) {
	return memcache.Increment(mc.c, key, amount, initialValue)
}

func (mc AppengineMemcache) IncrementExisting(key string, amount int64) (uint64, error) {
	return memcache.IncrementExisting(mc.c, key, amount)
}

func (mc AppengineMemcache) Set(item *memcache.Item) error {
	return memcache.Set(mc.c, item)
}

func (mc AppengineMemcache) SetMulti(item []*memcache.Item) error {
	return memcache.SetMulti(mc.c, item)
}

func (mc AppengineMemcache) Namespace(ns string) Memcache {
	newContext, _ := appengine.Namespace(mc.c, ns)
	return AppengineMemcache{newContext}
}
