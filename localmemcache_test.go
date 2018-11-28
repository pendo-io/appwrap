package appwrap

import (
	"time"
	//"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"
	. "gopkg.in/check.v1"
)

func (dsit *AppengineInterfacesTest) TestMemCache(c *C) {
	cache := NewLocalMemcache()
	keys := []string{"k0", "k1", "k2"}
	values := [][]byte{[]byte("zero"), []byte("one"), []byte("two")}

	c.Assert(cache.Add(&memcache.Item{Key: keys[0], Value: values[0]}), IsNil)
	c.Assert(cache.Add(&memcache.Item{Key: keys[0], Value: values[0]}), Equals, memcache.ErrNotStored)
	c.Assert(cache.Set(&memcache.Item{Key: keys[1], Value: values[1]}), IsNil)

	item, err := cache.Get(keys[0])
	c.Assert(err, IsNil)
	c.Assert(item.Value, DeepEquals, values[0])

	items, err := cache.GetMulti(keys)
	c.Assert(err, IsNil)
	c.Assert(items[keys[0]].Value, DeepEquals, values[0])
	c.Assert(items[keys[1]].Value, DeepEquals, values[1])
	c.Assert(items[keys[2]], IsNil)

	c.Assert(cache.DeleteMulti(keys[0:1]), Equals, nil)
	items, err = cache.GetMulti(keys)
	c.Assert(err, IsNil)
	c.Assert(items[keys[0]], IsNil)
	c.Assert(items[keys[1]].Value, DeepEquals, values[1])
	c.Assert(items[keys[2]], IsNil)

	c.Assert(cache.Flush(), IsNil)
	items, err = cache.GetMulti(keys)
	c.Assert(err, IsNil)
	c.Assert(len(items), Equals, 0)

	c.Assert(cache.SetMulti([]*memcache.Item{{Key: keys[0], Value: values[0]}, {Key: keys[1], Value: values[1]}}), IsNil)
	err = cache.AddMulti([]*memcache.Item{{Key: keys[0], Value: values[0]}, {Key: keys[2], Value: values[2]}})
	c.Assert(err, DeepEquals, appengine.MultiError{memcache.ErrNotStored, nil})
	c.Assert(cache.DeleteMulti(keys[0:1]), IsNil)
	c.Assert(cache.DeleteMulti(keys), DeepEquals, appengine.MultiError{memcache.ErrCacheMiss, nil, nil})

	c.Assert(cache.Add(&memcache.Item{Key: keys[0], Value: values[0]}), IsNil)
	_, err = cache.Get(keys[0])
	c.Assert(err, IsNil)
	c.Assert(cache.Delete(keys[0]), IsNil)
	_, err = cache.Get(keys[0])
	c.Assert(err, Equals, memcache.ErrCacheMiss)
	c.Assert(cache.Delete(keys[0]), Equals, memcache.ErrCacheMiss)

	// Make sure zero-value expires != very old expires. Zero value
	// means "never expire".
	c.Assert(cache.Add(&memcache.Item{Key: "neverexpire", Value: []byte("foo"), Expiration: 0}), IsNil)
	c.Assert(cache.Add(&memcache.Item{Key: "alreadyexpired", Value: []byte("bar"), Expiration: time.Duration(5)}), IsNil)
	_, err = cache.Get("neverexpire")
	c.Assert(err, IsNil)
	_, err = cache.Get("alreadyexpired")
	c.Assert(err, Equals, memcache.ErrCacheMiss)

}

func (dsit *AppengineInterfacesTest) TestMemCacheIncrement(c *C) {
	cache := NewLocalMemcache()
	_, err := cache.IncrementExisting("k", 15)
	c.Assert(err, Equals, memcache.ErrCacheMiss)

	v, err := cache.Increment("k", 15, 10)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(25))

	v, err = cache.IncrementExisting("k", -10)
	c.Assert(v, Equals, uint64(15))

	v, err = cache.Increment("k", 1, 0)
	c.Assert(v, Equals, uint64(16))

}

func (dsit *AppengineInterfacesTest) TestMemCacheCAS(c *C) {
	cache := NewLocalMemcache()
	c.Assert(cache.Add(&memcache.Item{Key: "k", Value: []byte("first")}), IsNil)

	item, err := cache.Get("k")
	c.Assert(err, IsNil)
	c.Assert(cache.Set(&memcache.Item{Key: "k", Value: []byte("second")}), IsNil)
	item.Value = []byte("third")
	c.Assert(cache.CompareAndSwap(item), Equals, memcache.ErrCASConflict)

	item, err = cache.Get("k")
	c.Assert(err, IsNil)
	item.Value = []byte("third")
	c.Assert(cache.CompareAndSwap(item), IsNil)

	newItem, err := cache.Get("k")
	c.Assert(err, IsNil)
	c.Assert(string(newItem.Value), Equals, "third")
}
