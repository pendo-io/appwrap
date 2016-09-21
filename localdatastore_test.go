package appwrap

import (
	"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) TestMemDsNewKey(c *C) {
	mem := NewLocalDatastore()
	k := mem.NewKey("Kind", "string", 0, nil)
	c.Assert(k.Kind(), Equals, "Kind")
	c.Assert(k.StringID(), Equals, "string")
	c.Assert(k.IntID(), Equals, int64(0))
	c.Assert(k.Parent(), IsNil)

	k2 := mem.NewKey("Child", "", 50, k)
	c.Assert(k2.Kind(), Equals, "Child")
	c.Assert(k2.StringID(), Equals, "")
	c.Assert(k2.IntID(), Equals, int64(50))
	c.Assert(k2.Parent(), Equals, k)
}

type simpleEntity struct {
	S string
}

type customEntity struct {
	i int
}

func (c *customEntity) Load(props []datastore.Property) error {
	for _, prop := range props {
		if prop.Name != "i" {
			return fmt.Errorf("unknown property %s", prop.Name)
		} else {
			c.i = prop.Value.(int)
		}
	}

	return nil
}

func (c *customEntity) Save() ([]datastore.Property, error) {
	return []datastore.Property{{Name: "i", Value: c.i}}, nil
}

func (dsit *AppengineInterfacesTest) TestMemDsPutGet(c *C) {
	mem := NewLocalDatastore()
	k, err := mem.Put(mem.NewKey("test", "keyval", 0, nil), &simpleEntity{"hello"})
	c.Assert(err, IsNil)
	c.Assert(k.StringID(), Equals, "keyval")
	c.Assert(k.IntID(), Equals, int64(0))

	k2, err := mem.Put(mem.NewKey("test", "", 0, nil), &customEntity{5})
	c.Assert(err, IsNil)
	c.Assert(k2.StringID(), Equals, "")
	c.Assert(k2.IntID() > 10000, Equals, true)

	k3, err := mem.Put(mem.NewKey("test", "", 5, nil), &customEntity{10})
	c.Assert(err, IsNil)
	c.Assert(k3.StringID(), Equals, "")
	c.Assert(k3.IntID(), Equals, int64(5))

	var s simpleEntity
	c.Assert(mem.Get(k, &s), IsNil)
	c.Assert(s.S, Equals, "hello")

	var e customEntity
	c.Assert(mem.Get(k2, &e), IsNil)
	c.Assert(e.i, Equals, 5)
	c.Assert(mem.Get(k3, &e), IsNil)
	c.Assert(e.i, Equals, 10)

	// try some error cases
	c.Assert(mem.Get(mem.NewKey("test", "missing", 0, nil), &s), Equals, datastore.ErrNoSuchEntity)

	_, err = mem.Put(mem.NewKey("test", "keyval", 0, nil), simpleEntity{"hello"})
	c.Assert(err, NotNil)
	c.Assert(mem.Get(k2, s), NotNil)

}

func (dsit *AppengineInterfacesTest) TestMemDsAllocateIds(c *C) {
	mem := NewLocalDatastore()

	first, last, err := mem.AllocateIDs("simple", nil, 10)
	c.Assert(err, IsNil)
	c.Assert(first > 10000, Equals, true)
	c.Assert(last-first+1, Equals, int64(10))

	first, last, err = mem.AllocateIDs("simple", nil, 5)
	c.Assert(err, IsNil)
	c.Assert(last-first+1, Equals, int64(5))
}

func (dsit *AppengineInterfacesTest) TestMemDsPutGetDeleteMulti(c *C) {
	mem := NewLocalDatastore()

	items := make([]simpleEntity, 5)
	itemPtrs := make([]*simpleEntity, 5)
	keys := make([]*datastore.Key, 5)

	for i := 1; i <= 5; i++ {
		keys[i-1] = mem.NewKey("test", "", int64(i), nil)
		items[i-1] = simpleEntity{fmt.Sprintf("%d", i)}
		itemPtrs[i-1] = &items[i-1]
	}

	finalKeys, err := mem.PutMulti(keys, items)
	c.Assert(err, IsNil)
	for i := range keys {
		c.Assert(finalKeys[i], DeepEquals, keys[i])
	}

	mem = NewLocalDatastore()
	finalKeys, err = mem.PutMulti(keys, itemPtrs)
	c.Assert(err, IsNil)
	for i := range keys {
		c.Assert(finalKeys[i], DeepEquals, keys[i])
	}

	gotItems := make([]simpleEntity, 5)
	c.Assert(mem.GetMulti(keys, gotItems), IsNil)
	for i := range keys {
		c.Assert(gotItems[i].S, Equals, items[i].S)
	}

	/* this should work?
	gotItemPtrs := make([]*simpleEntity, 5)
	c.Assert(mem.GetMulti(keys, gotItemPtrs), IsNil)
	for i := range keys {
		c.Assert(gotItemPtrs[i].S, Equals, items[i].S)
	}
	*/

	c.Assert(mem.DeleteMulti(keys[0:2]), IsNil)
	c.Assert(mem.GetMulti(keys, gotItems), NotNil)
	c.Assert(mem.GetMulti(keys[2:5], gotItems), IsNil)
	for i := 0; i < 3; i++ {
		c.Assert(gotItems[i].S, Equals, items[i+2].S)
	}

	_, err = mem.Put(keys[0], &items[0])
	c.Assert(err, IsNil)
	err = mem.DeleteMulti(keys[0:2])
	c.Assert(err, NotNil)
	multiErr, okay := err.(appengine.MultiError)
	c.Assert(okay, Equals, true)
	c.Assert(multiErr, HasLen, 2)
	c.Assert(multiErr[0], IsNil)
	c.Assert(multiErr[1], Equals, datastore.ErrNoSuchEntity)
}

func (dsit *AppengineInterfacesTest) TestMemDsPutGetDeleteMultiLoadSaver(c *C) {
	mem := NewLocalDatastore()

	items := make([]customEntity, 5)
	itemPtrs := make([]*customEntity, 5)
	keys := make([]*datastore.Key, 5)

	for i := 1; i <= 5; i++ {
		keys[i-1] = mem.NewKey("test", "", int64(i), nil)
		items[i-1] = customEntity{i}
		itemPtrs[i-1] = &items[i-1]
	}

	_, err := mem.PutMulti(keys, items)
	c.Assert(err, IsNil)

	gotItems := make([]customEntity, 5)
	c.Assert(mem.GetMulti(keys, gotItems), IsNil)
	for i := range keys {
		c.Assert(gotItems[i].i, Equals, items[i].i)
	}

	mem = NewLocalDatastore()
	_, err = mem.PutMulti(keys, itemPtrs)
	c.Assert(err, IsNil)

	gotItems = make([]customEntity, 5)
	c.Assert(mem.GetMulti(keys, gotItems), IsNil)
	for i := range keys {
		c.Assert(gotItems[i].i, Equals, items[i].i)
	}
}

func (dsit *AppengineInterfacesTest) TestMemDsQueryGetAll(c *C) {
	mem := NewLocalDatastore()
	parent := mem.NewKey("parent", "item", 0, nil)

	items := make([]customEntity, 5)
	keys := make([]*datastore.Key, 5)
	for i := 1; i <= 5; i++ {
		if i%2 == 0 {
			keys[i-1] = mem.NewKey("test", "", int64(i), parent)
		} else {
			keys[i-1] = mem.NewKey("test", "", int64(i), nil)
		}

		items[i-1] = customEntity{i}
	}

	_, err := mem.PutMulti(keys, items)
	c.Assert(err, IsNil)

	_, err = mem.Put(mem.NewKey("otherkind", "", 0, nil), &simpleEntity{"hello"})

	q := mem.NewQuery("test")

	var results []customEntity
	gotKeys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 5)
	c.Assert(results, HasLen, 5)
	for i := range keys {
		c.Assert(gotKeys[i].IntID(), Equals, int64(i+1))
		c.Assert(results[i].i, Equals, items[i].i)
	}

	results = nil
	gotKeys, err = q.Limit(2).Offset(2).GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 2)
	c.Assert(results, HasLen, 2)
	for i := 3; i <= 4; i++ {
		c.Assert(gotKeys[i-3].IntID(), Equals, int64(i))
		c.Assert(results[i-3].i, Equals, i)
	}

	results = nil
	gotKeys, err = q.Filter("i >", 2).GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 3)
	c.Assert(results, HasLen, 3)
	for i := 3; i <= 5; i++ {
		c.Assert(gotKeys[i-3].IntID(), Equals, int64(i))
		c.Assert(results[i-3].i, Equals, i)
	}

	q = q.Ancestor(parent)
	results = nil
	gotKeys, err = q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 2)
	c.Assert(results, HasLen, 2)

	for i := 2; i < 5; i += 2 {
		if i%2 == 0 {
			c.Assert(gotKeys[i/2-1].IntID(), Equals, int64(i))
			c.Assert(results[i/2-1].i, Equals, i)
		}
	}

	q = q.KeysOnly()
	results = nil
	gotKeys, err = q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 2)
	c.Assert(results, HasLen, 0)
}

func (dsit *AppengineInterfacesTest) TestMemDsQueryRun(c *C) {
	mem := NewLocalDatastore()

	items := make([]customEntity, 5)
	keys := make([]*datastore.Key, 5)
	for i := 1; i <= 5; i++ {
		keys[i-1] = mem.NewKey("test", "", int64(i), nil)
		items[i-1] = customEntity{i}
	}

	_, err := mem.PutMulti(keys, items)
	c.Assert(err, IsNil)

	q := mem.NewQuery("test")
	iter := q.Run()
	startCursor, _ := iter.Cursor()
	var middleCursor DatastoreCursor
	for i := 1; i <= 5; i++ {
		var item customEntity
		k, err := iter.Next(&item)
		c.Assert(err, IsNil)
		c.Assert(k.IntID(), Equals, int64(i))
		c.Assert(item.i, Equals, i)
		if i == 2 {
			middleCursor, _ = iter.Cursor()
		}
	}
	_, err = iter.Next(nil)
	c.Assert(err, Equals, datastore.Done)

	endCursor, _ := iter.Cursor()

	var item customEntity
	k, err := q.Start(startCursor).Run().Next(&item)
	c.Assert(err, IsNil)
	c.Assert(k.IntID(), Equals, int64(1))
	c.Assert(item.i, Equals, 1)

	k, err = q.Start(middleCursor).Run().Next(&item)
	c.Assert(err, IsNil)
	c.Assert(k.IntID(), Equals, int64(3))
	c.Assert(item.i, Equals, 3)

	k, err = q.Start(endCursor).Run().Next(&item)
	c.Assert(err, Equals, datastore.Done)

	q = q.KeysOnly()
	iter = q.Run()
	for i := 1; i <= 5; i++ {
		k, err := iter.Next(nil)
		c.Assert(err, IsNil)
		c.Assert(k.IntID(), Equals, int64(i))
	}
	_, err = iter.Next(nil)
	c.Assert(err, Equals, datastore.Done)
}

func (dsit *AppengineInterfacesTest) TestMemDsUnindexed(c *C) {
	mem := NewLocalDatastore()

	type unindexedEntity struct {
		S string `datastore:",noindex"`
	}

	iKey := mem.NewKey("test", "i", 1, nil)
	uKey := mem.NewKey("test", "u", 1, nil)
	_, err := mem.Put(iKey, &simpleEntity{"item"})
	c.Assert(err, IsNil)
	_, err = mem.Put(uKey, &unindexedEntity{"item"})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test")
	var results []simpleEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
	c.Assert(results, HasLen, 2)

	q = mem.NewQuery("test").Filter("S =", "item")
	results = nil
	keys, err = q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(results, HasLen, 1)
	c.Assert(keys[0].Equal(iKey), Equals, true)

	q = mem.NewQuery("test").Order("S")
	results = nil
	keys, err = q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(results, HasLen, 1)
	c.Assert(keys[0].Equal(iKey), Equals, true)
}

func (dsit *AppengineInterfacesTest) TestMemDsListQuery(c *C) {
	mem := NewLocalDatastore()

	type listEntity struct {
		S []string
	}

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	_, err := mem.Put(k1, &listEntity{[]string{"one", "two", "three"}})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &listEntity{[]string{"one", "four", "five"}})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").Filter("S =", "two")
	var results []listEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(keys[0].IntID(), Equals, int64(1))

	q = mem.NewQuery("test").Filter("S =", "one")
	results = nil
	keys, err = q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
}

func (dsit *AppengineInterfacesTest) TestTransaction(c *C) {
	mem := NewLocalDatastore()

	items := make([]customEntity, 5)
	keys := make([]*datastore.Key, 5)
	for i := 1; i <= 5; i++ {
		keys[i-1] = mem.NewKey("test", "", int64(i), nil)
		items[i-1] = customEntity{i}
	}

	_, err := mem.PutMulti(keys, items)
	c.Assert(err, IsNil)

	// Make a change and commit it
	c.Assert(mem.RunInTransaction(func(ds Datastore) error {
		var e customEntity
		err := ds.Get(keys[2], &e)
		e.i = 9000
		_, err = ds.Put(keys[2], &e)
		return err
	}, nil), IsNil)

	var updatedE customEntity
	c.Assert(mem.Get(keys[2], &updatedE), IsNil)
	c.Check(updatedE.i, Equals, 9000)

	// Make a change and roll it back
	c.Assert(mem.RunInTransaction(func(ds Datastore) error {
		var e customEntity
		_ = ds.Get(keys[2], &e)
		e.i = 42
		_, _ = ds.Put(keys[2], &e)
		return fmt.Errorf("NOOOOOO")
	}, nil), NotNil)

	c.Assert(mem.Get(keys[2], &updatedE), IsNil)
	c.Check(updatedE.i, Equals, 9000)

	// Now let's make a change and abort
}
