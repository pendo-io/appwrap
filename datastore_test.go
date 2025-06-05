package appwrap

import (
	"fmt"
	"sync"

	. "gopkg.in/check.v1"
)

func (dsit *AppengineInterfacesTest) TestMemDsNewKey(c *C) {
	mem := dsit.newDatastore()
	k := mem.NewKey("Kind", "string", 0, nil)
	c.Assert(KeyKind(k), Equals, "Kind")
	c.Assert(KeyStringID(k), Equals, "string")
	c.Assert(KeyIntID(k), Equals, int64(0))
	c.Assert(KeyParent(k), IsNil)

	k2 := mem.NewKey("Child", "", 50, k)
	c.Assert(KeyKind(k2), Equals, "Child")
	c.Assert(KeyStringID(k2), Equals, "")
	c.Assert(KeyIntID(k2), Equals, int64(50))
	c.Assert(KeyParent(k2).Equal(k), IsTrue)
}

type simpleEntity struct {
	S string
}

type customEntity struct {
	i int
}

func (c *customEntity) Load(props []DatastoreProperty) error {
	for _, prop := range props {
		if prop.Name != "i" {
			return fmt.Errorf("unknown property %s", prop.Name)
		} else {
			switch prop.Value.(type) {
			case int:
				// memory datastore does this
				c.i = prop.Value.(int)
			case int64:
				// cloud datastore library does this
				c.i = int(prop.Value.(int64))
			}
		}
	}

	return nil
}

func (c *customEntity) Save() ([]DatastoreProperty, error) {
	return []DatastoreProperty{{Name: "i", Value: c.i}}, nil
}

func (dsit *AppengineInterfacesTest) TestMemDsPutGet(c *C) {
	mem := dsit.newDatastore()
	k, err := mem.Put(mem.NewKey("test", "keyval", 0, nil), &simpleEntity{"hello"})
	c.Assert(err, IsNil)
	c.Assert(KeyStringID(k), Equals, "keyval")
	c.Assert(KeyIntID(k), Equals, int64(0))

	k2, err := mem.Put(mem.NewKey("test", "", 0, nil), &customEntity{5})
	c.Assert(err, IsNil)
	c.Assert(KeyStringID(k2), Equals, "")
	//c.Assert(KeyIntID(k2) > 10000, Equals, true)

	k3, err := mem.Put(mem.NewKey("test", "", 5, nil), &customEntity{10})
	c.Assert(err, IsNil)
	c.Assert(KeyStringID(k3), Equals, "")
	c.Assert(KeyIntID(k3), Equals, int64(5))

	var s simpleEntity
	c.Assert(mem.Get(k, &s), IsNil)
	c.Assert(s.S, Equals, "hello")

	var e customEntity
	c.Assert(mem.Get(k2, &e), IsNil)
	c.Assert(e.i, Equals, 5)
	c.Assert(mem.Get(k3, &e), IsNil)
	c.Assert(e.i, Equals, 10)

	// try some error cases
	c.Assert(mem.Get(mem.NewKey("test", "missing", 0, nil), &s), Equals, ErrNoSuchEntity)

	_, err = mem.Put(mem.NewKey("test", "keyval", 0, nil), simpleEntity{"hello"})
	c.Assert(err, NotNil)
	c.Assert(mem.Get(k2, s), NotNil)
}

func (dsit *AppengineInterfacesTest) TestMemDsPutGetDeleteMulti(c *C) {
	mem := dsit.newDatastore()

	items := make([]simpleEntity, 5)
	itemPtrs := make([]*simpleEntity, 5)
	keys := make([]*DatastoreKey, 5)

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

	mem = dsit.newDatastore()
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
	c.Assert(mem.GetMulti(keys[2:5], gotItems[0:3]), IsNil)
	for i := 0; i < 3; i++ {
		c.Assert(gotItems[i].S, Equals, items[i+2].S)
	}

	// deleting items that don't exist does not cause an error
	_, err = mem.Put(keys[0], &items[0])
	c.Assert(err, IsNil)
	err = mem.DeleteMulti(keys[0:2])
	c.Assert(err, IsNil)

	// make sure we can't get them
	multiErr := mem.GetMulti(keys[0:2], items[0:2])
	c.Assert(multiErr, NotNil)
	for _, err := range multiErr.(MultiError) {
		c.Assert(err, Equals, ErrNoSuchEntity)
	}
}

func (dsit *AppengineInterfacesTest) TestMemDsPutGetDeleteMultiLoadSaver(c *C) {
	mem := dsit.newDatastore()

	items := make([]customEntity, 5)
	itemPtrs := make([]*customEntity, 5)
	keys := make([]*DatastoreKey, 5)

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

	mem = dsit.newDatastore()
	_, err = mem.PutMulti(keys, itemPtrs)
	c.Assert(err, IsNil)

	gotItems = make([]customEntity, 5)
	c.Assert(mem.GetMulti(keys, gotItems), IsNil)
	for i := range keys {
		c.Assert(gotItems[i].i, Equals, items[i].i)
	}
}

func (dsit *AppengineInterfacesTest) TestMemDsQueryGetAll(c *C) {
	mem := dsit.newDatastore()
	parent := mem.NewKey("parent", "item", 0, nil)

	items := make([]customEntity, 5)
	keys := make([]*DatastoreKey, 5)
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

	q := mem.NewQuery("test").Order("i")

	var results []customEntity
	gotKeys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 5)
	c.Assert(results, HasLen, 5)
	for i := range keys {
		c.Assert(KeyIntID(gotKeys[i]), Equals, int64(i+1))
		c.Assert(results[i].i, Equals, items[i].i)
	}

	results = nil
	gotKeys, err = q.Limit(2).Offset(2).GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 2)
	c.Assert(results, HasLen, 2)
	for i := 3; i <= 4; i++ {
		c.Assert(KeyIntID(gotKeys[i-3]), Equals, int64(i))
		c.Assert(results[i-3].i, Equals, i)
	}

	results = nil
	gotKeys, err = q.Filter("i >", 2).GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(gotKeys, HasLen, 3)
	c.Assert(results, HasLen, 3)
	for i := 3; i <= 5; i++ {
		c.Assert(KeyIntID(gotKeys[i-3]), Equals, int64(i))
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
			c.Assert(KeyIntID(gotKeys[i/2-1]), Equals, int64(i))
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
	mem := dsit.newDatastore()

	items := make([]customEntity, 5)
	keys := make([]*DatastoreKey, 5)
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
		c.Assert(KeyIntID(k), Equals, int64(i))
		c.Assert(item.i, Equals, i)
		if i == 2 {
			middleCursor, _ = iter.Cursor()
		}
	}
	_, err = iter.Next(nil)
	c.Assert(err, Equals, DatastoreDone)

	endCursor, _ := iter.Cursor()

	var item customEntity
	k, err := q.Start(startCursor).Run().Next(&item)
	c.Assert(err, IsNil)
	c.Assert(KeyIntID(k), Equals, int64(1))
	c.Assert(item.i, Equals, 1)

	k, err = q.Start(middleCursor).Run().Next(&item)
	c.Assert(err, IsNil)
	c.Assert(KeyIntID(k), Equals, int64(3))
	c.Assert(item.i, Equals, 3)

	k, err = q.Start(endCursor).Run().Next(&item)
	c.Assert(err, Equals, DatastoreDone)

	q = q.KeysOnly()
	iter = q.Run()
	for i := 1; i <= 5; i++ {
		k, err := iter.Next(nil)
		c.Assert(err, IsNil)
		c.Assert(KeyIntID(k), Equals, int64(i))
	}
	_, err = iter.Next(nil)
	c.Assert(err, Equals, DatastoreDone)
}

func (dsit *AppengineInterfacesTest) TestMemDsUnindexed(c *C) {
	mem := dsit.newDatastore()

	type unindexedEntity struct {
		S string `datastore:",noindex"`
	}

	iKey := mem.NewKey("test", "i", 0, nil)
	uKey := mem.NewKey("test", "u", 0, nil)
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
	mem := dsit.newDatastore()

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
	c.Assert(KeyIntID(keys[0]), Equals, int64(1))

	q = mem.NewQuery("test").Filter("S =", "one")
	results = nil
	keys, err = q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_In(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", "in", []interface{}{1, 2})
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
	c.Assert(KeyIntID(keys[0]), Equals, int64(1))
	c.Assert(KeyIntID(keys[1]), Equals, int64(2))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_NotIn(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", "not-in", []interface{}{1, 2})
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(KeyIntID(keys[0]), Equals, int64(3))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_Equal(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", "=", 1)
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(KeyIntID(keys[0]), Equals, int64(1))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_NotEqual(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", "!=", 1)
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
	c.Assert(KeyIntID(keys[0]), Equals, int64(2))
	c.Assert(KeyIntID(keys[1]), Equals, int64(3))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_LessThan(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", "<", 2)
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(KeyIntID(keys[0]), Equals, int64(1))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_LessThanOrEqual(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", "<=", 2)
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
	c.Assert(KeyIntID(keys[0]), Equals, int64(1))
	c.Assert(KeyIntID(keys[1]), Equals, int64(2))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_GreaterThan(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", ">", 2)
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(KeyIntID(keys[0]), Equals, int64(3))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_GreaterThanOrEqual(c *C) {
	mem := dsit.newDatastore()

	k1 := mem.NewKey("test", "", 1, nil)
	k2 := mem.NewKey("test", "", 2, nil)
	k3 := mem.NewKey("test", "", 3, nil)
	_, err := mem.Put(k1, &customEntity{1})
	c.Assert(err, IsNil)
	_, err = mem.Put(k2, &customEntity{2})
	c.Assert(err, IsNil)
	_, err = mem.Put(k3, &customEntity{3})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("i", ">=", 2)
	var results []customEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
	c.Assert(KeyIntID(keys[0]), Equals, int64(2))
	c.Assert(KeyIntID(keys[1]), Equals, int64(3))
}

func (dsit *AppengineInterfacesTest) TestMemDsFilterField_TypeDefStr(c *C) {
	mem := dsit.newDatastore()

	type customStr string
	k1 := mem.NewKey("test", "", 1, nil)
	_, err := mem.Put(k1, &simpleEntity{"abc"})
	c.Assert(err, IsNil)

	q := mem.NewQuery("test").FilterField("S", "=", customStr("abc"))
	var results []simpleEntity
	keys, err := q.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 1)
	c.Assert(KeyIntID(keys[0]), Equals, int64(1))
}

func (dsit *AppengineInterfacesTest) TestTransaction(c *C) {
	mem := dsit.newDatastore()

	items := make([]customEntity, 5)
	keys := make([]*DatastoreKey, 5)
	for i := 1; i <= 5; i++ {
		keys[i-1] = mem.NewKey("test", "", int64(i), nil)
		items[i-1] = customEntity{i}
	}

	_, err := mem.PutMulti(keys, items)
	c.Assert(err, IsNil)

	// Make a change and commit it
	mem.RunInTransaction(func(ds DatastoreTransaction) error {
		var e customEntity
		err := ds.Get(keys[2], &e)
		c.Assert(err, IsNil)
		e.i = 9000
		_, err = ds.Put(keys[2], &e)
		return err
	}, nil)
	c.Assert(err, IsNil)

	var updatedE customEntity
	c.Assert(mem.Get(keys[2], &updatedE), IsNil)
	c.Check(updatedE.i, Equals, 9000)

	// Make a change and roll it back
	_, err = mem.RunInTransaction(func(ds DatastoreTransaction) error {
		var e customEntity
		err := ds.Get(keys[2], &e)
		c.Assert(err, IsNil)
		e.i = 42
		_, _ = ds.Put(keys[2], &e)
		return fmt.Errorf("NOOOOOO")
	}, nil)
	c.Assert(err.Error(), Equals, "NOOOOOO")

	// Try a query in the transaction. This also makes sure we carry the namespace through to the transaction's
	// new key and query.
	_, err = mem.RunInTransaction(func(ds DatastoreTransaction) error {
		k := ds.NewKey("test", "", 4, nil)
		q := ds.NewQuery("test").Ancestor(k).Filter("i <", 5)
		var results []customEntity
		_, err := q.GetAll(&results)
		c.Assert(err, IsNil)

		return nil
	}, nil)
	c.Assert(err, IsNil)

	c.Assert(mem.Get(keys[2], &updatedE), IsNil)
	c.Check(updatedE.i, Equals, 9000)

}

func (dsit *AppengineInterfacesTest) TestTransactionMultiThreaded(c *C) {
	mem := dsit.newDatastore()

	items := make([]customEntity, 5)
	keys := make([]*DatastoreKey, 5)
	for i := 1; i <= 5; i++ {
		keys[i-1] = mem.NewKey("test", "", int64(i), nil)
		items[i-1] = customEntity{i}
	}

	_, err := mem.PutMulti(keys, items)
	c.Assert(err, IsNil)

	wg := &sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			mem.RunInTransaction(func(ds DatastoreTransaction) error {
				var e customEntity
				// We expect to write e.i to be a 10 x the 1-based count of the thread
				// However, we will always rollback the writes to the fourth value, so
				// we expect to see 10, 20, 30, 4, 50 when verifying.
				_ = ds.Get(keys[i%len(keys)], &e)
				e.i = (i%len(keys) + 1) * 10
				_, _ = ds.Put(keys[i%len(keys)], &e)

				if (i%len(keys) + 1) == 4 {
					return fmt.Errorf("oof")
				}
				return nil
			}, nil)
		}(i)
	}

	wg.Wait()

	for i := range keys {
		var updatedEAgain customEntity
		c.Assert(mem.Get(keys[i], &updatedEAgain), IsNil)
		if i == 3 {
			c.Check(updatedEAgain.i, Equals, 4)
		} else {
			c.Check(updatedEAgain.i, Equals, (i+1)*10)
		}
	}

}

func (dsit *AppengineInterfacesTest) TestDsAllocateIdSet(c *C) {
	ds := dsit.newDatastore()

	templates := make([]*DatastoreKey, 5)
	parent := ds.NewKey("parentKind", "pId", 0, nil)
	for i := range templates {
		templates[i] = ds.NewKey("someKind", "", 0, parent)
	}

	keys, err := ds.AllocateIDSet(templates)
	c.Assert(err, IsNil)

	c.Assert(len(keys), Equals, len(templates))
	vals := make(map[int64]bool)
	for i := range keys {
		c.Assert(KeyKind(keys[i]), Equals, "someKind")
		c.Assert(KeyParent(keys[i]).Equal(parent), IsTrue)
		vals[KeyIntID(keys[i])] = true
	}

	c.Assert(len(vals), Equals, len(templates))
}

func (dsit *AppengineInterfacesTest) TestDistinct(c *C) {
	mem := dsit.newDatastore()

	type thing struct {
		A, B string
		C    int
	}

	var (
		keys   []*DatastoreKey
		things []thing
	)
	for i := 1; i <= 10; i++ {
		keys = append(keys, mem.NewKey("thing", "", int64(i), nil))
		things = append(things, thing{A: fmt.Sprintf("%v", i), B: "blah", C: 10 - i})
	}
	_, err := mem.PutMulti(keys, things)
	c.Assert(err, IsNil)

	//// Test: no projection, so it panics
	//c.Assert(func() { mem.NewQuery("thing").Distinct() }, Panics, "Distinct is only allowed with Projection Queries")

	// Test: projection, no real distinct items, should return all ten things
	var results []thing
	q1 := mem.NewQuery("thing").Project("A").Distinct()
	_, err = q1.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 10)

	// Test: projection on b, which is always the same; should return one item
	results = []thing{}
	q2 := mem.NewQuery("thing").Project("B").Distinct()
	_, err = q2.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 1)

	// Test: projection on b, c, returns all items
	results = []thing{}
	q3 := mem.NewQuery("thing").Project("B", "C").Distinct()
	_, err = q3.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 10)

	// Test: Modify one item and make a duplicate, now should have two distinct items for b
	results = []thing{}
	things[4].B = "blat"
	_, err = mem.Put(keys[4], &things[4])
	c.Assert(err, IsNil)
	q4 := mem.NewQuery("thing").Project("B").Distinct()
	_, err = q4.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 2)
}

func (dsit *AppengineInterfacesTest) TestDeclarations(c *C) {
	_ = DatastoreDone
	_ = ErrConcurrentTransaction
	_ = ErrInvalidEntityType
	_ = ErrInvalidKey
	_ = ErrNoSuchEntity
	_ = ToAppwrapPropertyList
	_ = ToDatastorePropertyList
	_ = KeyKind
	_ = KeyParent
	_ = KeyIntID
	_ = KeyStringID
	_ = KeyNamespace

	var (
		_ = AppwrapProperty{}
		_ = DatastoreEntity{}
		_ = DatastoreKey{}
		_ = DatastoreProperty{}
		_ = DatastorePropertyList{}
		_ = GeoPoint{}
		_ = PendingKey{}
	)

	type (
		_ = DatastorePropertyLoadSaver
	)
}

type loadSaverEntity struct {
	k     *DatastoreKey
	s     string
	iList []int64
	c     *C
}

func (s *loadSaverEntity) Load(props []DatastoreProperty) error {
	for _, prop := range ToAppwrapPropertyList(props) {
		switch prop.Name {
		case "s":
			s.s = prop.Value.(string)
		case "iList":
			s.c.Assert(prop.Multiple, IsTrue)
			s.iList = append(s.iList, prop.Value.(int64))
		}
	}
	return nil
}

type keyLoadSaverEntity struct {
	loadSaverEntity
}

func (s *keyLoadSaverEntity) LoadKey(k *DatastoreKey) error {
	s.c.Assert(s.s, Equals, "") // ensure LoadKey is called before Load
	s.k = k
	return nil
}

func (s *loadSaverEntity) Save() ([]DatastoreProperty, error) {
	aProps := []AppwrapProperty{{Name: "s", Value: s.s}}
	for _, i := range s.iList {
		aProps = append(aProps, AppwrapProperty{Name: "iList", Value: i, Multiple: true})
	}

	return ToDatastorePropertyList(aProps), nil
}

func (dsit *AppengineInterfacesTest) TestPropertyLoadSaver(c *C) {
	ds := dsit.newDatastore()
	s := loadSaverEntity{s: "name", iList: []int64{10, 20, 30}, c: c}
	k := ds.NewKey("loadSaver", "", 12345, nil)
	_, err := ds.Put(k, &s)
	c.Assert(err, IsNil)

	s2 := loadSaverEntity{c: c}
	c.Assert(ds.Get(k, &s2), IsNil)

	c.Assert(s.s, Equals, s2.s)
	c.Assert(s.iList, DeepEquals, s2.iList)
}

func (dsit *AppengineInterfacesTest) TestKeyLoader(c *C) {
	ds := dsit.newDatastore()
	s := loadSaverEntity{s: "name", iList: []int64{10, 20, 30}, c: c}
	k := ds.NewKey("loadSaver", "", 12345, nil)
	_, err := ds.Put(k, &s)
	c.Assert(err, IsNil)

	s2 := keyLoadSaverEntity{loadSaverEntity: loadSaverEntity{c: c}}
	c.Assert(ds.Get(k, &s2), IsNil)

	c.Assert(s.s, Equals, s2.s)
	c.Assert(s.iList, DeepEquals, s2.iList)
	c.Assert(k, DeepEquals, s2.k)
}
