package appwrap

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) TestMemDsNewKey(c *C) {
	mem := NewLocalDatastore(false)
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
	mem := NewLocalDatastore(false)
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

func (dsit *AppengineInterfacesTest) TestFieldInjection(c *C) {
	mem := NewLocalDatastore(true)
	k, err := mem.Put(mem.NewKey("test", "", 0, nil), &customEntity{5})
	c.Assert(err, IsNil)

	var e customEntity
	c.Assert(mem.Get(k, &e), ErrorMatches, "unknown property _debug_added_field")
}

func (dsit *AppengineInterfacesTest) TestMemDsAllocateIds(c *C) {
	mem := NewLocalDatastore(false)

	first, last, err := mem.AllocateIDs("simple", nil, 10)
	c.Assert(err, IsNil)
	c.Assert(first > 10000, Equals, true)
	c.Assert(last-first+1, Equals, int64(10))

	first, last, err = mem.AllocateIDs("simple", nil, 5)
	c.Assert(err, IsNil)
	c.Assert(last-first+1, Equals, int64(5))
}

func (dsit *AppengineInterfacesTest) TestMemDsPutGetDeleteMulti(c *C) {
	mem := NewLocalDatastore(false)

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

	mem = NewLocalDatastore(false)
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
	mem := NewLocalDatastore(false)

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

	mem = NewLocalDatastore(false)
	_, err = mem.PutMulti(keys, itemPtrs)
	c.Assert(err, IsNil)

	gotItems = make([]customEntity, 5)
	c.Assert(mem.GetMulti(keys, gotItems), IsNil)
	for i := range keys {
		c.Assert(gotItems[i].i, Equals, items[i].i)
	}
}

func (dsit *AppengineInterfacesTest) TestMemDsQueryGetAll(c *C) {
	mem := NewLocalDatastore(false)
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
	mem := NewLocalDatastore(false)

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
	mem := NewLocalDatastore(false)

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
	mem := NewLocalDatastore(false)

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
	mem := NewLocalDatastore(false)

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

}

func (dsit *AppengineInterfacesTest) TestTransactionMultiThreaded(c *C) {
	mem := NewLocalDatastore(false)

	items := make([]customEntity, 5)
	keys := make([]*datastore.Key, 5)
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
			_ = mem.RunInTransaction(func(ds Datastore) error {
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
			wg.Done()
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

func (dsit *AppengineInterfacesTest) TestValueFilter(c *C) {
	mem := NewLocalDatastore(false)

	checkVals := func(base interface{}, valsEq []interface{}, valsLT []interface{}, valsGT []interface{}) bool {
		var f valueFilter
		pass := true
		for _, vEq := range valsEq {
			f = eqValueFilter{val: vEq}
			pass = c.Check(f.cmpValue(base), IsTrue) && pass
			f = ineqValueFilter{ops: []string{"<=", ">="}, threshs: []interface{}{vEq, vEq}}
			pass = c.Check(f.cmpValue(base), IsTrue) && pass
			f = ineqValueFilter{ops: []string{"<", ">"}, threshs: []interface{}{vEq, vEq}}
			pass = c.Check(f.cmpValue(base), IsFalse) && pass
		}
		for _, vLT := range valsLT {
			f = eqValueFilter{val: vLT}
			pass = c.Check(f.cmpValue(base), IsFalse) && pass
			f = ineqValueFilter{ops: []string{">=", ">"}, threshs: []interface{}{vLT, vLT}}
			pass = c.Check(f.cmpValue(base), IsTrue) && pass
			f = ineqValueFilter{ops: []string{"<=", "<"}, threshs: []interface{}{vLT, vLT}}
			pass = c.Check(f.cmpValue(base), IsFalse) && pass
		}
		for _, vGT := range valsGT {
			f = eqValueFilter{val: vGT}
			pass = c.Check(f.cmpValue(base), IsFalse) && pass
			f = ineqValueFilter{ops: []string{"<=", "<"}, threshs: []interface{}{vGT, vGT}}
			pass = c.Check(f.cmpValue(base), IsTrue) && pass
			f = ineqValueFilter{ops: []string{">=", ">"}, threshs: []interface{}{vGT, vGT}}
			pass = c.Check(f.cmpValue(base), IsFalse) && pass
		}
		return pass
	}

	c.Assert(checkVals(false, []interface{}{false}, []interface{}{}, []interface{}{true}), IsTrue)
	c.Assert(checkVals(true, []interface{}{true}, []interface{}{false}, []interface{}{}), IsTrue)

	c.Assert(checkVals(3, []interface{}{3}, []interface{}{-1, 0, 2}, []interface{}{4, 10067}), IsTrue)
	c.Assert(checkVals(-42, []interface{}{-42}, []interface{}{-104, -23123}, []interface{}{-3, 0, 4, 10067}), IsTrue)

	c.Assert(checkVals(int64(3), []interface{}{int64(3), int32(3), 3}, []interface{}{int64(-1), int64(0), int64(2), int32(-1), int32(0), int32(2), -1, 0, 2}, []interface{}{int64(4), int64(10067), int32(4), int32(10067), 4, 10067}), IsTrue)
	c.Assert(checkVals(int64(-42), []interface{}{int64(-42), int32(-42), -42}, []interface{}{int64(-104), int64(-23123), int32(-104), int32(-23123), -104, -23123}, []interface{}{int64(-3), int64(0), int64(4), int64(10067), int32(-3), int32(0), int32(4), int32(10067), -3, 0, 4, 10067}), IsTrue)

	c.Assert(checkVals("miners", []interface{}{"miners"}, []interface{}{"miner", "minered", "liners", "linters"}, []interface{}{"minerses", "niner", "minter"}), IsTrue)

	format := "2006-01-02 15:04:05 -0700"
	timeBase, _ := time.Parse(format, "2016-12-10 12:00:00 -0000")
	timeEq1, _ := time.Parse(format, "2016-12-10 12:00:00 -0000")
	timeEq2, _ := time.Parse(format, "2016-12-10 07:00:00 -0500")
	timeEq3, _ := time.Parse(format, "2016-12-10 17:00:00 +0500")
	timeLT1, _ := time.Parse(format, "2016-12-10 11:59:59 -0000")
	timeLT2, _ := time.Parse(format, "2016-12-10 06:59:59 -0500")
	timeLT3, _ := time.Parse(format, "2016-12-10 16:59:59 +0500")
	timeGT1, _ := time.Parse(format, "2016-12-10 12:00:01 -0000")
	timeGT2, _ := time.Parse(format, "2016-12-10 07:00:01 -0500")
	timeGT3, _ := time.Parse(format, "2016-12-10 17:00:01 +0500")
	c.Assert(checkVals(timeBase,
		[]interface{}{timeEq1, timeEq2, timeEq3},
		[]interface{}{timeLT1, timeLT2, timeLT3},
		[]interface{}{timeGT1, timeGT2, timeGT3},
	), IsTrue)

	c.Assert(checkVals((*datastore.Key)(nil),
		[]interface{}{
			(*datastore.Key)(nil),
		},
		[]interface{}{},
		[]interface{}{
			mem.NewKey("anyKind", "stringKey", 0, nil),
			mem.NewKey("anyKind", "", 237, nil),
			mem.NewKey("anyKind", "", 0, nil),
		},
	), IsTrue)
	c.Assert(checkVals(mem.NewKey("kindC", "stringKey5", 0, nil),
		[]interface{}{
			mem.NewKey("kindC", "stringKey5", 0, nil),
		},
		[]interface{}{
			mem.NewKey("kindC", "stringKey4", 0, nil),
			mem.NewKey("kindA", "stringKey6", 0, nil),
		},
		[]interface{}{
			mem.NewKey("kindC", "stringKey6", 0, nil),
			mem.NewKey("kindC", "stringKey5", 0, mem.NewKey("anyKind", "", 0, nil)),
			mem.NewKey("kindZ", "stringKey4", 0, nil),
		},
	), IsTrue)
	c.Assert(checkVals(mem.NewKey("kindC", "", 5, mem.NewKey("kindL", "", 5, mem.NewKey("kindP", "", 0, nil))),
		[]interface{}{
			mem.NewKey("kindC", "", 5, mem.NewKey("kindL", "", 5, mem.NewKey("kindP", "", 0, nil))),
		},
		[]interface{}{
			mem.NewKey("kindC", "", 5, nil),
			mem.NewKey("kindC", "", 4, nil),
			mem.NewKey("kindB", "", 5, mem.NewKey("kindL", "", 5, mem.NewKey("kindP", "", 0, nil))),
			mem.NewKey("kindC", "", 5, mem.NewKey("kindK", "", 5, mem.NewKey("kindZ", "", 0, nil))),
			mem.NewKey("kindC", "", 5, mem.NewKey("kindL", "", 5, mem.NewKey("kindO", "", 0, nil))),
			mem.NewKey("kindC", "", 5, mem.NewKey("kindL", "", 4, mem.NewKey("kindP", "", 0, nil))),
			mem.NewKey("kindC", "", 5, mem.NewKey("kindL", "", 5, nil)),
		},
		[]interface{}{
			mem.NewKey("kindC", "", 6, nil),
			mem.NewKey("kindC", "", 5, mem.NewKey("kindM", "", 0, nil)),
			mem.NewKey("kindC", "", 5, mem.NewKey("kindL", "", 6, nil)),
			mem.NewKey("kindD", "", 5, mem.NewKey("kindL", "", 5, mem.NewKey("kindP", "", 0, nil))),
			mem.NewKey("kindC", "", 5, mem.NewKey("kindL", "", 5, mem.NewKey("kindP", "", 0, mem.NewKey("anyKind", "", 0, nil)))),
		},
	), IsTrue)
}

func (dsit *AppengineInterfacesTest) TestFilter(c *C) {
	mem := NewLocalDatastore(false)

	v := &dsItem{
		key: mem.NewKey("kind", "theKey", 0, nil),
		props: datastore.PropertyList{
			{Name: "colSingle", Value: 123},
			{Name: "colMulti", Value: 100, Multiple: true},
			{Name: "colMulti", Value: 200, Multiple: true},
			{Name: "colMulti", Value: 300, Multiple: true},
		},
	}

	c.Assert(filter{eqs: []eqValueFilter{{val: 123}}}.cmp("colSingle", v), IsTrue)
	c.Assert(filter{eqs: []eqValueFilter{{val: 321}}}.cmp("colSingle", v), IsFalse)
	c.Assert(filter{eqs: []eqValueFilter{{val: 123}, {val: 321}}}.cmp("colSingle", v), IsFalse)
	c.Assert(filter{ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{100, 200}}}.cmp("colSingle", v), IsTrue)
	c.Assert(filter{ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{200, 300}}}.cmp("colSingle", v), IsFalse)

	c.Assert(filter{eqs: []eqValueFilter{{val: 100}}}.cmp("colMulti", v), IsTrue)
	c.Assert(filter{eqs: []eqValueFilter{{val: 123}}}.cmp("colMulti", v), IsFalse)
	c.Assert(filter{eqs: []eqValueFilter{{val: 100}, {val: 300}}}.cmp("colMulti", v), IsTrue)
	c.Assert(filter{eqs: []eqValueFilter{{val: 100}, {val: 400}}}.cmp("colMulti", v), IsFalse)
	c.Assert(filter{ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{100, 200}}}.cmp("colMulti", v), IsTrue)
	c.Assert(filter{ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{100, 400}}}.cmp("colMulti", v), IsTrue)
	c.Assert(filter{ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{500, 600}}}.cmp("colMulti", v), IsFalse)
	c.Assert(filter{ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{0, 100}}}.cmp("colMulti", v), IsFalse)
}

func (dsit *AppengineInterfacesTest) TestBuilderClobber(c *C) {
	q := &memoryQuery{}

	q1 := q.Filter("fieldA =", 111).Filter("fieldA =", 112).Filter("fieldB =", 222).Filter("fieldC >=", 333).Filter("fieldC <", 334).Filter("fieldD <", 444).
		Order("fieldA").Order("fieldB").Order("fieldC").Order("fieldD").(*memoryQuery)
	q2 := q.Filter("fieldA2 =", 444).Filter("fieldA2 =", 443).Filter("fieldB2 =", 333).Filter("fieldC2 >=", 222).Filter("fieldC2 <", 221).Filter("fieldD2 <", 111).
		Order("fieldA2").Order("fieldB2").Order("fieldC2").Order("fieldD2").(*memoryQuery)

	c.Assert(q1.filters, DeepEquals, map[string]filter{
		"fieldA": {eqs: []eqValueFilter{{val: 111}, {val: 112}}},
		"fieldB": {eqs: []eqValueFilter{{val: 222}}},
		"fieldC": {ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{333, 334}}},
		"fieldD": {ineq: ineqValueFilter{ops: []string{"<"}, threshs: []interface{}{444}}},
	})
	c.Assert(q1.order, DeepEquals, []string{"fieldA", "fieldB", "fieldC", "fieldD"})

	c.Assert(q2.filters, DeepEquals, map[string]filter{
		"fieldA2": {eqs: []eqValueFilter{{val: 444}, {val: 443}}},
		"fieldB2": {eqs: []eqValueFilter{{val: 333}}},
		"fieldC2": {ineq: ineqValueFilter{ops: []string{">=", "<"}, threshs: []interface{}{222, 221}}},
		"fieldD2": {ineq: ineqValueFilter{ops: []string{"<"}, threshs: []interface{}{111}}},
	})
	c.Assert(q2.order, DeepEquals, []string{"fieldA2", "fieldB2", "fieldC2", "fieldD2"})
}

func (dsit *AppengineInterfacesTest) TestKinds(c *C) {
	entity := struct{ foo string }{"hello"}

	mem := NewLocalDatastore(false)
	k1 := mem.NewKey("kind1", "", 1, nil)
	k2 := mem.NewKey("kind2", "", 2, nil)

	mem.Put(k1, &entity)
	mem.Put(k2, &entity)

	kinds, err := mem.Kinds()
	c.Assert(err, IsNil)
	sort.Sort(sort.StringSlice(kinds))
	c.Assert(kinds, DeepEquals, []string{"kind1", "kind2"})
}

func (dsit *AppengineInterfacesTest) TestDistinct(c *C) {
	mem := NewLocalDatastore(false)

	type thing struct {
		A, B string
		C    int
	}

	var (
		keys   []*datastore.Key
		things []thing
	)
	for i := 1; i <= 10; i++ {
		keys = append(keys, mem.NewKey("thing", "", int64(i), nil))
		things = append(things, thing{A: fmt.Sprintf("%v", i), B: "blah", C: 10 - i})
	}
	_, err := mem.PutMulti(keys, things)
	c.Assert(err, IsNil)

	// Test: no projection, so it panics
	c.Assert(func() { mem.NewQuery("thing").Distinct() }, Panics, "Distinct is only allowed with Projection Queries")

	// Test: projection, no real distinct items, should return all ten things
	var results []thing
	q1 := mem.NewQuery("thing").Project("A").Distinct()
	_, err = q1.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 10)

	// Test: projection on b, which is always the same; should return one item
	q2 := mem.NewQuery("thing").Project("B").Distinct()
	_, err = q2.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 1)

	// Test: projection on b, c, returns all items
	q3 := mem.NewQuery("thing").Project("B", "C").Distinct()
	_, err = q3.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 10)

	// Test: Modify one item and make a duplicate, now should have two distinct items for b
	things[4].B = "blat"
	_, err = mem.Put(keys[4], &things[4])
	c.Assert(err, IsNil)
	q4 := mem.NewQuery("thing").Project("B").Distinct()
	_, err = q4.GetAll(&results)
	c.Assert(err, IsNil)
	c.Assert(results, HasLen, 2)
}
