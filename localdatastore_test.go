// +build appengine, appenginevm
// +build !clouddatastore

package appwrap

import (
	"sort"
	"time"

	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) newDatastore() Datastore {
	return NewLocalDatastore(false, nil)
}

func (dsit *AppengineInterfacesTest) TestFieldInjection(c *C) {
	mem := NewLocalDatastore(true, nil)
	k, err := mem.Put(mem.NewKey("test", "", 0, nil), &customEntity{5})
	c.Assert(err, IsNil)

	var e customEntity
	c.Assert(mem.Get(k, &e), ErrorMatches, "unknown property _debug_added_field")
}

func (dsit *AppengineInterfacesTest) TestValueFilter(c *C) {
	mem := dsit.newDatastore()

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

	c.Assert(checkVals((*DatastoreKey)(nil),
		[]interface{}{
			(*DatastoreKey)(nil),
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
	mem := dsit.newDatastore()

	v := &dsItem{
		key: mem.NewKey("kind", "theKey", 0, nil),
		props: DatastorePropertyList{
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

const checkIndexIndex = `
indexes:

- kind: entityKind
  properties:
     - name: A
     - name: B
       direction: desc
     - name: C

- kind: entityKind
  ancestor: yes
  properties:
     - name: E
     - name: C
     - name: A
`

func (dsit *AppengineInterfacesTest) TestCheckIndex(c *C) {
	// we can query a full entity w/o an index
	mem := NewLocalDatastore(false, DatastoreIndex{})
	key := mem.NewKey("someKey", "", 10, nil)
	q := mem.NewQuery("someKind")
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	// a single field is fine
	q = mem.NewQuery("someKind").Filter("singleField =", 0)
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	// even if it's an ancestor
	q = mem.NewQuery("someKind").Filter("singleField =", 0).Ancestor(mem.NewKey("foo", "", 1, nil))
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	// even if it's ordered
	q = mem.NewQuery("someKind").Filter("singleField =", 0).Order("singleField")
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)
	q = mem.NewQuery("someKind").Filter("singleField =", 0).Order("-singleField")
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	// but if it's ordered by a different field we're out of luck
	q = mem.NewQuery("someKind").Filter("singleField =", 0).Order("otherField")
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)

	index, err := LoadIndex([]byte(checkIndexIndex))
	c.Assert(err, IsNil)
	mem = NewLocalDatastore(false, index)

	q = mem.NewQuery("entityKind").Filter("A =", 123).Filter("B =", 456) // works
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	q = mem.NewQuery("entityKind").Filter("B =", 123).Filter("A =", 456) // order doesn't matter
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	q = mem.NewQuery("entityKind").Filter("A =", 123).Filter("D =", 456) // no index on D
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)

	q = mem.NewQuery("entityKind").Filter("E =", 123).Filter("C =", 456) // works
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	q = mem.NewQuery("entityKind").Filter("B =", 123).Filter("C =", 456) // we can't skip A
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)

	q = mem.NewQuery("entityKind").Filter("A =", 123).Filter("C =", 456) // we can't skip B
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)

	q = mem.NewQuery("entityKind").Filter("C =", 123).Filter("E =", 456).Order("A") // order matches
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	q = mem.NewQuery("entityKind").Filter("C =", 123).Filter("E =", 456).Order("-A") // order mismatch
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)

	q = mem.NewQuery("entityKind").Filter("E =", 123).Filter("C =", 456).Order("E") // can't sort by earlier item
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)

	q = mem.NewQuery("entityKind").Filter("A =", 123).Filter("B =", 456).Order("-B") // an intermediate one works though
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	q = mem.NewQuery("entityKind").Filter("A =", 123).Filter("B <", 456) // inequality is trailing
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	q = mem.NewQuery("entityKind").Filter("A <", 123).Filter("B =", 456) // inequality isn't trailing
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)

	q = mem.NewQuery("entityKind").Filter("C =", 123).Filter("E =", 456).Ancestor(key) // this has an ancestor index
	c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)

	q = mem.NewQuery("entityKind").Filter("A =", 123).Filter("B =", 456).Ancestor(key) // this doesn't have an ancestor
	c.Assert(q.(*memoryQuery).checkIndexes(false), NotNil)
}

func (dsit *AppengineInterfacesTest) TestMemDsAllocateIds(c *C) {
	rawDs := dsit.newDatastore()
	ds := rawDs.(LegacyDatastore)

	first, last, err := ds.AllocateIDs("simple", nil, 10)
	c.Assert(err, IsNil)
	c.Assert(first > 10000, Equals, true)
	c.Assert(last-first+1, Equals, int64(10))

	first, last, err = ds.AllocateIDs("simple", nil, 5)
	c.Assert(err, IsNil)
	c.Assert(last-first+1, Equals, int64(5))
}

func (dsit *AppengineInterfacesTest) TestKinds(c *C) {
	entity := struct{ foo string }{"hello"}

	ds := dsit.newDatastore()
	k1 := ds.NewKey("kind1", "", 1, nil)
	k2 := ds.NewKey("kind2", "", 2, nil)

	ds.Put(k1, &entity)
	ds.Put(k2, &entity)

	legacy := ds.(LegacyDatastore)
	kinds, err := legacy.Kinds()
	c.Assert(err, IsNil)
	sort.Sort(sort.StringSlice(kinds))
	c.Assert(kinds, DeepEquals, []string{"kind1", "kind2"})
}
