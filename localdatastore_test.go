package appwrap

import (
	"sort"
	"time"

	. "gopkg.in/check.v1"
)

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
		props: []AppwrapProperty{
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

// These are the queries that don't require an index
func (dsit *AppengineInterfacesTest) TestCheckIndexNoIndexNeeded(c *C) {
	mem := NewLocalDatastore(false, DatastoreIndex{})
	testKey := mem.NewKey("foo", "", 1, nil)
	noIndex := func(q DatastoreQuery) {
		c.Assert(q.(*memoryQuery).checkIndexes(false), IsNil)
	}

	// Kindless queries using only ancestor and key filters
	// (these are scary though, so we don't use them)
	noIndex(mem.NewQuery("").Filter("__key__ >", testKey).Ancestor(testKey))

	// Queries using only ancestor and equality filters
	noIndex(mem.NewQuery("testKindNoIndex"))
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField =", 0))
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField =", 0).Ancestor(testKey))
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField =", 0).Filter("otherField =", 1).Ancestor(testKey))

	// Queries using only inequality filters (which are limited to a single property)
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField >", 0))
	// Even if there's two on the same field.
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField >", 0).Filter("singleField <", 200))

	// Queries using only ancestor filters, equality filters on properties, and inequality filters on keys
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField =", 0).Filter("__key__ >", testKey).Ancestor(testKey))

	// Queries with no filters and only one sort order on a property, either ascending or descending
	// (unless descending key)
	noIndex(mem.NewQuery("testKindNoIndex").Order("singleField"))
	noIndex(mem.NewQuery("testKindNoIndex").Order("-singleField"))

	// Also with a filter on the ordered property (undocumented)
	// (Ordering of query results is undefined when no sort order is specified)
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField >", 0).Order("singleField"))
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField >", 0).Filter("singleField <", 0).Order("-singleField"))

	// If a query does not need an index, making it keys-only does not make you need one.
	noIndex(mem.NewQuery("testKindNoIndex").KeysOnly())
	noIndex(mem.NewQuery("testKindNoIndex").Filter("singleField >", 0).KeysOnly())

	// Projecting once and ordering once is AOK
	noIndex(mem.NewQuery("testKindNoIndex").Project("A").Order("-A"))
}

func (dsit *AppengineInterfacesTest) TestCheckIndexNeedIndexFor(c *C) {
	mem := NewLocalDatastore(false, DatastoreIndex{})
	testKey := mem.NewKey("foo", "", 1, nil)
	indexSatisfies := func(query func(Datastore) DatastoreQuery, index string, satisfies bool) {
		// If queried without index, error about a missing index
		c.Check(query(mem).(*memoryQuery).checkIndexes(false), ErrorMatches, "(?s:missing index.*)")

		// Create a datastore with only the provided index
		loadedIndex, err := LoadIndexYaml([]byte("indexes:\n" + index))
		c.Assert(err, IsNil)
		indexedDs := NewLocalDatastore(false, loadedIndex)
		if satisfies {
			// If the provided index is supposed to satisfy, verify no error
			c.Assert(query(indexedDs).(*memoryQuery).checkIndexes(false), IsNil)
		} else {
			// Otherwise, verify index is still required.
			c.Assert(query(indexedDs).(*memoryQuery).checkIndexes(false), ErrorMatches, "(?s:missing index.*)")
		}
	}

	iAbC := `
- kind: testKindWithIndex
  properties:
     - name: A
     - name: B
       direction: desc
     - name: C`

	iDCA := `
- kind: testKindWithIndex
  ancestor: yes
  properties:
     - name: D
     - name: C
     - name: A`

	iDAncestor := `
- kind: testKindWithIndex
  ancestor: yes
  properties:
    - name: D`

	iDAncestorOrder := `
- kind: testKindWithIndex
  ancestor: yes
  properties:
    - name: D
      direction: desc
`

	iKeyDesc := `
- kind: testKindWithIndex
  properties:
    - name: __key__
      direction: desc`

	iZigzag := `
- kind: testKindWithIndex
  properties:
    - name: zigzag
    - name: merge

- kind: testKindWithIndex
  properties:
    - name: join
    - name: merge
`

	const (
		satisfies               = true  // works in datastore and passes checkIndexes
		doesNotSatisfy          = false // does not work in datastore and fails checkIndexes
		unsupportedButSatisfies = false // works on datastore but we don't support it in appwrap
	)

	// Queries with ancestor and inequality filters
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("D >", 0).Ancestor(testKey)
	}, iDAncestor, satisfies)

	// If you've got an ancestor index...
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("D =", 0).Filter("C =", 0).Filter("A >", 0).Ancestor(testKey)
	}, iDCA, satisfies)

	// ...it won't satisfy non-ancestor queries.
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("D =", 0).Filter("C =", 0).Filter("A >", 0)
	}, iDCA, doesNotSatisfy)

	// Queries with one or more inequality filters on a property and one or more equality filters on other properties
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B =", 0).Filter("C >", 0)
	}, iAbC, satisfies)

	// No index on D
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B =", 0).Filter("D >", 0)
	}, iAbC, doesNotSatisfy)

	// Sub-indexes do not work as prefix
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B >", 0)
	}, iAbC, doesNotSatisfy)

	// ...or when skipping
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("C >", 0)
	}, iAbC, doesNotSatisfy)

	// ...or postfix. Or at all.
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("B =", 0).Filter("C >", 0)
	}, iAbC, doesNotSatisfy)

	// Declared order of filters does not matter
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("C >", 0).Filter("B =", 0).Filter("A =", 0)
	}, iAbC, satisfies)

	// ...but, inequality must be on last indexed field
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("C =", 0).Filter("B >", 0)
	}, iAbC, doesNotSatisfy)

	// ...unless there's an order on a later field
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B >", 0).Order("-B").Order("C")
	}, iAbC, satisfies)

	// Queries with a sort order on keys in descending order
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Order("-__key__")
	}, iKeyDesc, satisfies)

	// Queries with multiple sort orders
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Order("A").Order("-B").Order("C")
	}, iAbC, satisfies)

	// Queries with one or more filters and one or more sort orders (on a different field)
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B =", 0).Order("C")
	}, iAbC, satisfies)

	// Yes you can have an inequality and sort order on the same property! This can even be
	// useful for reasons of e.g. re-using an index.
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B <", 0).Order("-B").Order("C")
	}, iAbC, satisfies)

	// If the sort descends, so must the index.
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Order("B").Order("C")
	}, iAbC, doesNotSatisfy)

	// Queries whose earlier properties ( (In)Equality filters ) are the same as a later property,
	// consistnt with the index. If you have a valid order-only query, prefixing it with an (in)equality
	// filter on the first indexed property does not invalidate it.
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Order("A").Order("-B").Order("C")
	}, iAbC, satisfies)

	// ...but not if you use a later property.
	// This query parses as [Filter(B) Order(A, C)] as sort order on quality filter is ignored.
	// And we don't have an index for that.
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("B =", 0).Order("A").Order("-B").Order("C")
	}, iAbC, doesNotSatisfy)

	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A >", 0).Order("A").Order("-B").Order("C")
	}, iAbC, satisfies)

	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B =", 0).Order("A").Order("-B").Order("C")
	}, iAbC, satisfies)

	// All properties in projection queries must be specified
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B =", 0).Filter("C >", 0).Project("C")
	}, iAbC, satisfies)

	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B =", 0).Filter("C >", 0).Project("D")
	}, iAbC, doesNotSatisfy)

	// If a property is projected on, it doesn't need to be filtered on.
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B >", 0).Project("B", "C")
	}, iAbC, satisfies)

	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Project("B", "C")
	}, iAbC, satisfies)

	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Project("A", "B", "C")
	}, iAbC, satisfies)

	// This is a simplified query we actually use
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("A =", 0).Filter("B =", 0).Project("C").Order("C")
	}, iAbC, satisfies)

	// Zigzag merge join algorithm
	// https://web.archive.org/web/20181026092431/https://cloud.google.com/appengine/articles/indexselection
	// This page is no longer linked to, and "zigzag merge join algorithm site:cloud.google.com" returns 0 results
	// It was introduced at Google I/O in 2010. It is still valid for queries.
	// Summary thusly: Index(A, C) && Index(B, C) satisfies Query(A, B, C)
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Filter("zigzag =", 0).Filter("join =", 0).Order("merge")
	}, iZigzag, unsupportedButSatisfies)

	// ancestor queries with a single order filter require an index
	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Ancestor(testKey).Order("-D")
	}, iDAncestorOrder, satisfies)

	indexSatisfies(func(m Datastore) DatastoreQuery {
		return m.NewQuery("testKindWithIndex").Ancestor(testKey).Order("-D")
	}, iDAncestor, doesNotSatisfy)
}

func (dsit *AppengineInterfacesTest) TestMemDsAllocateIds(c *C) {
	rawDs := dsit.newDatastore()
	ds := rawDs.(LegacyDatastore)

	first, last, err := ds.AllocateIDs("simple", nil, 10)
	c.Assert(err, IsNil)
	c.Assert(first > 10000, Equals, true)
	c.Assert(last-first, Equals, int64(10))

	first, last, err = ds.AllocateIDs("simple", nil, 5)
	c.Assert(err, IsNil)
	c.Assert(last-first, Equals, int64(5))
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

func (dsit *AppengineInterfacesTest) TestPanicOnDatastoreEntitySave(c *C) {
	mem := dsit.newDatastore().(*LocalDatastore)

	v := &dsItem{
		key: mem.NewKey("kind", "theKey", 0, nil),
		props: []AppwrapProperty{
			{Name: "shouldPanic", Value: &DatastoreEntity{}},
		},
	}
	c.Assert(func() {
		mem.put("key", v)
	}, PanicMatches, "cannot save non-flattened structs.*")
}

func (dsit *AppengineInterfacesTest) TestProjectionQuery(c *C) {
	type e struct {
		Foo string
		Bar string
	}

	entity := e{"hello", "banana"}

	ds := dsit.newDatastore()
	k1 := ds.NewKey("entity", "", 1, nil)

	ds.Put(k1, &entity)

	q := ds.NewQuery("entity").Project("Foo")

	iter := q.Run()
	res := e{}
	_, err := iter.Next(&res)
	c.Assert(err, IsNil)

	c.Assert(res.Foo, Equals, "hello")
	c.Assert(res.Bar, Equals, "")
}

func (dsit *AppengineInterfacesTest) TestGetWithIncompleteKeyShouldFail(c *C) {
	ds := dsit.newDatastore()

	k := ds.NewKey("entity", "", 1, nil)
	c.Assert(k.Incomplete(), IsFalse)

	incKey := ds.NewKey("entity", "", 0, nil)
	c.Assert(incKey.Incomplete(), IsTrue)

	type whatever struct {
		however string
	}
	var w whatever
	c.Assert(ds.Get(incKey, &w), Equals, ErrInvalidKey)

	wList := make([]whatever, 2)
	err := ds.GetMulti([]*DatastoreKey{k, incKey}, wList)
	c.Assert(err, NotNil)
	c.Assert(err, FitsTypeOf, MultiError{})
	me := err.(MultiError)
	c.Assert(me, HasLen, 2)
	c.Assert(me[0], Equals, ErrNoSuchEntity)
	c.Assert(me[1], Equals, ErrInvalidKey)
}

func (dsit *AppengineInterfacesTest) TestLocalDatastoreNamespacing(c *C) {
	rootDs := dsit.newDatastore()

	parentKey := rootDs.NewKey("sootsprite", "", 1, nil)
	c.Assert(KeyNamespace(parentKey), Equals, "s~memds")

	childKey := rootDs.NewKey("sootsprite", "", 2, parentKey)
	c.Assert(KeyNamespace(childKey), Equals, "s~memds")

	nsDs := rootDs.Namespace("chihiro")
	nsParentKey := nsDs.NewKey("otherthing", "", 3, nil)
	c.Assert(KeyNamespace(nsParentKey), Equals, "chihiro")

	nsChildKey := nsDs.NewKey("otherthing", "", 4, nsParentKey)
	c.Assert(KeyNamespace(nsChildKey), Equals, "chihiro")

	// ensure that we use the parent's key's namespace if we mix the two
	crossNamespaceKey := nsDs.NewKey("otherthing", "", 5, parentKey)
	c.Assert(KeyNamespace(crossNamespaceKey), Equals, "s~memds")
}

func (dsit *AppengineInterfacesTest) TestLocalDatastoreNamespaceInTransaction(c *C) {
	rootDs := dsit.newDatastore()
	nsDs := rootDs.Namespace("other_place")

	type e struct {
		Foo string
		Bar string
	}

	entity := e{"hello", "banana"}
	k, err := nsDs.Put(nsDs.NewKey("spoon", "too_big", 0, nil), &entity)
	c.Assert(KeyNamespace(k), Equals, "other_place")
	c.Assert(err, IsNil)

	_, err = nsDs.RunInTransaction(func(txn DatastoreTransaction) error {
		var txnEntity e
		txnK := txn.NewKey("spoon", "too_big", 0, nil)
		c.Assert(KeyNamespace(txnK), Equals, "other_place")
		if getErr := txn.Get(txnK, &txnEntity); getErr != nil {
			return getErr
		}
		txnEntity.Bar = "potato"
		_, putErr := txn.Put(k, &txnEntity)
		return putErr
	}, nil)
	c.Assert(err, IsNil)

	var updatedEntity e
	getErr := nsDs.Get(k, &updatedEntity)
	c.Assert(getErr, IsNil)
}
