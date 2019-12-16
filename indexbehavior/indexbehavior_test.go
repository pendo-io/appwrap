// +build indexbehavior

// Package indexbehavior is meant to test our understanding of Google Datastore (well, Cloud Firestore in Datastore Mode) indexes.
// We want to answer two questions:
//   a) What queries require an index?
//   b) What indexes can satisfy those queries?
//
// The most helpful pages of documentation in this endeavor are:
//   https://cloud.google.com/datastore/docs/concepts/indexes
//   https://cloud.google.com/datastore/docs/concepts/queries
// Optionally:
//   https://web.archive.org/web/20181026092431/https://cloud.google.com/appengine/articles/indexselection
//   https://www.youtube.com/watch?v=ofhEyDBpngM
//   https://www.youtube.com/watch?v=d4CiMWy0J70
//
// One of the goals of this package is easy experimentation - we want to, as much as possible, make it easy to test our assumptions.
//   Something we believed for a while, but cannot find either documentation or experimental evidence to support, was that subindexes
//   existed, in the sense that Index(A, B, C) would satisfy Query(A, B).
// This package is also intended to be a source of truth for appwrap/localdatastore.go::checkIndexes. That function is designed to
//   confirm that every query exercised via tests has a satisfactory index in our index.yaml. It is important that this function closely
//   follows Google's actual behavior. If it does not, our developers will be misinformed about the actual requirements, and we lose money
//   from increased development time or unnecessary indexes. As another example, a prior version of that function disallowed multiple sort
//   orders, saying they are not supported. As our main code co-evolves with the test code, there are no instances of multiple sort orders
//   present there. We want to, if possible, prevent frustration and make it easy to test.
//
// The reader is strongly encouraged to add to this file as they see fit.
// Development:
//   Indexes used for testing should be placed in the adjacent index.yaml. This should be kept to a minimum - re-use indexes when possible.
//   When creating a new index, take care to ensure it cannot be easily confused with an existing one. I.E. if there is Index(A, B, C),
//     avoid adding Index(A, B, -C). (Instead, opt for Index(G, H, J) or similar). This is because we cannot isolate indexes per-test, and
//     we want to minimize what must be kept in working memory to reason about a query.
//   When adding a test, follow the existing pattern. If you add a new test here (to lock down our understanding), it is strongly encouraged
//     that you add a similar test to appwrap/localdatastore_test.go. If that would cause tests to fail, please  open an issue and
//     mark the test in this file with a comment, but if you're feeling up to it, by all means try to amend checkIndexes.
//   Testing: use `gcloud datastore create index.yaml --project=$TestProject` (TestProject being the const below).
//     Run `go test ./... -tags indexbehavior`.
package indexbehavior

import (
	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"testing"

	. "gopkg.in/check.v1"
)

// This is the project that we'll be running our test queries against.
const TestProject = "pendo-dev"

func Test(t *testing.T) { TestingT(t) }

type IndexBehaviorTest struct{}

var _ = Suite(&IndexBehaviorTest{})

type testKind struct{} // Since there aren't any real entities, we don't have to allocate fields for them

var testKey = datastore.IDKey("testKindWithIndex", 1, nil) // Dummy key for abbreviative reasons

// These are the queries that don't require an index.
func (ibt *IndexBehaviorTest) TestCheckIndexNoIndexNeeded(c *C) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, TestProject)
	if err != nil {
		panic(err)
	}
	noIndex := func(q *datastore.Query) {
		t := dsClient.Run(ctx, q)
		var x testKind
		_, err := t.Next(&x)
		if err != iterator.Done {
			c.Assert(err, IsNil)
		}
	}

	// Kindless queries using only ancestor and key filters
	// (these are scary though, so we don't use them)
	noIndex(datastore.NewQuery("").Filter("__key__ >", testKey).Ancestor(testKey))

	// Queries using only ancestor and equality filters
	noIndex(datastore.NewQuery("testKindNoIndex"))
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField =", 0))
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField =", 0).Ancestor(testKey))
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField =", 0).Filter("otherField =", 1).Ancestor(testKey))

	// Queries using only inequality filters (which are limited to a single property)
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField >", 0))
	// Even if there's two on the same field.
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField >", 0).Filter("singleField <", 200))

	// Queries using only ancestor filters, equality filters on properties, and inequality filters on keys
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField =", 0).Filter("__key__ >", testKey).Ancestor(testKey))

	// Queries with no filters and only one sort order on a property, either ascending or descending
	// (unless descending key)
	noIndex(datastore.NewQuery("testKindNoIndex").Order("singleField"))
	noIndex(datastore.NewQuery("testKindNoIndex").Order("-singleField"))

	// Also with a filter on the ordered property (undocumented)
	// (Ordering of query results is undefined when no sort order is specified)
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField >", 0).Order("singleField"))
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField >", 0).Filter("singleField <", 0).Order("-singleField"))

	// If a query does not need an index, making it keys-only does not make you need one.
	noIndex(datastore.NewQuery("testKindNoIndex").KeysOnly())
	noIndex(datastore.NewQuery("testKindNoIndex").Filter("singleField >", 0).KeysOnly())

	// Single project + Order
	noIndex(datastore.NewQuery("testKindNoIndex").Project("A").Order("-A"))
}

func (ibt *IndexBehaviorTest) TestCheckIndexNeedIndexFor(c *C) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, TestProject)
	if err != nil {
		panic(err)
	}

	indexSatisfies := func(query func(string) *datastore.Query, satisfies bool) {
		q := query("testKindNoIndex")
		t := dsClient.Run(ctx, q)
		var x testKind
		_, err := t.Next(&x)
		if err != iterator.Done {
			c.Assert(err, NotNil)
		}

		q = query("testKindWithIndex")
		t = dsClient.Run(ctx, q)
		_, err = t.Next(&x)
		if satisfies {
			c.Assert(err, Equals, iterator.Done)
		} else {
			c.Assert(err, NotNil)
			c.Assert(err, Not(Equals), iterator.Done)
		}

	}

	// Queries with ancestor and inequality filters
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("D >", 0).Ancestor(testKey)
	}, true)

	// If you've got an ancestor index...
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("D =", 0).Filter("C =", 0).Filter("A >", 0).Ancestor(testKey)
	}, true)

	// ...it won't satisfy non-ancestor queries.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("D =", 0).Filter("C =", 0).Filter("A >", 0)
	}, false)

	// Queries with one or more inequality filters on a property and one or more equality filters on other properties
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B =", 0).Filter("C >", 0)
	}, true)

	// No index on D
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B =", 0).Filter("D >", 0)
	}, false)

	// Sub-indexes do not work as prefix
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B >", 0)
	}, false)

	// ...or when skipping
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("C >", 0)
	}, false)

	// ...or postfix. Or at all.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("B =", 0).Filter("C >", 0)
	}, false)

	// Declared order of filters does not matter
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("C >", 0).Filter("B =", 0).Filter("A =", 0)
	}, true)

	// ...but, inequality must be on last indexed field
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("C =", 0).Filter("B >", 0)
	}, false)

	// ...unless there's an order on a later field
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B >", 0).Order("-B").Order("C")
	}, true)

	// ...but not if there's not an order on the inequality field.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B >", 0).Order("C")
	}, false)

	// Queries with a sort order on keys in descending order
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Order("-__key__")
	}, true)

	// Queries with multiple sort orders
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Order("A").Order("-B").Order("C")
	}, true)

	// Queries with one or more filters and one or more sort orders (on a different field)
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B =", 0).Order("C")
	}, true)

	// Yes you can have an inequality and sort order on the same property! This can even be
	// useful for reasons of e.g. re-using an index.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B <", 0).Order("-B").Order("C")
	}, true)

	// If the sort descends, so must the index.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Order("B").Order("C")
	}, false)

	// Queries whose earlier properties ( (In)Equality filters ) are the same as a later property,
	// consistnt with the index. If you have a valid order-only query, prefixing it with an (in)equality
	// filter on the first indexed property does not invalidate it.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Order("A").Order("-B").Order("C")
	}, true)

	// ...but not if you use a later property.
	// This query parses as [Filter(B) Order(A, C)] as sort order on quality filter is ignored.
	// And we don't have an index for that.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("B =", 0).Order("A").Order("-B").Order("C")
	}, false)

	// (Properties used in inequality filters must be sorted first)
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("B >", 0).Order("A").Order("-B").Order("C")
	}, false)

	// (sort orders are ignored on properties with equality filters)
	// (inequality filter and first sort order must be the same)
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A >", 0).Order("-B").Order("C")
	}, false)

	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A >", 0).Order("A").Order("-B").Order("C")
	}, true)

	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B =", 0).Order("A").Order("-B").Order("C")
	}, true)

	// All properties in projection queries must be specified
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B =", 0).Filter("C >", 0).Project("C")
	}, true)

	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B =", 0).Filter("C >", 0).Project("D")
	}, false)

	// If a property is projected on, it doesn't need to be filtered on.
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B >", 0).Project("B", "C")
	}, true)

	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Project("B", "C")
	}, true)

	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Project("A", "B", "C")
	}, true)

	// This is a simplified query we actually use
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("A =", 0).Filter("B =", 0).Project("C").Order("C")
	}, true)

	// Zigzag merge join algorithm
	// https://web.archive.org/web/20181026092431/https://cloud.google.com/appengine/articles/indexselection
	// This page is no longer linked to, and "zigzag merge join algorithm site:cloud.google.com" returns 0 results
	// It was introduced at Google I/O in 2010. It is still valid for queries.
	// Summary thusly: Index(A, C) && Index(B, C) satisfies Query(A, B, C)
	indexSatisfies(func(kind string) *datastore.Query {
		return datastore.NewQuery(kind).Filter("zigzag =", 0).Filter("join =", 0).Order("merge")
	}, true)
}
