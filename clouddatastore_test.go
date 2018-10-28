// +build !appengine, clouddatastore
// +build !appenginevm, clouddatastore

package appwrap

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	. "gopkg.in/check.v1"
)

var _ = fmt.Printf

func (s *AppengineInterfacesTest) newDatastore() Datastore {
	cds, err := NewCloudDatastore(context.Background())
	if err != nil {
		panic(err)
	}

	return cds.Namespace(fmt.Sprintf("%s_%d", time.Now().Format("Jan__2_15_04_05_000000000"), rand.Uint64()))
}

func (s *AppengineInterfacesTest) TestToAppwrapPropertyList(c *C) {
	dList := []DatastoreProperty{
		{Name: "str", Value: "hello"},
		{Name: "int", Value: int64(17), NoIndex: true},
		{Name: "list", Value: []interface{}{"one", "two"}},
	}

	aList := ToAppwrapPropertyList(dList)
	c.Assert(aList, DeepEquals, []AppwrapProperty{
		{Name: "str", Value: "hello"},
		{Name: "int", Value: int64(17), NoIndex: true},
		{Name: "list", Value: "one", Multiple: true},
		{Name: "list", Value: "two", Multiple: true},
	})
}

func (s *AppengineInterfacesTest) TestToDatastorePropertyList(c *C) {
	aList := []AppwrapProperty{
		{Name: "strList", Multiple: true, Value: "first"},
		{Name: "boolList", Multiple: true, Value: true, NoIndex: true},
		{Name: "strList", Multiple: true, Value: "second"},
		{Name: "strList", Multiple: true, Value: "third"},
		{Name: "int", Value: int64(17)},
		{Name: "boolList", Multiple: true, Value: false, NoIndex: true},
		{Name: "strList", Multiple: true, Value: "fourth"},
	}

	dList := ToDatastorePropertyList(aList)
	c.Assert(dList, DeepEquals, []DatastoreProperty{
		{Name: "strList", Value: []interface{}{"first", "second", "third", "fourth"}},
		{Name: "boolList", Value: []interface{}{true, false}, NoIndex: true},
		{Name: "int", Value: int64(17)},
	})
}
