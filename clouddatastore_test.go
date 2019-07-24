// +build clouddatastore

package appwrap

import (
	"fmt"

	"cloud.google.com/go/datastore"
	. "gopkg.in/check.v1"
)

var _ = fmt.Printf

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

func (s *AppengineInterfacesTest) TestSetKeyNamespace(c *C) {

	ck := func(k *datastore.Key, expectedNs string) {
		for thisKey := k; thisKey != nil; thisKey = thisKey.Parent {
			c.Assert(thisKey.Namespace, Equals, expectedNs)
		}
	}

	mem := NewLocalDatastore(true, nil)

	keyWithNoParent := mem.NewKey("Test", "NumberOne", 0, nil)
	ck(SetKeyNamespace(keyWithNoParent, "NewNamespace"), "NewNamespace")

	keyWithOneParent := mem.NewKey("Test", "NumberTwo", 0, mem.NewKey("Test", "NumberTwoParent", 0, nil))
	ck(SetKeyNamespace(keyWithOneParent, "NewNamespace"), "NewNamespace")

	keyWithTwoParents := mem.NewKey("Test", "NumberThree", 0, mem.NewKey("Test", "NumberThreeParent", 0, mem.NewKey("Test", "NumberThreeGrandparent", 0, nil)))
	ck(SetKeyNamespace(keyWithTwoParents, "NewNamespace"), "NewNamespace")
}
