// +build clouddatastore

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

// tests we can decode both old and new cloud datastore style encoding
func (s *AppengineInterfacesTest) TestDecodeCompatibility(c *C) {
	newEncoding := "CgkiB3NfbWVtZHMSFAoKcGFyZW50S2luZBoGc29tZUlkEg0KCWNoaWxkS2luZBAX"
	oldEncoding := "agdzX21lbWRzciULEgpwYXJlbnRLaW5kIgZzb21lSWQMCxIJY2hpbGRLaW5kGBcMogEHc19tZW1kcw"

	ds := s.newDatastore().Namespace("s_memds")
	parentKey := ds.NewKey("parentKind", "someId", 0, nil)
	key := ds.NewKey("childKind", "", 23, parentKey)

	oldKey, err := DecodeKey(oldEncoding)
	c.Assert(err, IsNil)
	c.Assert(oldKey.Equal(key), IsTrue)

	newKey, err := DecodeKey(newEncoding)
	c.Assert(err, IsNil)
	c.Assert(newKey.Equal(key), IsTrue)
}
