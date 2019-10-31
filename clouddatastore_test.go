// +build clouddatastore

package appwrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	newKey := SetKeyNamespace(keyWithNoParent, "NewNamespace")
	c.Assert(newKey, NotNil)
	ck(newKey, "NewNamespace")

	keyWithOneParent := mem.NewKey("Test", "NumberTwo", 0, mem.NewKey("Test", "NumberTwoParent", 0, nil))
	newKey = SetKeyNamespace(keyWithOneParent, "NewNamespace")
	c.Assert(newKey, NotNil)
	ck(newKey, "NewNamespace")

	keyWithTwoParents := mem.NewKey("Test", "NumberThree", 0, mem.NewKey("Test", "NumberThreeParent", 0, mem.NewKey("Test", "NumberThreeGrandparent", 0, nil)))
	newKey = SetKeyNamespace(keyWithTwoParents, "NewNamespace")
	c.Assert(newKey, NotNil)
	ck(newKey, "NewNamespace")

}

func (s *AppengineInterfacesTest) TestDatastoreDeadlineTimeout(c *C) {
	testErr := errors.New("aaah")
	called := 0
	testFunc := func(c context.Context) error {
		called++
		return testErr
	}

	// timeout exceeded
	err := withTimeout(context.Background(), time.Duration(-10), testFunc)
	c.Assert(status.Convert(err).Code(), Equals, codes.DeadlineExceeded)
	c.Assert(called, Equals, 1)

	// timeout not exceeded
	err = withTimeout(context.Background(), 20*time.Second, testFunc)
	c.Assert(err, Equals, testErr)
	c.Assert(called, Equals, 2)

	// deadline exceeded
	err = withDeadline(context.Background(), time.Now().Add(-10), testFunc)
	c.Assert(status.Convert(err).Code(), Equals, codes.DeadlineExceeded)
	c.Assert(called, Equals, 3)

	// deadline not exceeded
	err = withDeadline(context.Background(), time.Now().Add(20*time.Second), testFunc)
	c.Assert(err, Equals, testErr)
	c.Assert(called, Equals, 4)
}
