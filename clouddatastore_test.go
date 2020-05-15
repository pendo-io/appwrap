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

func (s *AppengineInterfacesTest) TestNewKey(c *C) {

	ck := func(k *datastore.Key, expectedNs string) {
		for thisKey := k; thisKey != nil; thisKey = thisKey.Parent {
			c.Assert(thisKey.Namespace, Equals, expectedNs)
		}
	}

	defaultNamespace := "s~memds"
	mem := NewLocalDatastore(true, nil)

	keyWithNoParent := mem.NewKey("Test", defaultNamespace, 0, nil)
	c.Assert(keyWithNoParent, NotNil)
	ck(keyWithNoParent, "s~memds")

	parent := mem.NewKey("Test", "Parent", 0, nil)
	parent.Namespace = ""

	keyWithParentNoNamespace := mem.NewKey("Test", "NumberTwo", 0, parent)
	c.Assert(keyWithParentNoNamespace, NotNil)
	ck(keyWithParentNoNamespace, "")

	parent = mem.NewKey("Test", "Parent", 0, nil)
	parent.Namespace = "Name SPACE"

	keyWithParentNamespaced := mem.NewKey("Test", "NumberThree", 0, parent)
	c.Assert(keyWithParentNamespaced, NotNil)
	ck(keyWithParentNamespaced, "Name SPACE")
}

type deadlineCheck struct {
	C              *C
	ExpectDeadline time.Time
	ExpectErr      error
	ReturnErr      error

	Called bool
}

func (c *deadlineCheck) Func(ctx context.Context) error {
	dl, hasDL := ctx.Deadline()

	if c.ExpectDeadline.IsZero() {
		c.C.Assert(hasDL, IsFalse)
	} else {
		c.C.Assert(hasDL, IsTrue)
		c.C.Assert(dl.Equal(c.ExpectDeadline), IsTrue)
	}

	ctxErr := ctx.Err()
	if c.ExpectErr == nil {
		c.C.Assert(ctxErr, IsNil)
	} else {
		c.C.Assert(ctxErr, Equals, c.ExpectErr)
	}

	c.Called = true
	return c.ReturnErr
}

func (s *AppengineInterfacesTest) TestDatastoreWithDeadline(c *C) {
	testErr := errors.New("aaah")

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// Deadline in future, no func error
	{
		deadline := time.Now().Add(time.Hour)
		ck := deadlineCheck{C: c, ExpectDeadline: deadline}
		err := withDeadline(context.Background(), deadline, ck.Func)
		c.Assert(err, IsNil)
		c.Assert(ck.Called, IsTrue)
	}
	// Deadline in past, no func error
	{
		deadline := time.Now().Add(-time.Hour)
		ck := deadlineCheck{C: c, ExpectDeadline: deadline, ExpectErr: context.DeadlineExceeded}
		err := withDeadline(context.Background(), deadline, ck.Func)
		c.Assert(err, IsNil)
		c.Assert(ck.Called, IsTrue)
	}
	// Deadline in future, parent context cancelled, canceled err
	{
		deadline := time.Now().Add(time.Hour)
		ck := deadlineCheck{C: c, ExpectDeadline: deadline, ExpectErr: context.Canceled}
		err := withDeadline(cancelledCtx, deadline, ck.Func)
		c.Assert(status.Code(err), Equals, codes.Canceled)
		c.Assert(ck.Called, IsTrue)
	}
	// Deadline in future, func error
	{
		deadline := time.Now().Add(time.Hour)
		ck := deadlineCheck{C: c, ExpectDeadline: deadline, ReturnErr: testErr}
		err := withDeadline(context.Background(), deadline, ck.Func)
		c.Assert(err, Equals, testErr)
		c.Assert(ck.Called, IsTrue)
	}
	// Deadline in past, func error
	{
		deadline := time.Now().Add(-time.Hour)
		ck := deadlineCheck{C: c, ExpectDeadline: deadline, ExpectErr: context.DeadlineExceeded, ReturnErr: testErr}
		err := withDeadline(context.Background(), deadline, ck.Func)
		c.Assert(status.Code(err), Equals, codes.DeadlineExceeded)
		c.Assert(ck.Called, IsTrue)
	}
	// Deadline in past, parent context cancelled, func error
	{
		deadline := time.Now().Add(-time.Hour)
		ck := deadlineCheck{C: c, ExpectDeadline: deadline, ExpectErr: context.Canceled, ReturnErr: testErr}
		err := withDeadline(cancelledCtx, deadline, ck.Func)
		c.Assert(status.Code(err), Equals, codes.Canceled)
		c.Assert(ck.Called, IsTrue)
	}
}
