package appwrap

import (
	"reflect"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type AppengineInterfacesTest struct {
}

var _ = Suite(&AppengineInterfacesTest{})

func sameMemory(one, two interface{}) bool {
	valOne, valTwo := reflect.ValueOf(one), reflect.ValueOf(two)

	if valOne.Kind() != valTwo.Kind() {
		return false
	}

	switch valOne.Kind() {
	case reflect.Slice:
		return valOne.Pointer() == valTwo.Pointer() && valOne.Cap() == valTwo.Cap()
	case reflect.Map:
		return valOne.Pointer() == valTwo.Pointer()
	default:
		panic("unsupported type")
	}
}

func (s *AppengineInterfacesTest) TestSameMemory(c *C) {
	thing := make(map[string]interface{}, 0)
	c.Assert(sameMemory(thing, thing), IsTrue)
	thingTwo := make(map[string]interface{}, 0)
	c.Assert(sameMemory(thing, thingTwo), IsFalse)

	slice := make([]string, 0, 10)
	c.Assert(sameMemory(slice, slice), IsTrue)
	sliceTwo := make([]string, 0, 10)
	c.Assert(sameMemory(slice, sliceTwo), IsFalse)
}
