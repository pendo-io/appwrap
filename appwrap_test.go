package appwrap

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type AppengineInterfacesTest struct {
}

var _ = Suite(&AppengineInterfacesTest{})
