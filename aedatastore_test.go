// +build !clouddatastore

package appwrap

import (
	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) TestSetKeyNamespace(c *C) {

	mem := NewLocalDatastore(true, nil)

	keyWithNoParent := mem.NewKey("Test", "NumberOne", 0, nil)
	c.Assert(func() {
		_ = SetKeyNamespace(keyWithNoParent, "NewNamespace")
	}, Panics, "Cannot set a key's namespace using old APIs")

}
