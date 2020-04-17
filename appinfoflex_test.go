package appwrap

import (
	. "gopkg.in/check.v1"
)

func (t *AppengineInterfacesTest) TestModuleHostname(c *C) {
	ai := AppengineInfoFlex{}
	ck := func(v, s, a, expect string) {
		actual, err := ai.ModuleHostname(v, s, a)
		c.Assert(err, IsNil)
		c.Assert(actual, Equals, expect)
	}

	ck("", "", "", "theversion-dot-theservice-dot-theapp.appspot.com")
	ck("v", "", "", "v-dot-theservice-dot-theapp.appspot.com")
	ck("", "s", "", "theversion-dot-s-dot-theapp.appspot.com")
	ck("", "", "a", "theversion-dot-theservice-dot-a.appspot.com")
	ck("v", "s", "a", "v-dot-s-dot-a.appspot.com")
}
