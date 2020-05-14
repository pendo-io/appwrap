package appwrap

import (
	"os"

	. "gopkg.in/check.v1"
)

func (t *AppengineInterfacesTest) TestModuleHostnameK8s(c *C) {
	ai := AppengineInfoK8s{}
	ck := func(v, s, a, expect string) {
		actual, err := ai.ModuleHostname(v, s, a)
		c.Assert(err, IsNil)
		c.Assert(actual, Equals, expect)
	}

	// GKE
	os.Setenv("K8S_SERVICE", "theservice")
	os.Setenv("K8S_DOMAIN", "domain.com")
	ck("", "", "", "theservice-dot-theapp.domain.com")
	ck("v", "", "", "theservice-dot-theapp.domain.com")
	ck("", "s", "", "s-dot-theapp.domain.com")
	ck("", "", "a", "theservice-dot-a.domain.com")
	ck("v", "s", "a", "s-dot-a.domain.com")
	os.Setenv("K8S_SERVICE", "")
	os.Setenv("K8S_DOMAIN", "")
}
