package appwrap

import (
	"strings"

	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) TestTruncateLog(c *C) {
	notTooLong := strings.Repeat("a", maxLogLength)
	c.Assert(len(notTooLong), Equals, maxLogLength)
	log := truncateLog(notTooLong)
	c.Assert(len(log), Equals, maxLogLength)
	c.Assert(log, Equals, notTooLong)

	slightlyTooLong := notTooLong + "a"
	log = truncateLog(slightlyTooLong)
	// 31 is the length of the prefix that we append
	c.Assert(len(log), Equals, maxLogLength+len(truncatedLogPrefix))
	c.Assert(log, Equals, truncatedLogPrefix+notTooLong)
}
