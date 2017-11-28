package appwrap

import (
	"bytes"
	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) TestLogging(c *C) {
	w := &bytes.Buffer{}

	log := NewWriterLogger(w)
	log.Debugf("msg %d", 0)
	log.Infof("msg %d", 1)
	log.Warningf("msg %d", 2)
	log.Errorf("msg %d", 3)
	log.Criticalf("msg %d", 4)
	log.Request("GET", "http://foo.com", "param %d", "hello")

	c.Check(w.String(), Equals,
		"debug: msg 0\n"+
			"info: msg 1\n"+
			"warning: msg 2\n"+
			"error: msg 3\n"+
			"critical: msg 4\n"+
			"REQUEST: GET http://foo.com param %!d(string=hello)\n")
}

func (s *AppengineInterfacesTest) TestLevelLogger(c *C) {
	logThings := func(l Logging) {
		l.Request("GET", "http://foo.com", "param %s", "hello")
		l.Debugf("WASSUP")
		l.Infof("YO")
		l.Warningf("WATCH OUT")
		l.Errorf("BUSTED")
		l.Criticalf("OUTTA HERE")
	}

	w := &bytes.Buffer{}
	logThings(NewLevelLogger(LogLevelSilence, NewWriterLogger(w)))
	c.Check(w.String(), Equals, "")

	w = &bytes.Buffer{}
	logThings(NewLevelLogger(LogLevelError, NewWriterLogger(w)))
	c.Check(w.String(), Equals,
		"REQUEST: GET http://foo.com param hello\n"+
			"error: BUSTED\n"+
			"critical: OUTTA HERE\n")

}

func (s *AppengineInterfacesTest) TestPrefixLogger(c *C) {
	w := &bytes.Buffer{}

	log := PrefixLogger{NewWriterLogger(w), "prefix: "}
	log.Debugf("msg %d", 0)
	log.Infof("msg %d", 1)
	log.Warningf("msg %d", 2)
	log.Errorf("msg %d", 3)
	log.Criticalf("msg %d", 4)

	c.Check(w.String(), Equals,
		"debug: prefix: msg 0\n"+
			"info: prefix: msg 1\n"+
			"warning: prefix: msg 2\n"+
			"error: prefix: msg 3\n"+
			"critical: prefix: msg 4\n")
}

func (s *AppengineInterfacesTest) TestDoublePercents(c *C) {
	w := &bytes.Buffer{}
	log := NewWriterLogger(w)
	log.Infof("%.2f%%", 50.5)
	c.Assert(w.String(), Equals, "info: 50.50%\n")

}
