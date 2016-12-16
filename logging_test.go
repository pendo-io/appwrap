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

	c.Check(w.String(), Equals,
		"debug: msg 0\n"+
			"info: msg 1\n"+
			"Warning: msg 2\n"+
			"Error: msg 3\n"+
			"CRITICAL: msg 4\n")
}

func (s *AppengineInterfacesTest) TestLevelLogger(c *C) {
	w := &bytes.Buffer{}

	jay := NewWriterLogger(w)
	silentBob := NewLevelLogger(LogLevelSilence, jay)

	silentBob.Debugf("WASSUP")
	silentBob.Infof("YO")
	silentBob.Warningf("WATCH OUT")
	silentBob.Errorf("BUSTED")
	silentBob.Criticalf("OUTTA HERE")

	c.Check(w.String(), Equals, "")
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
			"Warning: prefix: msg 2\n"+
			"Error: prefix: msg 3\n"+
			"CRITICAL: prefix: msg 4\n")
}

func (s *AppengineInterfacesTest) TestDoublePercents(c *C) {
	w := &bytes.Buffer{}
	log := NewWriterLogger(w)
	log.Infof("%.2f%%", 50.5)
	c.Assert(w.String(), Equals, "info: 50.50%\n")

}
