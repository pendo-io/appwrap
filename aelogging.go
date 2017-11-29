// +build appengine,!appenginevm

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
)

func NewAppengineLogging(c context.Context) Logging {
	return appengineLogging{c}
}

type appengineLogging struct {
	c context.Context
}

func (al appengineLogging) Debugf(format string, args ...interface{}) {
	log.Debugf(al.c, format, args...)
}

func (al appengineLogging) Infof(format string, args ...interface{}) {
	log.Infof(al.c, format, args...)
}

func (al appengineLogging) Warningf(format string, args ...interface{}) {
	log.Warningf(al.c, format, args...)
}

func (al appengineLogging) Errorf(format string, args ...interface{}) {
	log.Errorf(al.c, format, args...)
}

func (al appengineLogging) Criticalf(format string, args ...interface{}) {
	log.Criticalf(al.c, format, args...)
}

func (al appengineLogging) Request(method, url, format string, args ...interface{}) {
	// this is logged automatically by appengine
}
