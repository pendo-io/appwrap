// +build appengine,!appenginevm

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
	"net/http"
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

func (al appengineLogging) Debug(data interface{}) {
	log.Debugf(al.c, "%+v", data)
}

func (al appengineLogging) Info(data interface{}) {
	log.Infof(al.c, "%+v", data)
}

func (al appengineLogging) Warning(data interface{}) {
	log.Warningf(al.c, "%+v", data)
}

func (al appengineLogging) Error(data interface{}) {
	log.Errorf(al.c, "%+v", data)
}

func (al appengineLogging) Critical(data interface{}) {
	log.Criticalf(al.c, "%+v", data)
}

func (al appengineLogging) Close(w http.ResponseWriter) {
	// this is not needed for app engine standard
}


