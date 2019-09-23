// +build appengine

package appwrap

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"google.golang.org/appengine/log"
)

func NewAppengineLogging(c context.Context) Logging {
	return appengineLogging{c}
}

func NewAppEngineLoggingService(c context.Context, aeInfo AppengineInfo) LoggingServiceInterface {
	loggingService := newStandardLoggingService(NewAppengineLogging(c))
	return loggingService
}

func WrapHandlerWithStackdriverLogger(h http.Handler, logName string, opts ...option.ClientOption) http.Handler {
	return h
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

func NewStackdriverLogging(c context.Context) Logging {
	return appengineLogging{c}
}
