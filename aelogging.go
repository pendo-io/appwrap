// +build appengine

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
	"net/http"
)

func NewStackDriverLogging(r *http.Request, c context.Context) (CloseableLogger, func(w http.ResponseWriter)) {
	logger := NewAppengineLogging(c).(CloseableLogger)
	stackDriverService := StandardLoggingService{}

	closeLogger := func(w http.ResponseWriter) {
		// Empty method exists for flexlogging compatibility
	}
	return logger, closeLogger
}

func NewAppengineLogging(c context.Context) Logging {
	return appengineLogging{c}
}

func NewAppEngineLoggingService(c context.Context, aeInfo AppengineInfo, log Logging) LoggingServiceInterface {
	loggingService := newStandardLoggingService(log)
	return loggingService
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

func (al appengineLogging) Close(w http.ResponseWriter) {
	// Empty method exists for flexlogging compatibility
}
