package appwrap

import (
	"cloud.google.com/go/errorreporting"
	"context"
	"fmt"
	"net/http"
	"sync"
)

type ErrorReporter interface {
	Report(errReport ErrorReport)
	FlushReports()
}

type googleErrorReporter struct {
	client *errorreporting.Client
}

type ErrorReport struct {
	Err             error
	Req             *http.Request
	ErrorAffectsKey string
}

func NewGoogleErrorReporter(info AppengineInfo, ctx context.Context, log Logging) (*googleErrorReporter, error) {
	client, err := errorreporting.NewClient(ctx, info.DataProjectID(), errorreporting.Config{
		ServiceName:    info.ModuleName(),
		ServiceVersion: info.VersionID(),
		OnError: func(err error) {
			log.Errorf("failed to report error: %s", err.Error())
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create error reporter, error was: %s", err.Error())
	}
	return &googleErrorReporter{client: client}, nil
}

func (g *googleErrorReporter) Report(errReport ErrorReport) {
	g.client.Report(errorreporting.Entry{
		Error: errReport.Err,
		Req:   errReport.Req,
		User:  errReport.ErrorAffectsKey,
	})
}

func (g *googleErrorReporter) FlushReports() {
	g.client.Flush()
}

type errorForwardingLogger struct {
	wrappedLogger        Logging
	errorReporter        ErrorReporter
	errorAffectsKeyLabel string
	labelsLock           *sync.RWMutex
	labels               map[string]string
	req                  *http.Request
}

func (e errorForwardingLogger) Debugf(format string, args ...interface{}) {
	e.wrappedLogger.Debugf(format, args)
}

func (e errorForwardingLogger) Infof(format string, args ...interface{}) {
	e.wrappedLogger.Infof(format, args)
}

func (e errorForwardingLogger) Warningf(format string, args ...interface{}) {
	e.Warningf(format, args)
}

func (e errorForwardingLogger) Request(request, url, format string, args ...interface{}) {
	req, err := http.NewRequest(request, url, nil)
	if err != nil {
		e.wrappedLogger.Warningf("could not parse request for potential error forwarding: %s", err.Error())
	} else {
		e.req = req
	}
	e.wrappedLogger.Request(request, url, format, args)
}

func (e errorForwardingLogger) AddLabels(labels map[string]string) error {
	e.labelsLock.Lock()
	for k, v := range labels {
		e.labels[k] = v
	}
	e.labelsLock.Unlock()
	return e.wrappedLogger.AddLabels(labels)
}

func (e errorForwardingLogger) Errorf(format string, args ...interface{}) {
	e.wrappedLogger.Errorf(format, args)
	e.forwardError(forwardedError{
		msg: fmt.Sprintf(format, args),
	})
}

func (e errorForwardingLogger) Criticalf(format string, args ...interface{}) {
	e.wrappedLogger.Criticalf(format, args)
	e.forwardError(forwardedError{
		msg: fmt.Sprintf(format, args),
	})
}

type forwardedError struct {
	msg string
}

func (f forwardedError) Error() string {
	return f.msg
}

func (e errorForwardingLogger) forwardError(err error) {
	var affects string
	if len(e.errorAffectsKeyLabel) > 0 {
		e.labelsLock.RLock()
		affects = e.labels[e.errorAffectsKeyLabel]
		e.labelsLock.RUnlock()
	}
	e.errorReporter.Report(ErrorReport{
		Err:             err,
		Req:             e.req,
		ErrorAffectsKey: affects,
	})
}

func (g *googleErrorReporter) WrapLogger(logging Logging) errorForwardingLogger {
	return errorForwardingLogger{
		wrappedLogger: logging,
		errorReporter: g,
	}
}
