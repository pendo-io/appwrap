package appwrap

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"cloud.google.com/go/errorreporting"
)

type ErrorReporter interface {
	Close() error
	FlushReports()
	Report(errReport ErrorReport)
	WrapLogger(logging Logging, errorAffectsLabel string) Logging
}

type googleErrorReporter struct {
	client googleErrorReportingClient
}

type googleErrorReportingClient interface {
	Close() error
	Flush()
	Report(entry errorreporting.Entry)
}

type ErrorReport struct {
	Err             error
	Req             *http.Request
	ErrorAffectsKey string
}

/**
 * These reporters are meant to be long-lived, ideally one per program instance; Not used liberally
 * for individual requests/threads
 */
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

/**
 * It is important that all loggers returned from WrapLogger be unused or garbage collected before this
 * function is called
 */
func (g *googleErrorReporter) Close() error {
	return g.client.Close()
}

type errorForwardingLogger struct {
	wrappedLogger     Logging
	errorReporter     ErrorReporter
	errorAffectsLabel string
	labelsLock        *sync.RWMutex
	labels            map[string]string
	req               *http.Request
}

func (e errorForwardingLogger) Debugf(format string, args ...interface{}) {
	e.wrappedLogger.Debugf(format, args...)
}

func (e errorForwardingLogger) Infof(format string, args ...interface{}) {
	e.wrappedLogger.Infof(format, args...)
}

func (e errorForwardingLogger) Warningf(format string, args ...interface{}) {
	e.wrappedLogger.Warningf(format, args...)
}

func (e *errorForwardingLogger) Request(request, url, format string, args ...interface{}) {
	req, err := http.NewRequest(request, url, nil)
	if err != nil {
		e.wrappedLogger.Warningf("could not parse request for potential error forwarding: %s", err.Error())
	} else {
		e.req = req
	}
	e.wrappedLogger.Request(request, url, format, args...)
}

func (e *errorForwardingLogger) AddLabels(labels map[string]string) error {
	e.labelsLock.Lock()
	for k, v := range labels {
		e.labels[k] = v
	}
	e.labelsLock.Unlock()
	return e.wrappedLogger.AddLabels(labels)
}

func (e *errorForwardingLogger) Errorf(format string, args ...interface{}) {
	e.wrappedLogger.Errorf(format, args...)
	e.forwardError(forwardedError{
		msg: fmt.Sprintf(format, args...),
	})
}

func (e *errorForwardingLogger) Criticalf(format string, args ...interface{}) {
	e.wrappedLogger.Criticalf(format, args...)
	e.forwardError(forwardedError{
		msg: fmt.Sprintf(format, args...),
	})
}

type forwardedError struct {
	msg string
}

func (f forwardedError) Error() string {
	return f.msg
}

func (e *errorForwardingLogger) forwardError(err error) {
	var affects string
	if len(e.errorAffectsLabel) > 0 {
		e.labelsLock.RLock()
		affects = e.labels[e.errorAffectsLabel]
		e.labelsLock.RUnlock()
	}
	e.errorReporter.Report(ErrorReport{
		Err:             err,
		Req:             e.req,
		ErrorAffectsKey: affects,
	})
}

/**
 * Unlike NewGoogleErrorReporter this function may be used liberally, and across threads/requests
 */
func (g *googleErrorReporter) WrapLogger(logging Logging, errorAffectsLabel string) Logging {
	if _, alreadyWrapped := logging.(*errorForwardingLogger); alreadyWrapped {
		panic("bug! this logger is already wrapped!")
	}
	return &errorForwardingLogger{
		wrappedLogger:     logging,
		errorReporter:     g,
		errorAffectsLabel: errorAffectsLabel,
		labelsLock:        &sync.RWMutex{},
		labels:            make(map[string]string),
	}
}
