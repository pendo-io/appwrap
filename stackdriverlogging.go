package appwrap

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/logging"
	"github.com/go-martini/martini"
)

const (
	LogPendoJob       = LogName("pendo_job")
	LogPendoMapReduce = LogName("pendo_map_reduce")

	headerCloudTraceContext = "X-Cloud-Trace-Context"

	traceIdPath    = "appengine.googleapis.com/trace_id"
	requestLogPath = "appengine.googleapis.com/request_log"
)

// StackdriverLogging is a logger set up to work with an App Engine flexible environment
type StackdriverLogging struct {
	ctx          context.Context
	commonLabels map[string]string
	maxSeverity  logging.Severity
	childLogger  LoggerInterface
	parentLogger LoggerInterface
	logName      LogName
	mtx          *sync.Mutex
	traceContext string
	start        time.Time
	request      *http.Request
}

// NewStackdriverLogging will return a new logger set up to work with an App Engine flexible environment
func (sl *StackdriverLoggingService) newStackdriverLogging(commonLabels map[string]string, logName LogName, req *http.Request, traceId string) DataLogging {
	traceContext := traceId
	if req != nil {
		if traceHeader := req.Header.Get(headerCloudTraceContext); traceHeader != "" {
			traceContext = traceHeader
		}
	}

	labelsOptions := logging.CommonLabels(map[string]string{
		traceIdPath: traceContext,
	})

	return &StackdriverLogging{
		commonLabels: commonLabels,
		logName:      logName,
		maxSeverity:  logging.Default,
		childLogger:  sl.client.Logger(string(logName), labelsOptions, sl.resourceOptions),
		parentLogger: sl.client.Logger(requestLogPath, labelsOptions, sl.resourceOptions),
		mtx:          &sync.Mutex{},
		start:        time.Now(),
		traceContext: traceContext,
		request:      req,
	}
}

// AddLabel will add a common label used to link relevant log entries together
func (sl *StackdriverLogging) AddLabel(key, value string) {
	sl.mtx.Lock()
	defer sl.mtx.Unlock()
	sl.commonLabels[key] = value
}

// RemoveLabel will remove a common label used to link relevant log entries together
func (sl *StackdriverLogging) RemoveLabel(key string) {
	sl.mtx.Lock()
	defer sl.mtx.Unlock()
	delete(sl.commonLabels, key)
}

// Debugf will log the data in the specified format with a severity level of debug
func (sl *StackdriverLogging) Debugf(format string, args ...interface{}) {
	sl.formattedStringLog(logging.Debug, format, args...)
}

// Infof will log the data in the specified format with a severity level of info
func (sl *StackdriverLogging) Infof(format string, args ...interface{}) {
	sl.formattedStringLog(logging.Info, format, args...)
}

// Warningf will log the data in the specified format with a severity level of warning
func (sl *StackdriverLogging) Warningf(format string, args ...interface{}) {
	sl.formattedStringLog(logging.Warning, format, args...)
}

// Errorf will log the data in the specified format with a severity level of error
func (sl *StackdriverLogging) Errorf(format string, args ...interface{}) {
	sl.formattedStringLog(logging.Error, format, args...)
}

// Criticalf will log the data in the specified format with a severity level of critical
func (sl *StackdriverLogging) Criticalf(format string, args ...interface{}) {
	sl.formattedStringLog(logging.Critical, format, args...)
}

func (sl *StackdriverLogging) Request(method, url, format string, args ...interface{}) {}

func (sl *StackdriverLogging) AddLabels(labels map[string]string) error {
	for k, v := range labels {
		sl.AddLabel(k, v)
	}
	return nil
}

// Debug will log the specified data to the logging system with the debug log level
func (sl *StackdriverLogging) Debug(data interface{}) {
	sl.processLog(logging.Debug, data)
}

// Info will log the specified data to the logging system with the info log level
func (sl *StackdriverLogging) Info(data interface{}) {
	sl.processLog(logging.Info, data)
}

// Warning will log the specified data to the logging system with the warning log level
func (sl *StackdriverLogging) Warning(data interface{}) {
	sl.processLog(logging.Warning, data)
}

// Error will log the specified data to the logging system with the error log level
func (sl *StackdriverLogging) Error(data interface{}) {
	sl.processLog(logging.Error, data)
}

// Critical will log the specified data to the logging system with the critical log level
func (sl *StackdriverLogging) Critical(data interface{}) {
	sl.processLog(logging.Critical, data)
}

// formattedStringLog will format the string and log it with the child logger at the applicable log level
func (sl *StackdriverLogging) formattedStringLog(severity logging.Severity, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	sl.processLog(severity, msg)
}

// processLog will create a new log entry with the child logger
func (sl *StackdriverLogging) processLog(severity logging.Severity, data interface{}) {

	entry := logging.Entry{
		Labels:    sl.commonLabels,
		Payload:   data,
		Severity:  severity,
		Timestamp: time.Now(),
		Trace:     sl.traceContext,
	}

	sl.childLogger.Log(entry)

	if sl.maxSeverity < severity {
		sl.maxSeverity = severity
	}
}

// Close will close the logger and log request and response information to the parent logger
func (sl StackdriverLogging) Close(w http.ResponseWriter) {
	defer sl.childLogger.Flush()
	defer sl.parentLogger.Flush()
	if sl.request == nil {
		return
	}

	var entry logging.Entry
	if martiniWriter, ok := w.(martini.ResponseWriter); ok {
		entry = logging.Entry{
			HTTPRequest: &logging.HTTPRequest{
				Latency:      time.Now().Sub(sl.start),
				ResponseSize: int64(martiniWriter.Size()),
				Request:      sl.request,
				Status:       martiniWriter.Status(),
			},
			Labels:    sl.commonLabels,
			Severity:  sl.maxSeverity,
			Timestamp: time.Now(),
			Trace:     sl.traceContext,
		}

	} else {
		entry = logging.Entry{
			HTTPRequest: &logging.HTTPRequest{
				Latency: time.Now().Sub(sl.start),
				Request: sl.request,
			},
			Labels:    sl.commonLabels,
			Severity:  sl.maxSeverity,
			Timestamp: time.Now(),
			Trace:     sl.traceContext,
		}
	}
	sl.parentLogger.Log(entry)
}
