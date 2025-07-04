package appwrap

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

type LogLevel int

const (
	LogLevelDebug = LogLevel(iota)
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelCritical
	LogLevelSilence
)

const (
	ChildLogName = "pendo.io/child_log"
)

// Interface for system logging using printf() style strings
type Logging interface {
	Debugf(format string, args ...interface{})                // Debug message
	Infof(format string, args ...interface{})                 // Information message
	Warningf(format string, args ...interface{})              // Warning message
	Errorf(format string, args ...interface{})                // Error message
	Criticalf(format string, args ...interface{})             // Critical message
	Request(request, url, format string, args ...interface{}) // This is conditionally implemented
	AddLabels(labels map[string]string) error                 // Adds labels to your log message
	TraceID() string                                          // Trace ID for current request, or "" if N/A
}

// DataLogging for system logging that can accept json, strings, or structs
type DataLogging interface {
	Logging
	Debug(data interface{})
	Info(data interface{})
	Warning(data interface{})
	Error(data interface{})
	Critical(data interface{})
	Close(http.ResponseWriter)
}

type LogLeveler interface {
	GetMinLogLevel() LogLevel
}

// Sometimes, you just need to satify the interface and do nothing.
type NullLogger struct{}

func (nl NullLogger) Debugf(format string, args ...interface{})                {}
func (nl NullLogger) Infof(format string, args ...interface{})                 {}
func (nl NullLogger) Warningf(format string, args ...interface{})              {}
func (nl NullLogger) Errorf(format string, args ...interface{})                {}
func (nl NullLogger) Criticalf(format string, args ...interface{})             {}
func (nl NullLogger) Request(request, url, format string, args ...interface{}) {}
func (nl NullLogger) AddLabels(labels map[string]string) error                 { return nil }
func (nl NullLogger) TraceID() string                                          { return "" }

type FormatLogger struct {
	Logf func(format string, args ...interface{})
}

func (fl FormatLogger) Debugf(format string, args ...interface{}) {
	fl.Logf("debug: "+format, args...)
}

func (fl FormatLogger) Infof(format string, args ...interface{}) {
	fl.Logf("info: "+format, args...)
}

func (fl FormatLogger) Warningf(format string, args ...interface{}) {
	fl.Logf("warning: "+format, args...)
}

func (fl FormatLogger) Errorf(format string, args ...interface{}) {
	fl.Logf("error: "+format, args...)
}

func (fl FormatLogger) Criticalf(format string, args ...interface{}) {
	fl.Logf("critical: "+format, args...)
}

func (fl FormatLogger) Request(request, url, format string, args ...interface{}) {
	if len(format) > 0 {
		format = " " + format
	}

	fl.Logf("REQUEST: %s %s"+format, append([]interface{}{request, url}, args...)...)
}

func (fl FormatLogger) AddLabels(labels map[string]string) error {
	return nil
}

func (fl FormatLogger) TraceID() string { return "" }

type WriterLogger struct {
	FormatLogger
}

func NewWriterLogger(writer io.Writer) Logging {
	return WriterLogger{
		FormatLogger{
			func(format string, args ...interface{}) {
				buf := []byte(fmt.Sprintf(format+"\n", args...))
				written := 0
				for written < len(buf) {
					if wrote, err := writer.Write(buf[written:len(buf)]); err != nil {
						return
					} else {
						written += wrote
					}
				}
			},
		},
	}
}

// LevelLogger is a wrapper for another Logging that will filter based on log level. It calls the inner Logging's
// Request once any log message passes the filter.
type LevelLogger struct {
	minlevel                                 LogLevel
	wrappedLogger                            Logging
	requestMethod, requestUrl, requestFormat string
	requestArgs                              []interface{}
}

func NewLevelLogger(minlevel LogLevel, wrappedLogger Logging) *LevelLogger {
	if levelLogger, ok := wrappedLogger.(*LevelLogger); ok {
		// unwrap LevelLogger so we can override the level with a lower one
		wrappedLogger = levelLogger.wrappedLogger
	}
	return &LevelLogger{minlevel: minlevel, wrappedLogger: wrappedLogger}
}

func (ll *LevelLogger) Debugf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelDebug {
		ll.emitRequest()
		ll.wrappedLogger.Debugf(format, args...)
	}
}

func (ll *LevelLogger) Infof(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelInfo {
		ll.emitRequest()
		ll.wrappedLogger.Infof(format, args...)
	}
}

func (ll *LevelLogger) Warningf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelWarning {
		ll.emitRequest()
		ll.wrappedLogger.Warningf(format, args...)
	}
}

func (ll *LevelLogger) Errorf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelError {
		ll.emitRequest()
		ll.wrappedLogger.Errorf(format, args...)
	}
}

func (ll *LevelLogger) Criticalf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelCritical {
		ll.emitRequest()
		ll.wrappedLogger.Criticalf(format, args...)
	}
}

func (ll *LevelLogger) Request(request, url, format string, args ...interface{}) {
	ll.requestMethod = request
	ll.requestUrl = url
	ll.requestFormat = format
	ll.requestArgs = args
}

func (ll *LevelLogger) AddLabels(labels map[string]string) error {
	return ll.wrappedLogger.AddLabels(labels)
}

func (ll *LevelLogger) emitRequest() {
	if ll.requestMethod != "" {
		ll.wrappedLogger.Request(ll.requestMethod, ll.requestUrl, ll.requestFormat, ll.requestArgs...)
		ll.requestMethod = ""
		ll.requestUrl = ""
		ll.requestFormat = ""
		ll.requestArgs = nil
	}
}

func (ll *LevelLogger) TraceID() string {
	return ll.wrappedLogger.TraceID()
}

func (ll *LevelLogger) SetMinLevel(level LogLevel) {
	ll.minlevel = level // unsynchronized write, since we don't care if it takes a bit to be observed by other callers
}

func (ll *LevelLogger) GetMinLogLevel() LogLevel {
	return ll.minlevel
}

type TeeLogging struct {
	Logs []Logging
}

func (tee TeeLogging) Debugf(format string, args ...interface{}) {
	for _, l := range tee.Logs {
		l.Debugf(format, args...)
	}
}

func (tee TeeLogging) Infof(format string, args ...interface{}) {
	for _, l := range tee.Logs {
		l.Infof(format, args...)
	}
}

func (tee TeeLogging) Warningf(format string, args ...interface{}) {
	for _, l := range tee.Logs {
		l.Warningf(format, args...)
	}
}

func (tee TeeLogging) Errorf(format string, args ...interface{}) {
	for _, l := range tee.Logs {
		l.Errorf(format, args...)
	}
}

func (tee TeeLogging) Criticalf(format string, args ...interface{}) {
	for _, l := range tee.Logs {
		l.Criticalf(format, args...)
	}
}

func (tee TeeLogging) Request(request, url, format string, args ...interface{}) {
	for _, l := range tee.Logs {
		l.Request(request, url, format, args...)
	}
}

func (tee TeeLogging) AddLabels(labels map[string]string) error {
	for _, l := range tee.Logs {
		if err := l.AddLabels(labels); err != nil {
			return err
		}
	}
	return nil
}

func (tee TeeLogging) TraceID() string {
	for _, log := range tee.Logs {
		if id := log.TraceID(); id != "" {
			return id
		}
	}
	return ""
}

type PrefixLogger struct {
	Logging
	Prefix string
}

func (pl PrefixLogger) Debugf(format string, args ...interface{}) {
	pl.Logging.Debugf(pl.Prefix+format, args...)
}

func (pl PrefixLogger) Infof(format string, args ...interface{}) {
	pl.Logging.Infof(pl.Prefix+format, args...)
}

func (pl PrefixLogger) Warningf(format string, args ...interface{}) {
	pl.Logging.Warningf(pl.Prefix+format, args...)
}

func (pl PrefixLogger) Errorf(format string, args ...interface{}) {
	pl.Logging.Errorf(pl.Prefix+format, args...)
}

func (pl PrefixLogger) Criticalf(format string, args ...interface{}) {
	pl.Logging.Criticalf(pl.Prefix+format, args...)
}

func (pl PrefixLogger) Request(request, url, format string, args ...interface{}) {
	pl.Logging.Request(request, url, pl.Prefix+format, args...)
}

func (pl PrefixLogger) AddLabels(labels map[string]string) error {
	return pl.Logging.AddLabels(labels)
}

func (pl PrefixLogger) TraceID() string {
	return pl.Logging.TraceID()
}

func NewStdLogger(writer io.Writer) Logging {
	if LocalDebug {
		return NewWriterLogger(writer)
	}
	return NewJsonLogger(writer, true)
}

func NewJsonLogger(writer io.Writer, addStandardLogLabels bool) Logging {
	logger := &JsonLogger{
		slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{
			AddSource:   false,
			ReplaceAttr: replaceStdOutKeys,
		})),
	}
	if addStandardLogLabels {
		aeInfo := NewAppengineInfoFromContext(context.Background())
		if err := logger.AddLabels(map[string]string{
			"appengine.googleapis.com/instance_name": aeInfo.InstanceID(),
			"pendo_io_node_name":                     aeInfo.NodeName(),
			"pendo_io_service":                       aeInfo.ModuleName(),
			"pendo_io_version":                       aeInfo.VersionID(),
		}); err != nil {
			logger.Errorf("Error adding standard log labels: %v", err)
		}
		return logger
	}
	return logger
}

type JsonLogger struct {
	log *slog.Logger
}

func (jl *JsonLogger) Debugf(format string, args ...interface{}) {
	jl.log.Debug(fmt.Sprintf(format, args...))
}

func (jl *JsonLogger) Infof(format string, args ...interface{}) {
	jl.log.Info(fmt.Sprintf(format, args...))
}

func (jl *JsonLogger) Warningf(format string, args ...interface{}) {
	jl.log.Warn(fmt.Sprintf(format, args...))
}

func (jl *JsonLogger) Errorf(format string, args ...interface{}) {
	jl.log.Error(fmt.Sprintf(format, args...))
}

func (jl *JsonLogger) Criticalf(format string, args ...interface{}) {
	// There is no critical in slog
	jl.log.Error(fmt.Sprintf(format, args...))
}

func (jl *JsonLogger) Request(request, url, format string, args ...interface{}) {
	if len(format) > 0 {
		format = " " + format
	}

	jl.log.Info("REQUEST: %s %s"+format, append([]interface{}{request, url}, args...)...)
}

func (jl *JsonLogger) AddLabels(labels map[string]string) error {
	jl.log = jl.log.With("logging.googleapis.com/labels", labels)
	return nil
}

func (jl *JsonLogger) TraceID() string { return "" }

func replaceStdOutKeys(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.MessageKey {
		a.Key = "message"
	}
	if a.Key == slog.LevelKey {
		a.Key = "severity"
	}
	return a
}
