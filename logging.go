package appwrap

import (
	"fmt"
	"io"
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

// Interface for system logging using printf() style strings
type Logging interface {
	Debugf(format string, args ...interface{})                // Debug message
	Infof(format string, args ...interface{})                 // Information message
	Warningf(format string, args ...interface{})              // Warning message
	Errorf(format string, args ...interface{})                // Error message
	Criticalf(format string, args ...interface{})             // Critical message
	Request(request, url, format string, args ...interface{}) // This is conditionally implemented
}

// Sometimes, you just need to satify the interface and do nothing.
type NullLogger struct{}

func (nl NullLogger) Debugf(format string, args ...interface{})                {}
func (nl NullLogger) Infof(format string, args ...interface{})                 {}
func (nl NullLogger) Warningf(format string, args ...interface{})              {}
func (nl NullLogger) Errorf(format string, args ...interface{})                {}
func (nl NullLogger) Criticalf(format string, args ...interface{})             {}
func (nl NullLogger) Request(request, url, format string, args ...interface{}) {}

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

// LevelLogger is a wrapper for any goisms.SimpleLogging that will filter out logging based on a minimum logging level. It emits
// the request itself iff another log is emitted
type LevelLogger struct {
	minlevel                                 LogLevel
	wrappedLogger                            Logging
	requestMethod, requestUrl, requestFormat string
	requestArgs                              []interface{}
}

func NewLevelLogger(minlevel LogLevel, wrappedLogger Logging) Logging {
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

func (ll *LevelLogger) emitRequest() {
	if ll.requestMethod != "" {
		ll.wrappedLogger.Request(ll.requestMethod, ll.requestUrl, ll.requestFormat, ll.requestArgs...)
		ll.requestMethod = ""
		ll.requestUrl = ""
		ll.requestFormat = ""
		ll.requestArgs = nil
	}
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
