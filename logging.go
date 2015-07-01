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
	Debugf(format string, args ...interface{})    // Debug message
	Infof(format string, args ...interface{})     // Information message
	Warningf(format string, args ...interface{})  // Warning message
	Errorf(format string, args ...interface{})    // Error message
	Criticalf(format string, args ...interface{}) // Critical message
}

// Sometimes, you just need to satify the interface and do nothing.
type NullLogger struct{}

func (nl NullLogger) Debugf(format string, args ...interface{})    {}
func (nl NullLogger) Infof(format string, args ...interface{})     {}
func (nl NullLogger) Warningf(format string, args ...interface{})  {}
func (nl NullLogger) Errorf(format string, args ...interface{})    {}
func (nl NullLogger) Criticalf(format string, args ...interface{}) {}

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
	fl.Logf("Warning: "+format, args...)
}

func (fl FormatLogger) Errorf(format string, args ...interface{}) {
	fl.Logf("Error: "+format, args...)
}

func (fl FormatLogger) Criticalf(format string, args ...interface{}) {
	fl.Logf("CRITICAL: "+format, args...)
}

type WriterLogger struct {
	FormatLogger
}

func NewWriterLogger(writer io.Writer) Logging {
	return WriterLogger{
		FormatLogger{
			func(format string, args ...interface{}) {
				buf := []byte(fmt.Sprintf(fmt.Sprintf(format+"\n", args...)))
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

// LevelLogger is a wrapper for any goisms.SimpleLogging that
// will filter out logging based on a minimum logging level.
type LevelLogger struct {
	minlevel      LogLevel
	wrappedLogger Logging
}

func NewLevelLogger(minlevel LogLevel, wrappedLogger Logging) Logging {
	return LevelLogger{minlevel, wrappedLogger}
}

func (ll LevelLogger) Debugf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelDebug {
		ll.wrappedLogger.Debugf(format, args...)
	}
}

func (ll LevelLogger) Infof(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelInfo {
		ll.wrappedLogger.Infof(format, args...)
	}
}

func (ll LevelLogger) Warningf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelWarning {
		ll.wrappedLogger.Warningf(format, args...)
	}
}

func (ll LevelLogger) Errorf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelError {
		ll.wrappedLogger.Errorf(format, args...)
	}
}

func (ll LevelLogger) Criticalf(format string, args ...interface{}) {
	if ll.minlevel <= LogLevelCritical {
		ll.wrappedLogger.Criticalf(format, args...)
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
