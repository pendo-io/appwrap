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

type WriterLogger struct {
	writer io.Writer
}

func NewWriterLogger(writer io.Writer) Logging {
	return WriterLogger{writer}
}

func (fl WriterLogger) log(priority string, format string, args ...interface{}) {
	buf := []byte(fmt.Sprintf("%s: %s\n", priority, fmt.Sprintf(format, args...)))
	written := 0
	for written < len(buf) {
		if wrote, err := fl.writer.Write(buf[written:len(buf)]); err != nil {
			return
		} else {
			written += wrote
		}
	}
}

func (fl WriterLogger) Debugf(format string, args ...interface{}) {
	fl.log("debug", format, args...)
}

func (fl WriterLogger) Infof(format string, args ...interface{}) {
	fl.log("info", format, args...)
}

func (fl WriterLogger) Warningf(format string, args ...interface{}) {
	fl.log("Warning", format, args...)
}

func (fl WriterLogger) Errorf(format string, args ...interface{}) {
	fl.log("Error", format, args...)
}

func (fl WriterLogger) Criticalf(format string, args ...interface{}) {
	fl.log("CRITICAL", format, args...)
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
