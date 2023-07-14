package appwrap

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

var labelsMtx = &sync.RWMutex{}

func NewAppengineLogging(c context.Context) Logging {
	nonce := fmt.Sprintf("%016x", rand.Uint64())
	stdout := FormatLogger{
		Logf: func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stdout, nonce+": "+format+"\n", args...)
		},
	}
	stderr := FormatLogger{
		Logf: func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, nonce+": "+format+"\n", args...)
		},
	}

	return TeeLogging{
		Logs: []Logging{
			stdout,
			NewLevelLogger(LogLevelWarning, stderr),
		},
	}
}

func NewAppEngineLoggingService(c context.Context, aeInfo AppengineInfo) LoggingServiceInterface {
	stackdriverClient := newStackdriverClient()
	loggingService := newStackdriverLoggingService(stackdriverClient, aeInfo, NewAppengineLogging(c)).(*StackdriverLoggingService)

	return loggingService
}

func NewStackdriverLogging(c context.Context) Logging {
	if !IsValidLoggingContext(c) {
		l := NewWriterLogger(os.Stderr)
		l.Criticalf("attempt to wrap non-Stackdriver-enabled context for logging; falling back to stderr")
		return l
	}
	return stackdriverLogging{c}
}

type stackdriverLogging struct {
	c context.Context
}

func (sl stackdriverLogging) Debugf(format string, args ...interface{}) {
	Debugf(sl.c, format, args...)
}

func (sl stackdriverLogging) Infof(format string, args ...interface{}) {
	Infof(sl.c, format, args...)
}

func (sl stackdriverLogging) Warningf(format string, args ...interface{}) {
	Warningf(sl.c, format, args...)
}

func (sl stackdriverLogging) Errorf(format string, args ...interface{}) {
	Errorf(sl.c, format, args...)
}

func (sl stackdriverLogging) Criticalf(format string, args ...interface{}) {
	Criticalf(sl.c, format, args...)
}

func (sl stackdriverLogging) Request(method, url, format string, args ...interface{}) {
	// this is logged automatically by stackdriver logger
}

func (sl stackdriverLogging) AddLabels(labels map[string]string) error {
	ctxVal := (sl.c).Value(loggingCtxKey)
	if ctxVal == nil {
		return errors.New("failed to add log labels, needs to have a logging context")
	}

	logCtxVal := ctxVal.(*loggingCtxValue)

	labelsMtx.Lock()
	defer labelsMtx.Unlock()
	for k, v := range labels {
		logCtxVal.labels[k] = v
	}

	return nil
}

func (sl stackdriverLogging) TraceID() string {
	ctxVal := (sl.c).Value(loggingCtxKey)
	if ctxVal == nil {
		panic(errors.New("failed to add log labels, needs to have a logging context"))
	}
	return ctxVal.(*loggingCtxValue).trace
}

func (logCtxVal *loggingCtxValue) getLabels() map[string]string {
	labelsMtx.RLock()
	defer labelsMtx.RUnlock()
	labels := make(map[string]string, len(logCtxVal.labels))
	for k, v := range logCtxVal.labels {
		labels[k] = v
	}

	return labels
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()) ^ time.Now().Unix())
}
