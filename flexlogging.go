// +build appenginevm go1.11

package appwrap

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/logging"
	"golang.org/x/net/context"
)

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
	client, err := logging.NewClient(c, aeInfo.AppID())
	if err != nil {
		panic(fmt.Sprintf("unable to configure stackdriver logger: %s", err.Error()))
	}

	stackdriverClient := newStackdriverClient(client)
	loggingService := newStackdriverLoggingService(stackdriverClient, aeInfo, NewAppengineLogging(c)).(*StackdriverLoggingService)

	return loggingService
}

func NewStackdriverLogging(c context.Context) Logging {
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

func init() {
	rand.Seed(int64(time.Now().Nanosecond()) ^ time.Now().Unix())
}
