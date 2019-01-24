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

func NewAppEngineLoggingService(c context.Context, aeInfo AppengineInfo, log Logging) LoggingServiceInterface {
	client, err := logging.NewClient(c, aeInfo.AppID())
	if err != nil {
		panic(fmt.Sprintf("unable to configure stackdriver logger: %s", err.Error()))
	}

	stackdriverClient := newStackdriverClient(client)
	loggingService := newStackdriverLoggingService(stackdriverClient, aeInfo, log).(*StackdriverLoggingService)

	return loggingService
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()) ^ time.Now().Unix())
}
