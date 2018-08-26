// +build appenginevm go1.11

package appwrap

import (
	"cloud.google.com/go/logging"
	"fmt"
	"golang.org/x/net/context"
	"math/rand"
	"os"
	"time"
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
	client, _ := logging.NewClient(c, aeInfo.AppID())
	stackdriverClient := NewStackdriverClient(client)
	logCh := make(chan LogMessage)
	loggingService := newStackdriverLoggingService(stackdriverClient, aeInfo, logCh, log)
	go loggingService.ProcessLogEntries()

	return loggingService
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()) ^ time.Now().Unix())
}
