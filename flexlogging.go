// +build appenginevm go1.11

package appwrap

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/logging"
	"golang.org/x/net/context"
)

const (
	logName = LogName("stackdriver-logging")
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

func NewStackDriverLogging(r *http.Request, fakeCtx context.Context) (CloseableLogger, func(w http.ResponseWriter)) {
	ctx := context.Background()
	log := NewAppengineLogging(ctx)
	appinfo := NewAppengineInfoFromContext(ctx)
	stackDriverService := NewAppEngineLoggingService(ctx, appinfo, log).(*StackdriverLoggingService)
	logger := stackDriverService.CreateLog(map[string]string{
		"module_id": appinfo.ModuleName(),
	},
		logName,
		r,
		getTraceId()).(CloseableLogger)

	closeLogger := func (w http.ResponseWriter) {
		stackDriverService.Close()
		logger.Close(w)
	}

	return logger, closeLogger
}

func getTraceId() string {
	traceBytes := make([]byte, 30)
	rand.Read(traceBytes)
	return base64.StdEncoding.EncodeToString(traceBytes)
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()) ^ time.Now().Unix())
}
