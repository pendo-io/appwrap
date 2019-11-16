// +build appenginevm go1.12

package appwrap

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"google.golang.org/api/option"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	logtypepb "google.golang.org/genproto/googleapis/logging/type"
)

var loggingCtxKey = struct{ k string }{"hlog context key"}

type loggingCtxValue struct {
	aeInfo AppengineInfo
	hreq   *http.Request
	logger *logging.Logger
	parent string
	sev    logtypepb.LogSeverity
	trace  string
}

// statusWriter pulled from here - used to keep track of size of response and the response code.
// https://www.reddit.com/r/golang/comments/7p35s4/how_do_i_get_the_response_status_for_my_middleware/dse625w/?context=8&depth=9
type statusWriter struct {
	http.ResponseWriter
	status int
	length int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, err := w.ResponseWriter.Write(b)
	w.length += n
	return n, err
}

func getLogger(aeInfo AppengineInfo, lc *logging.Client, logName string) *logging.Logger {
	return lc.Logger(logName, logging.CommonResource(&mrpb.MonitoredResource{
		Type: "gae_app",
		Labels: map[string]string{
			"module_id":  aeInfo.ModuleName(),
			"version_id": aeInfo.VersionID(),
			"project_id": aeInfo.AppID(),
		},
	}), logging.DelayThreshold(2*time.Second))
}

func getLogCtxVal(aeInfo AppengineInfo, hreq *http.Request, logger *logging.Logger, parent, trace string) *loggingCtxValue {
	return &loggingCtxValue{
		aeInfo: aeInfo,
		hreq:   hreq,
		logger: logger,
		parent: parent,
		trace:  trace,
	}
}

func logLabels(logCtxVal *loggingCtxValue) map[string]string {
	return map[string]string{
		"appengine.googleapis.com/instance_name": logCtxVal.aeInfo.InstanceID(),
		"pendo.io/request_url":                   logCtxVal.hreq.URL.String(),
	}
}

// for use in flex services with long-running tasks that don't handle http requests
func WrapBackgroundContextWithStackdriverLogger(c context.Context, logName string) (context.Context, func()) {
	aeInfo := NewAppengineInfoFromContext(c)

	project := aeInfo.AppID()
	if project == "" {
		panic("aelog: no GCP project set in environment")
	}
	parent := "projects/" + project
	lc, err := logging.NewClient(c, parent)
	if err != nil {
		panic(err)
	}
	if logName == "" {
		logName = ChildLogName
	}
	req, err := http.NewRequest("GET", "pendo.io/background", bytes.NewReader([]byte{}))
	if err != nil {
		panic(err)
	}
	logger := getLogger(aeInfo, lc, logName)
	logCtxVal := getLogCtxVal(aeInfo, req, logger, parent, parent+"/traces/"+fmt.Sprintf("%d", rand.Int63()))

	ctx := context.WithValue(c, loggingCtxKey, logCtxVal)
	return ctx, func() {
		lc.Close()
	}
}

func WrapHandlerWithStackdriverLogger(h http.Handler, logName string, opts ...option.ClientOption) http.Handler {
	if IsDevAppServer {
		return h
	}

	ctx := context.Background()
	aeInfo := NewAppengineInfoFromContext(ctx)

	project := aeInfo.AppID()
	if project == "" {
		panic("aelog: no GCP project set in environment")
	}
	parent := "projects/" + project

	lc, err := logging.NewClient(ctx, parent)
	if err != nil {
		panic(err)
	}
	if logName == "" {
		logName = ChildLogName
	}

	logger := getLogger(aeInfo, lc, logName)

	parentLogger := getLogger(aeInfo, lc, requestLogPath)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logCtxVal := getLogCtxVal(aeInfo, r, logger, parent, "")
		traceHeader := r.Header.Get("X-Cloud-Trace-Context")
		if traceHeader != "" {
			logCtxVal.trace = parent + "/traces/" + strings.Split(traceHeader, "/")[0]
		} else {
			logCtxVal.trace = parent + "/traces/" + fmt.Sprintf("%d", rand.Int63())
		}
		ctx := context.WithValue(r.Context(), loggingCtxKey, logCtxVal)
		start := time.Now()
		sw := &statusWriter{
			ResponseWriter: w,
		}
		h.ServeHTTP(sw, r.WithContext(ctx))

		e := logging.Entry{
			HTTPRequest: &logging.HTTPRequest{
				Latency:      time.Now().Sub(start),
				ResponseSize: int64(sw.length),
				Request:      r,
				Status:       sw.status,
			},
			Labels:    logLabels(logCtxVal),
			Severity:  logging.Severity(logCtxVal.sev),
			Timestamp: time.Now(),
			Trace:     logCtxVal.trace,
		}
		parentLogger.Log(e)
	})
}

func IsValidLoggingContext(ctx context.Context) bool {
	return ctx.Value(loggingCtxKey) != nil
}

func logFromContext(ctx context.Context, sev logtypepb.LogSeverity, format string, args ...interface{}) {
	ctxVal := ctx.Value(loggingCtxKey)
	if ctxVal == nil {
		panic("need to wrap http handler to use stackdriver logger")
	}

	logCtxVal := ctxVal.(*loggingCtxValue)

	e := logging.Entry{
		Labels:    logLabels(logCtxVal),
		Payload:   fmt.Sprintf(format, args...),
		Severity:  logging.Severity(sev),
		Timestamp: time.Now(),
		Trace:     logCtxVal.trace,
	}
	logCtxVal.logger.Log(e)
	if sev > logCtxVal.sev {
		logCtxVal.sev = sev
	}
}

func Criticalf(ctx context.Context, format string, args ...interface{}) {
	logFromContext(ctx, logtypepb.LogSeverity_CRITICAL, format, args...)
}

func Debugf(ctx context.Context, format string, args ...interface{}) {
	logFromContext(ctx, logtypepb.LogSeverity_DEBUG, format, args...)
}

func Errorf(ctx context.Context, format string, args ...interface{}) {
	logFromContext(ctx, logtypepb.LogSeverity_ERROR, format, args...)
}

func Infof(ctx context.Context, format string, args ...interface{}) {
	logFromContext(ctx, logtypepb.LogSeverity_INFO, format, args...)
}

func Warningf(ctx context.Context, format string, args ...interface{}) {
	logFromContext(ctx, logtypepb.LogSeverity_WARNING, format, args...)
}
