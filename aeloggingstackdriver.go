// +build appenginevm

package appwrap

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"google.golang.org/api/option"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	logtypepb "google.golang.org/genproto/googleapis/logging/type"
)

var ctxKey = struct{ k string }{"hlog context key"}

type ctxValue struct {
	parent string
	hreq   *http.Request
	logger *logging.Logger
	sev    logtypepb.LogSeverity
	trace  string
}

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

func WrapHandlerWithStackdriverLogger(h http.Handler, logName string, opts ...option.ClientOption) http.Handler {
	if IsDevAppServer {
		return h
	}

	ctx := context.Background()

	project := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if project == "" {
		panic("aelog: GOOGLE_CLOUD_PROJECT not set")
	}
	parent := "projects/" + project

	lc, err := logging.NewClient(ctx, parent)
	if err != nil {
		panic(err)
	}
	if logName == "" {
		logName = ChildLogName
	}
	logger := lc.Logger(logName, logging.CommonResource(&mrpb.MonitoredResource{
		Type: "gae_app",
		Labels: map[string]string{
			"module_id":  os.Getenv("GAE_SERVICE"),
			"version_id": os.Getenv("GAE_VERSION"),
			"project_id": project,
		},
	}))

	parentLogger := lc.Logger(requestLogPath, logging.CommonResource(&mrpb.MonitoredResource{
		Type: "gae_app",
		Labels: map[string]string{
			"module_id":  os.Getenv("GAE_SERVICE"),
			"version_id": os.Getenv("GAE_VERSION"),
			"project_id": project,
		},
	}))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cvt := &ctxValue{
			parent: parent,
			hreq:   r,
			logger: logger,
		}
		traceHeader := r.Header.Get("X-Cloud-Trace-Context")
		if traceHeader != "" {
			cvt.trace = parent + "/traces/" + strings.Split(traceHeader, "/")[0]
		} else {
			cvt.trace = parent + "/traces/" + fmt.Sprintf("%d", rand.Int63())
		}
		ctx := context.WithValue(r.Context(), ctxKey, cvt)
		start := time.Now()
		sw := &statusWriter{
			ResponseWriter: w,
		}
		h.ServeHTTP(sw, r.WithContext(ctx))

		e := logging.Entry{
			Timestamp: time.Now(),
			Severity:  logging.Severity(cvt.sev),
			HTTPRequest: &logging.HTTPRequest{
				Latency:      time.Now().Sub(start),
				ResponseSize: int64(sw.length),
				Request:      r,
				Status:       sw.status,
			},
			Trace: cvt.trace,
		}
		parentLogger.Log(e)
	})
}

func logFromContext(ctx context.Context, sev logtypepb.LogSeverity, format string, args ...interface{}) {
	cv := ctx.Value(ctxKey)
	if cv == nil {
		// Handler wasn't wrapped.
		panic("need to wrap http handler to use stackdriver logger")
	}

	cvt := cv.(*ctxValue)

	e := logging.Entry{
		Timestamp: time.Now(),
		Severity:  logging.Severity(sev),
		Payload:   fmt.Sprintf(format, args...),
		Trace:     cvt.trace,
	}
	cvt.logger.Log(e)
	if sev > cvt.sev {
		cvt.sev = sev
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
