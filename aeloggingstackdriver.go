package appwrap

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"google.golang.org/api/option"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	logtypepb "google.golang.org/genproto/googleapis/logging/type"
)

var loggingCtxKey = struct{ k string }{"hlog context key"}

const IPListHeader = "X-Forwarded-For"

type loggingCtxValue struct {
	aeInfo AppengineInfo
	hreq   *http.Request
	labels map[string]string
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

// Flush() doing nothing can be a bug for callers that need Flush() to actually flush. But we can either not implement flush (which is bad),
// set an error to the writer (which is bad, though it's what other libraries do), or panic. so we're just going with this behavior
func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
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

const (
	// thresholds that, when reached, cause a flush of logs to be sent over grpc
	loggingFlushTimeTrigger  = 5 * time.Second
	loggingFlushSizeTrigger  = 5 << 20
	loggingFlushCountTrigger = 5000
)

func resourceLabels(aeInfo AppengineInfo) map[string]string {
	if InKubernetes() {
		return map[string]string{
			"project_id":     aeInfo.DataProjectID(),
			"location":       aeInfo.Zone(),
			"namespace_name": aeInfo.DataProjectID(),
			"pod_name":       aeInfo.InstanceID(),
			"cluster_name":   aeInfo.ClusterName(),
		}
	} else {
		return map[string]string{
			"module_id":  aeInfo.ModuleName(),
			"version_id": aeInfo.VersionID(),
			"project_id": aeInfo.NativeProjectID(),
		}
	}
}

func getLogger(aeInfo AppengineInfo, lc *logging.Client, logName string) *logging.Logger {
	return lc.Logger(logName, logging.CommonResource(&mrpb.MonitoredResource{
		Type:   monitoredType(),
		Labels: resourceLabels(aeInfo),
	}), logging.DelayThreshold(loggingFlushTimeTrigger), logging.EntryByteThreshold(loggingFlushSizeTrigger), logging.EntryCountThreshold(loggingFlushCountTrigger))
}

func getLogCtxVal(aeInfo AppengineInfo, hreq *http.Request, logger *logging.Logger, trace string) *loggingCtxValue {
	var remoteIp string
	if addr := hreq.Header.Get(IPListHeader); addr != "" {
		remoteIp = strings.Split(addr, ",")[0]
	}

	labels := map[string]string{
		"appengine.googleapis.com/instance_name": aeInfo.InstanceID(),
		"pendo_io_node_name":                     aeInfo.NodeName(),
		"pendo_io_service":                       aeInfo.ModuleName(),
		"pendo_io_version":                       aeInfo.VersionID(),
		"pendo_io_request_host":                  hreq.Host,
		"pendo_io_request_method":                hreq.Method,
		"pendo_io_request_url":                   hreq.URL.String(),
		"pendo_io_remote_ip":                     remoteIp,
		"pendo_io_useragent":                     hreq.UserAgent(),
	}

	tlsVersionHeader, tlsCipherSuiteHeader := hreq.Header.Get("X-Client-TLS-Version"), hreq.Header.Get("X-Client-Cipher-Suite")
	if tlsVersionHeader != "" {
		labels["pendo_io_client_tls_version"] = tlsVersionHeader
	}
	if tlsCipherSuiteHeader != "" {
		labels["pendo_io_client_cipher_suite"] = tlsCipherSuiteHeader
	}

	return &loggingCtxValue{
		aeInfo: aeInfo,
		hreq:   hreq,
		labels: labels,
		logger: logger,
		trace:  trace,
	}
}

// for use in flex services with long-running tasks that don't handle http requests
func WrapBackgroundContextWithStackdriverLogger(c context.Context, logName string) context.Context {
	if IsDevAppServer {
		return c
	}
	return wrapBackgroundContextWithStackdriverLogger(c, logName, GetOrCreateLoggingClient())
}

func wrapBackgroundContextWithStackdriverLogger(c context.Context, logName string, lc *logging.Client) context.Context {
	aeInfo := NewAppengineInfoFromContext(c)

	project := aeInfo.NativeProjectID()
	if project == "" {
		panic("aelog: no GCP project set in environment")
	}
	parent := "projects/" + project
	if logName == "" {
		logName = ChildLogName
	}
	req, err := http.NewRequest(http.MethodGet, "pendo.io/background", bytes.NewReader([]byte{}))
	if err != nil {
		panic(err)
	}

	return context.WithValue(c, loggingCtxKey, getLogCtxVal(aeInfo, req, getLogger(aeInfo, lc, logName), parent+"/traces/"+fmt.Sprintf("%d", rand.Int63())))
}

func WrapBackgroundContextWithStackdriverLoggerWithCloseFunc(c context.Context, logName string) (context.Context, func()) {
	if IsDevAppServer {
		return c, func() {}
	}

	aeInfo := NewAppengineInfoFromContext(c)
	client, err := logging.NewClient(c, fmt.Sprintf("projects/%s", aeInfo.NativeProjectID()))
	if err != nil {
		panic(fmt.Sprintf("failed to create logging client %s", err.Error()))
	}

	ctx := wrapBackgroundContextWithStackdriverLogger(c, logName, client)

	return ctx, func() {
		_ = client.Close()
	}
}

var sharedClientCtxKey = struct{ k string }{"shared client context key"}

type sharedClientLogCtxVal struct {
	aeInfo       AppengineInfo
	client       *logging.Client
	logger       *logging.Logger
	parent       string
	parentLogger *logging.Logger
}

func AddSharedLogClientToBackgroundContext(c context.Context, logName string) context.Context {
	if IsDevAppServer {
		return c
	}

	aeInfo := NewAppengineInfoFromContext(c)
	project := aeInfo.NativeProjectID()
	if project == "" {
		panic("aelog: no GCP project set in environment")
	}
	parent := "projects/" + project
	lc := GetOrCreateLoggingClient()

	if logName == "" {
		logName = ChildLogName
	}

	logger := getLogger(aeInfo, lc, logName)
	parentLogger := getLogger(aeInfo, lc, requestLogPath)

	return context.WithValue(c, sharedClientCtxKey, &sharedClientLogCtxVal{
		aeInfo:       aeInfo,
		client:       lc,
		logger:       logger,
		parent:       parent,
		parentLogger: parentLogger,
	})
}

func RunFuncWithDedicatedLogger(c context.Context, simulatedUrl, traceId string, fn func(log Logging)) {
	ctxVal := c.Value(sharedClientCtxKey)
	if ctxVal == nil {
		panic("must wrap context with AddSharedLogClientToBackgroundContext")
	}

	req, err := http.NewRequest(http.MethodGet, simulatedUrl, nil)
	if err != nil {
		panic(err)
	}

	sharedClientCtxVal := ctxVal.(*sharedClientLogCtxVal)

	if traceId == "" {
		traceId = strconv.FormatInt(rand.Int63(), 10)
	}

	logCtxVal := getLogCtxVal(sharedClientCtxVal.aeInfo, req, sharedClientCtxVal.logger, sharedClientCtxVal.parent+"/traces/"+traceId)
	fctx := context.WithValue(c, loggingCtxKey, logCtxVal)

	dedicatedLogger := NewStackdriverLogging(fctx)

	start := time.Now()

	defer func() {
		sev := logging.Severity(logCtxVal.sev)
		status := http.StatusOK
		if sev >= logging.Error {
			status = http.StatusInternalServerError
		}
		sharedClientCtxVal.parentLogger.Log(logging.Entry{
			HTTPRequest: &logging.HTTPRequest{
				Latency:      time.Now().Sub(start),
				ResponseSize: 0,
				Request:      req,
				Status:       status,
			},
			Labels:    logCtxVal.getLabels(),
			Severity:  sev,
			Timestamp: start,
			Trace:     logCtxVal.trace,
		})
	}()

	fn(dedicatedLogger)
}

func WrapHandlerWithStackdriverLogger(h http.Handler, logName string, opts ...option.ClientOption) http.Handler {
	if IsDevAppServer {
		return h
	}

	ctx := context.Background()
	aeInfo := NewAppengineInfoFromContext(ctx)

	project := aeInfo.NativeProjectID()
	if project == "" {
		panic("aelog: no GCP project set in environment")
	}
	parent := "projects/" + project

	lc := GetOrCreateLoggingClient()
	if logName == "" {
		logName = ChildLogName
	}

	logger := getLogger(aeInfo, lc, logName)

	parentLogger := getLogger(aeInfo, lc, requestLogPath)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		logCtxVal := getLogCtxVal(aeInfo, r, logger, "")
		traceHeader := r.Header.Get("X-Cloud-Trace-Context")
		if traceHeader != "" {
			logCtxVal.trace = parent + "/traces/" + strings.Split(traceHeader, "/")[0]
		} else {
			logCtxVal.trace = parent + "/traces/" + fmt.Sprintf("%d", rand.Int63())
		}
		ctx := context.WithValue(r.Context(), loggingCtxKey, logCtxVal)
		sw := &statusWriter{
			ResponseWriter: w,
			status:         http.StatusOK, // default response if we don't explicitly set one
		}
		h.ServeHTTP(sw, r.WithContext(ctx))

		e := logging.Entry{
			HTTPRequest: &logging.HTTPRequest{
				Latency:      time.Now().Sub(start),
				ResponseSize: int64(sw.length),
				Request:      r,
				Status:       sw.status,
			},
			Labels:    logCtxVal.getLabels(),
			Severity:  logging.Severity(logCtxVal.sev),
			Timestamp: start,
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
		Labels:    logCtxVal.getLabels(),
		Payload:   truncateLog(format, args...),
		Severity:  logging.Severity(sev),
		Timestamp: time.Now(),
		Trace:     logCtxVal.trace,
	}
	logCtxVal.logger.Log(e)
	if sev > logCtxVal.sev {
		logCtxVal.sev = sev
	}
}

// set to 240k, slightly under real max of 256k
const maxLogLength = 240 << 10
const truncatedLogPrefix = "TRUNCATED: (full log in stderr) "

func truncateLog(format string, args ...interface{}) string {
	payload := fmt.Sprintf(format, args...)
	if len(payload) > maxLogLength {
		_, _ = fmt.Fprintln(os.Stderr, payload)
		payload = truncatedLogPrefix + payload[:maxLogLength]
	}
	return strings.ToValidUTF8(payload, "\uFFFD")
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
