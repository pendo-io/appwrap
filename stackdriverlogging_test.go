package appwrap

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"github.com/pendo-io/appwrap"
	. "gopkg.in/check.v1"
)

type StackdriverLoggingFixture struct {
	logCh chan LogMessage
}

type StackdriverLoggingTests struct {
	ctx context.Context
}

var _ = Suite(&StackdriverLoggingTests{})

func (s *StackdriverLoggingTests) SetUpTest(c *C) {
	s.ctx = appwrap.StubContext()
}

func (s *StackdriverLoggingTests) SetUpFixture(c *C) StackdriverLoggingFixture {
	return StackdriverLoggingFixture{
		logCh: make(chan LogMessage),
	}
}

func (f *StackdriverLoggingFixture) assertMocks(c *C) {

}

func (f *StackdriverLoggingFixture) assertParentLogEntry(c *C, log *StackdriverLogging, logMessage LogMessage, severity logging.Severity, startTime time.Time) {
	expectedLabels := logging.CommonLabels(map[string]string{
		traceIdPath: log.traceContext,
	})
	c.Assert(logMessage.LogName, Equals, LogName(requestLogPath))
	c.Assert(logMessage.CommonLabels, DeepEquals, expectedLabels)

	entry := logMessage.Entry
	c.Assert(entry.Labels, HasLen, 1)
	c.Assert(entry.Labels["subscriptionId"], Equals, "12345")
	c.Assert(entry.Payload, IsNil)
	c.Assert(entry.Severity, Equals, severity)
	c.Assert(entry.Timestamp.After(startTime), IsTrue)
	c.Assert(entry.Timestamp.Before(time.Now()), IsTrue)
	c.Assert(entry.Trace, Equals, "id-to-connect-logs")

	c.Assert(entry.HTTPRequest.Latency <= time.Now().Sub(log.start), IsTrue)
	c.Assert(entry.HTTPRequest.Request, DeepEquals, log.request)
}

func (f *StackdriverLoggingFixture) arrangeValidClient(r *http.Request) DataLogging {
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}
	return NewStackdriverLogging(map[string]string{}, LogName("pendo_test"), f.logCh, r, "")
}

func (s *StackdriverLoggingTests) TestLogSetup(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}

	log := NewStackdriverLogging(map[string]string{"test": "this"}, LogName("pendo_test"), f.logCh, r, "").(*StackdriverLogging)

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.maxSeverity, Equals, logging.Default)
	c.Assert(log.logName, Equals, LogName("pendo_test"))
	c.Assert(log.traceContext, Equals, "id-to-connect-logs")
	c.Assert(log.request, DeepEquals, r)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestLogSetupNoAppEngineTrace(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))

	log := NewStackdriverLogging(map[string]string{"test": "this"}, LogName("pendo_test"), f.logCh, r, "custom-trace-id").(*StackdriverLogging)

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.maxSeverity, Equals, logging.Default)
	c.Assert(log.logName, Equals, LogName("pendo_test"))
	c.Assert(log.traceContext, Equals, "custom-trace-id")
	c.Assert(log.request, DeepEquals, r)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestLogSetupRequestNil(c *C) {
	f := s.SetUpFixture(c)

	log := NewStackdriverLogging(map[string]string{"test": "this"}, LogName("pendo_test"), f.logCh, nil, "custom-trace-id").(*StackdriverLogging)

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.maxSeverity, Equals, logging.Default)
	c.Assert(log.logName, Equals, LogName("pendo_test"))
	c.Assert(log.traceContext, Equals, "custom-trace-id")
	c.Assert(log.request, IsNil)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestAddCommonLabel(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := f.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestAddSameLabelTwice(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := f.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")
	log.AddLabel("test", "this")

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestAddTwoLabel(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := f.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")
	log.AddLabel("test2", "this2")

	c.Assert(log.commonLabels, HasLen, 2)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.commonLabels["test2"], Equals, "this2")
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestRemoveLabel(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := f.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")
	log.RemoveLabel("test")

	c.Assert(log.commonLabels, HasLen, 0)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestRemoveLabelNoValidKey(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.RemoveLabel("test")

	c.Assert(log.commonLabels, HasLen, 0)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestLogStringFormatFunctions(c *C) {
	f := s.SetUpFixture(c)

	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	testCases := []struct {
		severity logging.Severity
		logf     func(format string, args ...interface{})
	}{
		{logging.Critical, log.Criticalf},
		{logging.Error, log.Errorf},
		{logging.Info, log.Infof},
		{logging.Warning, log.Warningf},
		{logging.Debug, log.Debugf},
	}

	for _, testCase := range testCases {
		startTime := time.Now()

		testCase.logf("This is a test %d, %s, and %d", 1, "two", 3)

		logMessage := <-f.logCh
		expectedLabels := logging.CommonLabels(map[string]string{
			traceIdPath: log.traceContext,
		})
		c.Assert(logMessage.LogName, Equals, LogName("pendo_test"))
		c.Assert(logMessage.CommonLabels, DeepEquals, expectedLabels)

		entry := logMessage.Entry
		c.Assert(entry.Labels, HasLen, 1)
		c.Assert(entry.Labels["subscriptionId"], Equals, "12345")
		c.Assert(entry.Payload, Equals, fmt.Sprintf("This is a test %d, %s, and %d", 1, "two", 3))
		c.Assert(entry.Severity, Equals, testCase.severity)
		c.Assert(entry.Timestamp.After(startTime), IsTrue)
		c.Assert(entry.Timestamp.Before(time.Now()), IsTrue)
		c.Assert(entry.Trace, Equals, "id-to-connect-logs")
	}

	f.assertMocks(c)
}

type testObject struct {
	MaxLimit int
}

func (s *StackdriverLoggingTests) TestLogDataFunctions(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	testCases := []struct {
		severity    logging.Severity
		data        interface{}
		logDataFunc func(data interface{})
	}{
		{logging.Info, "Test string", log.Info},
		{logging.Debug, "Test string", log.Debug},
		{logging.Warning, "Test string", log.Warning},
		{logging.Error, "Test string", log.Error},
		{logging.Critical, "Test string", log.Critical},
		{logging.Info, `{"test": "body json"}`, log.Info},
		{logging.Debug, `{"test": "body json"}`, log.Debug},
		{logging.Warning, `{"test": "body json"}`, log.Warning},
		{logging.Error, `{"test": "body json"}`, log.Error},
		{logging.Critical, `{"test": "body json"}`, log.Critical},
		{logging.Info, testObject{200}, log.Info},
		{logging.Debug, testObject{300}, log.Debug},
		{logging.Warning, testObject{400}, log.Warning},
		{logging.Error, testObject{500}, log.Error},
		{logging.Critical, testObject{600}, log.Critical},
	}
	for _, testCase := range testCases {
		startTime := time.Now()
		testCase.logDataFunc(testCase.data)

		logMessage := <-f.logCh
		expectedLabels := logging.CommonLabels(map[string]string{
			traceIdPath: log.traceContext,
		})
		c.Assert(logMessage.LogName, Equals, LogName("pendo_test"))
		c.Assert(logMessage.CommonLabels, DeepEquals, expectedLabels)

		entry := logMessage.Entry
		c.Assert(entry.Labels, HasLen, 1)
		c.Assert(entry.Labels["subscriptionId"], Equals, "12345")
		c.Assert(entry.Payload, DeepEquals, testCase.data)
		c.Assert(entry.Severity, Equals, testCase.severity)
		c.Assert(entry.Timestamp.After(startTime), IsTrue)
		c.Assert(entry.Timestamp.Before(time.Now()), IsTrue)
		c.Assert(entry.Trace, Equals, "id-to-connect-logs")
	}

	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestCloseNoRequest(c *C) {
	f := s.SetUpFixture(c)
	w := httptest.NewRecorder()
	log := NewStackdriverLogging(map[string]string{"test": "this"}, LogName("pendo_test"), f.logCh, nil, "custom-trace-id").(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	doneCh := make(chan (bool))
	var logs []LogMessage
	go func() {
		select {
		case logMessage, ok := <-f.logCh:
			if ok {
				logs = append(logs, logMessage)
			} else {
				doneCh <- true
				return
			}
		}
	}()

	log.Close(w)
	close(f.logCh)
	<-doneCh

	c.Assert(logs, HasLen, 0)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelDefault(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}
	startTime := time.Now()
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Default, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelDebug(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Debug(testObject{300})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Debug, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelInfo(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Info(testObject{300})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Info, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelWarning(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Warning(testObject{300})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Warning, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelError(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Error(testObject{300})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Error, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelCritical(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Critical(testObject{300})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Critical, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelDebugToInfo(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Debug(testObject{300})
	<-f.logCh

	log.Info(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Info, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelInfoToWarning(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Info(testObject{300})
	<-f.logCh

	log.Warning(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Warning, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelWarningToError(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Warning(testObject{300})
	<-f.logCh

	log.Error(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Error, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelErrorToCritical(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Error(testObject{300})
	<-f.logCh

	log.Critical(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Critical, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelInfoDebug(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Info(testObject{300})
	<-f.logCh

	log.Debug(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Info, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelWarningInfo(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Warning(testObject{300})
	<-f.logCh

	log.Info(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Warning, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelErrorWarning(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Error(testObject{300})
	<-f.logCh

	log.Warning(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Error, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelCriticalError(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Critical(testObject{300})
	<-f.logCh

	log.Error(testObject{299})
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Critical, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatDebug(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Debugf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Debug, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatInfo(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Infof("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Info, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatWarning(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Warningf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Warning, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatError(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Errorf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Error, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatCritical(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Criticalf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Critical, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelDebugToInfo(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	r.Response = &http.Response{}

	startTime := time.Now()
	log.Debugf("This is a test")
	<-f.logCh

	log.Infof("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Info, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelInfoToWarning(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Infof("This is a test")
	<-f.logCh

	log.Warningf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Warning, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelWarningToError(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Warningf("This is a test")
	<-f.logCh

	log.Errorf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Error, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelErrorToCritical(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Errorf("This is a test")
	<-f.logCh

	log.Criticalf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Critical, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelInfoDebug(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Infof("This is a test")
	<-f.logCh

	log.Debugf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Info, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelWarningInfo(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Warningf("This is a test")
	<-f.logCh

	log.Infof("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Warning, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelErrorWarning(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Errorf("This is a test")
	<-f.logCh

	log.Warningf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Error, startTime)
	f.assertMocks(c)
}

func (s *StackdriverLoggingTests) TestParentLogFormatLevelCriticalError(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := f.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}

	startTime := time.Now()
	log.Criticalf("This is a test")
	<-f.logCh

	log.Errorf("This is a test")
	<-f.logCh
	log.Close(w)
	logMessage := <-f.logCh

	f.assertParentLogEntry(c, log, logMessage, logging.Critical, startTime)
	f.assertMocks(c)
}
