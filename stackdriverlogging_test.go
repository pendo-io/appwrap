package appwrap

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"github.com/stretchr/testify/mock"
	. "gopkg.in/check.v1"
)

type StackdriverLoggingTests struct {
	appInfoMock *AppengineInfoMock
	clientMock  *ClientMock
	sl          *StackdriverLoggingService
	ctx         context.Context
}

var _ = Suite(&StackdriverLoggingTests{})

func (s *StackdriverLoggingTests) SetUpTest(c *C) {
	s.ctx = StubContext()
	s.clientMock = &ClientMock{}
	s.appInfoMock = &AppengineInfoMock{}

	s.clientMock.On("SetUpOnError").Return().Once()
	s.appInfoMock.On("ModuleName").Return("my-module").Once()
	s.appInfoMock.On("AppID").Return("my-project").Once()
	s.appInfoMock.On("VersionID").Return("my-version").Once()

	s.sl = newStackdriverLoggingService(s.clientMock, s.appInfoMock, NullLogger{}).(*StackdriverLoggingService)
}

func assertParentLogEntry(c *C, log *StackdriverLogging, entry logging.Entry, severity logging.Severity, startTime time.Time) {
	assertLogEntry(c, entry, severity, startTime)

	c.Assert(entry.Payload, IsNil)
	c.Assert(entry.HTTPRequest.Latency <= time.Now().Sub(log.start), IsTrue)
	c.Assert(entry.HTTPRequest.Request, DeepEquals, log.request)
}

func assertChildLogEntry(c *C, entry logging.Entry, severity logging.Severity, startTime time.Time, payload interface{}) {
	assertLogEntry(c, entry, severity, startTime)
	c.Assert(entry.Payload, DeepEquals, payload)
}

func assertLogEntry(c *C, entry logging.Entry, severity logging.Severity, startTime time.Time) {
	c.Assert(entry.Labels, HasLen, 1)
	c.Assert(entry.Labels["subscriptionId"], Equals, "12345")
	c.Assert(entry.Severity, Equals, severity)
	c.Assert(entry.Timestamp.After(startTime), IsTrue)
	c.Assert(entry.Timestamp.Before(time.Now()), IsTrue)
	c.Assert(entry.Trace, Equals, "id-to-connect-logs")
}

func (s *StackdriverLoggingTests) arrangeValidClient(r *http.Request) DataLogging {
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}
	s.clientMock.On("Logger", "pendo_test", mock.Anything).Return(&LoggerMock{}).Once()
	s.clientMock.On("Logger", requestLogPath, mock.Anything).Return(&LoggerMock{}).Once()
	return s.sl.newStackdriverLogging(map[string]string{}, LogName("pendo_test"), r, "")
}

func (s *StackdriverLoggingTests) TestLogSetup(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}

	s.clientMock.On("Logger", "pendo_test", mock.Anything).Return(LoggerMock{}).Once()
	s.clientMock.On("Logger", requestLogPath, mock.Anything).Return(LoggerMock{}).Once()

	log := s.sl.CreateLog(map[string]string{"test": "this"}, LogName("pendo_test"), r, "").(*StackdriverLogging)

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.maxSeverity, Equals, logging.Default)
	c.Assert(log.logName, Equals, LogName("pendo_test"))
	c.Assert(log.traceContext, Equals, "id-to-connect-logs")
	c.Assert(log.request, DeepEquals, r)
}

func (s *StackdriverLoggingTests) TestLogSetupNoAppEngineTrace(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))

	s.clientMock.On("Logger", "pendo_test", mock.Anything).Return(LoggerMock{}).Once()
	s.clientMock.On("Logger", requestLogPath, mock.Anything).Return(LoggerMock{}).Once()
	log := s.sl.CreateLog(map[string]string{"test": "this"}, LogName("pendo_test"), r, "custom-trace-id").(*StackdriverLogging)

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.maxSeverity, Equals, logging.Default)
	c.Assert(log.logName, Equals, LogName("pendo_test"))
	c.Assert(log.traceContext, Equals, "custom-trace-id")
	c.Assert(log.request, DeepEquals, r)
}

func (s *StackdriverLoggingTests) TestLogSetupRequestNil(c *C) {
	s.clientMock.On("Logger", "pendo_test", mock.Anything).Return(LoggerMock{}).Once()
	s.clientMock.On("Logger", requestLogPath, mock.Anything).Return(LoggerMock{}).Once()
	log := s.sl.CreateLog(map[string]string{"test": "this"}, LogName("pendo_test"), nil, "custom-trace-id").(*StackdriverLogging)

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.maxSeverity, Equals, logging.Default)
	c.Assert(log.logName, Equals, LogName("pendo_test"))
	c.Assert(log.traceContext, Equals, "custom-trace-id")
	c.Assert(log.request, IsNil)
}

func (s *StackdriverLoggingTests) TestAddCommonLabel(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := s.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
}

func (s *StackdriverLoggingTests) TestAddSameLabelTwice(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := s.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")
	log.AddLabel("test", "this")

	c.Assert(log.commonLabels, HasLen, 1)
	c.Assert(log.commonLabels["test"], Equals, "this")
}

func (s *StackdriverLoggingTests) TestAddTwoLabel(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := s.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")
	log.AddLabel("test2", "this2")

	c.Assert(log.commonLabels, HasLen, 2)
	c.Assert(log.commonLabels["test"], Equals, "this")
	c.Assert(log.commonLabels["test2"], Equals, "this2")
}

func (s *StackdriverLoggingTests) TestRemoveLabel(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := s.arrangeValidClient(r).(*StackdriverLogging)

	log.AddLabel("test", "this")
	log.RemoveLabel("test")

	c.Assert(log.commonLabels, HasLen, 0)
}

func (s *StackdriverLoggingTests) TestRemoveLabelNoValidKey(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := s.arrangeValidClient(r).(*StackdriverLogging)
	log.RemoveLabel("test")

	c.Assert(log.commonLabels, HasLen, 0)
}

func (s *StackdriverLoggingTests) TestLogStringFormatFunctions(c *C) {

	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := s.arrangeValidClient(r).(*StackdriverLogging)
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
		log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			c.Assert(entry.Labels, HasLen, 1)
			c.Assert(entry.Labels["subscriptionId"], Equals, "12345")
			c.Assert(entry.Payload, Equals, fmt.Sprintf("This is a test %d, %s, and %d", 1, "two", 3))
			c.Assert(entry.Severity, Equals, testCase.severity)
			c.Assert(entry.Timestamp.After(startTime), IsTrue)
			c.Assert(entry.Timestamp.Before(time.Now()), IsTrue)
			c.Assert(entry.Trace, Equals, "id-to-connect-logs")
			return true
		})).Once()
		testCase.logf("This is a test %d, %s, and %d", 1, "two", 3)
		log.childLogger.(*LoggerMock).AssertExpectations(c)
	}

}

type testObject struct {
	MaxLimit int
}

func (s *StackdriverLoggingTests) TestLogDataFunctions(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	log := s.arrangeValidClient(r).(*StackdriverLogging)
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
		log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			c.Assert(entry.Labels, HasLen, 1)
			c.Assert(entry.Labels["subscriptionId"], Equals, "12345")
			c.Assert(entry.Payload, DeepEquals, testCase.data)
			c.Assert(entry.Severity, Equals, testCase.severity)
			c.Assert(entry.Timestamp.After(startTime), IsTrue)
			c.Assert(entry.Timestamp.Before(time.Now()), IsTrue)
			c.Assert(entry.Trace, Equals, "id-to-connect-logs")
			return true
		})).Once()
		testCase.logDataFunc(testCase.data)
		log.childLogger.(*LoggerMock).AssertExpectations(c)
	}
}

func (s *StackdriverLoggingTests) TestCloseNoRequest(c *C) {
	w := httptest.NewRecorder()
	s.clientMock.On("Logger", "pendo_test", mock.Anything).Return(&LoggerMock{}).Once()
	s.clientMock.On("Logger", requestLogPath, mock.Anything).Return(&LoggerMock{}).Once()
	log := s.sl.CreateLog(map[string]string{"test": "this"}, LogName("pendo_test"), nil, "custom-trace-id").(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")

	// without a request, we shouldn't log any parent messages
	log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
	log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
	log.Close(w)
	log.parentLogger.(*LoggerMock).AssertExpectations(c)
	log.childLogger.(*LoggerMock).AssertExpectations(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelDefault(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	w := httptest.NewRecorder()
	log := s.arrangeValidClient(r).(*StackdriverLogging)
	log.AddLabel("subscriptionId", "12345")
	r.Response = &http.Response{}
	startTime := time.Now()
	log.parentLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
		assertParentLogEntry(c, log, entry, logging.Default, startTime)
		return true
	})).Once()
	log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
	log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
	log.Close(w)
	log.parentLogger.(*LoggerMock).AssertExpectations(c)
	log.childLogger.(*LoggerMock).AssertExpectations(c)
}

func (s *StackdriverLoggingTests) TestParentLogLevelNonDefault(c *C) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Response = &http.Response{}
	payload := testObject{300}
	testCases := []struct {
		log      *StackdriverLogging
		severity logging.Severity
		logFunc  func(interface{})
	}{
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Debug, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Info, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Warning, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Error, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Critical, nil},
	}
	testCases[0].logFunc = testCases[0].log.Debug
	testCases[0].log.AddLabel("subscriptionId", "12345")
	testCases[1].logFunc = testCases[1].log.Info
	testCases[1].log.AddLabel("subscriptionId", "12345")
	testCases[2].logFunc = testCases[2].log.Warning
	testCases[2].log.AddLabel("subscriptionId", "12345")
	testCases[3].logFunc = testCases[3].log.Error
	testCases[3].log.AddLabel("subscriptionId", "12345")
	testCases[4].logFunc = testCases[4].log.Critical
	testCases[4].log.AddLabel("subscriptionId", "12345")

	for _, testCase := range testCases {
		startTime := time.Now()
		w := httptest.NewRecorder()
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severity, startTime, payload)
			return true
		})).Once()
		testCase.logFunc(payload)
		testCase.log.parentLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertParentLogEntry(c, testCase.log, entry, testCase.severity, startTime)
			return true
		})).Once()
		testCase.log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.Close(w)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.childLogger.(*LoggerMock).AssertExpectations(c)
	}

	formatTestCases := []struct {
		log      *StackdriverLogging
		severity logging.Severity
		logFunc  func(string, ...interface{})
	}{
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Debug, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Info, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Warning, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Error, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Critical, nil},
	}
	formatTestCases[0].logFunc = formatTestCases[0].log.Debugf
	formatTestCases[0].log.AddLabel("subscriptionId", "12345")
	formatTestCases[1].logFunc = formatTestCases[1].log.Infof
	formatTestCases[1].log.AddLabel("subscriptionId", "12345")
	formatTestCases[2].logFunc = formatTestCases[2].log.Warningf
	formatTestCases[2].log.AddLabel("subscriptionId", "12345")
	formatTestCases[3].logFunc = formatTestCases[3].log.Errorf
	formatTestCases[3].log.AddLabel("subscriptionId", "12345")
	formatTestCases[4].logFunc = formatTestCases[4].log.Criticalf
	formatTestCases[4].log.AddLabel("subscriptionId", "12345")
	for _, testCase := range formatTestCases {
		startTime := time.Now()
		w := httptest.NewRecorder()
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severity, startTime, fmt.Sprintf("%v", payload))
			return true
		})).Once()
		testCase.logFunc("%v", payload)
		testCase.log.parentLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertParentLogEntry(c, testCase.log, entry, testCase.severity, startTime)
			return true
		})).Once()
		testCase.log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.Close(w)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.childLogger.(*LoggerMock).AssertExpectations(c)
	}
}

func (s *StackdriverLoggingTests) TestParentLogLevelHighOverrideLow(c *C) {
	c.Skip("broken")
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Response = &http.Response{}
	payload := testObject{300}
	testCases := []struct {
		log          *StackdriverLogging
		severityLow  logging.Severity
		severityHigh logging.Severity
		logFuncLow   func(interface{})
		logFuncHigh  func(interface{})
	}{
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Debug, logging.Info, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Info, logging.Warning, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Warning, logging.Error, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Error, logging.Critical, nil, nil},
	}
	testCases[0].logFuncLow = testCases[0].log.Debug
	testCases[0].logFuncHigh = testCases[0].log.Info
	testCases[0].log.AddLabel("subscriptionId", "12345")
	testCases[1].logFuncLow = testCases[1].log.Info
	testCases[1].logFuncHigh = testCases[1].log.Warning
	testCases[1].log.AddLabel("subscriptionId", "12345")
	testCases[2].logFuncLow = testCases[2].log.Warning
	testCases[2].logFuncHigh = testCases[2].log.Error
	testCases[2].log.AddLabel("subscriptionId", "12345")
	testCases[3].logFuncLow = testCases[3].log.Error
	testCases[3].logFuncHigh = testCases[3].log.Critical
	testCases[3].log.AddLabel("subscriptionId", "12345")

	for _, testCase := range testCases {
		startTime := time.Now()
		w := httptest.NewRecorder()
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityLow, startTime, payload)
			return true
		})).Once()
		testCase.logFuncLow(payload)
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityHigh, startTime, payload)
			return true
		})).Once()
		testCase.logFuncHigh(payload)
		testCase.log.parentLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertParentLogEntry(c, testCase.log, entry, testCase.severityHigh, startTime)
			return true
		})).Once()
		testCase.log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.Close(w)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.childLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
	}

	formatTestCases := []struct {
		log          *StackdriverLogging
		severityLow  logging.Severity
		severityHigh logging.Severity
		logFuncLow   func(string, ...interface{})
		logFuncHigh  func(string, ...interface{})
	}{
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Debug, logging.Info, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Info, logging.Warning, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Warning, logging.Error, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Error, logging.Critical, nil, nil},
	}
	formatTestCases[0].logFuncLow = formatTestCases[0].log.Debugf
	formatTestCases[0].logFuncHigh = formatTestCases[0].log.Infof
	formatTestCases[0].log.AddLabel("subscriptionId", "12345")
	formatTestCases[1].logFuncLow = formatTestCases[1].log.Infof
	formatTestCases[1].logFuncHigh = formatTestCases[1].log.Warningf
	formatTestCases[1].log.AddLabel("subscriptionId", "12345")
	formatTestCases[2].logFuncLow = formatTestCases[2].log.Warningf
	formatTestCases[2].logFuncHigh = formatTestCases[2].log.Errorf
	formatTestCases[2].log.AddLabel("subscriptionId", "12345")
	formatTestCases[3].logFuncLow = formatTestCases[3].log.Errorf
	formatTestCases[3].logFuncHigh = formatTestCases[3].log.Criticalf
	formatTestCases[3].log.AddLabel("subscriptionId", "12345")

	for _, testCase := range formatTestCases {
		startTime := time.Now()
		w := httptest.NewRecorder()
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityLow, startTime, fmt.Sprintf("%v", payload))
			return true
		})).Once()
		testCase.logFuncLow("%v", payload)
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityHigh, startTime, fmt.Sprintf("%v", payload))
			return true
		})).Once()
		testCase.logFuncHigh("%v", payload)
		testCase.log.parentLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertParentLogEntry(c, testCase.log, entry, testCase.severityHigh, startTime)
			return true
		})).Once()
		testCase.log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.Close(w)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.childLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
	}
}

func (s *StackdriverLoggingTests) TestParentLogLevelLowNotOverrideHigh(c *C) {
	c.Skip("broken")
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Response = &http.Response{}
	payload := testObject{300}
	testCases := []struct {
		log          *StackdriverLogging
		severityLow  logging.Severity
		severityHigh logging.Severity
		logFuncLow   func(interface{})
		logFuncHigh  func(interface{})
	}{
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Debug, logging.Info, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Info, logging.Warning, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Warning, logging.Error, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Error, logging.Critical, nil, nil},
	}
	testCases[0].logFuncLow = testCases[0].log.Debug
	testCases[0].logFuncHigh = testCases[0].log.Info
	testCases[0].log.AddLabel("subscriptionId", "12345")
	testCases[1].logFuncLow = testCases[1].log.Info
	testCases[1].logFuncHigh = testCases[1].log.Warning
	testCases[1].log.AddLabel("subscriptionId", "12345")
	testCases[2].logFuncLow = testCases[2].log.Warning
	testCases[2].logFuncHigh = testCases[2].log.Error
	testCases[2].log.AddLabel("subscriptionId", "12345")
	testCases[3].logFuncLow = testCases[3].log.Error
	testCases[3].logFuncHigh = testCases[3].log.Critical
	testCases[3].log.AddLabel("subscriptionId", "12345")

	for _, testCase := range testCases {
		startTime := time.Now()
		w := httptest.NewRecorder()
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityHigh, startTime, payload)
			return true
		})).Once()
		testCase.logFuncHigh(payload)
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityLow, startTime, payload)
			return true
		})).Once()
		testCase.logFuncLow(payload)
		testCase.log.parentLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertParentLogEntry(c, testCase.log, entry, testCase.severityHigh, startTime)
			return true
		})).Once()
		testCase.log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.Close(w)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.childLogger.(*LoggerMock).AssertExpectations(c)
	}

	formatTestCases := []struct {
		log          *StackdriverLogging
		severityLow  logging.Severity
		severityHigh logging.Severity
		logFuncLow   func(string, ...interface{})
		logFuncHigh  func(string, ...interface{})
	}{
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Debug, logging.Info, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Info, logging.Warning, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Warning, logging.Error, nil, nil},
		{s.arrangeValidClient(r).(*StackdriverLogging), logging.Error, logging.Critical, nil, nil},
	}
	formatTestCases[0].logFuncLow = formatTestCases[0].log.Debugf
	formatTestCases[0].logFuncHigh = formatTestCases[0].log.Infof
	formatTestCases[0].log.AddLabel("subscriptionId", "12345")
	formatTestCases[1].logFuncLow = formatTestCases[1].log.Infof
	formatTestCases[1].logFuncHigh = formatTestCases[1].log.Warningf
	formatTestCases[1].log.AddLabel("subscriptionId", "12345")
	formatTestCases[2].logFuncLow = formatTestCases[2].log.Warningf
	formatTestCases[2].logFuncHigh = formatTestCases[2].log.Errorf
	formatTestCases[2].log.AddLabel("subscriptionId", "12345")
	formatTestCases[3].logFuncLow = formatTestCases[3].log.Errorf
	formatTestCases[3].logFuncHigh = formatTestCases[3].log.Criticalf
	formatTestCases[3].log.AddLabel("subscriptionId", "12345")

	for _, testCase := range formatTestCases {
		startTime := time.Now()
		w := httptest.NewRecorder()
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityHigh, startTime, fmt.Sprintf("%v", payload))
			return true
		})).Once()
		testCase.logFuncHigh("%v", payload)
		testCase.log.childLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertChildLogEntry(c, entry, testCase.severityLow, startTime, fmt.Sprintf("%v", payload))
			return true
		})).Once()
		testCase.logFuncLow("%v", payload)
		testCase.log.parentLogger.(*LoggerMock).On("Log", mock.MatchedBy(func(entry logging.Entry) bool {
			assertParentLogEntry(c, testCase.log, entry, testCase.severityHigh, startTime)
			return true
		})).Once()
		testCase.log.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
		testCase.log.Close(w)
		testCase.log.parentLogger.(*LoggerMock).AssertExpectations(c)
		testCase.log.childLogger.(*LoggerMock).AssertExpectations(c)
	}
}
