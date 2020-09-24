package appwrap

import (
	"errors"
	"net/http"
	"sync"

	"cloud.google.com/go/errorreporting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	. "gopkg.in/check.v1"
)

type ErrorReportingTest struct{}

var _ = Suite(&ErrorReportingTest{})

type googleErrorReportingClientMock struct {
	mock.Mock
}

func (g *googleErrorReportingClientMock) Report(entry errorreporting.Entry) {
	g.Called(entry)
}

func (g *googleErrorReportingClientMock) Flush() {
	g.Called()
}

func (g *googleErrorReportingClientMock) Close() error {
	args := g.Called()
	return args.Error(0)
}

func (e *ErrorReportingTest) TestReport(c *C) {
	client := &googleErrorReportingClientMock{}
	reporter := &googleErrorReporter{client: client}

	err := errors.New("some error")
	req := &http.Request{}
	key := "pendo.io/sub_name"

	testReport := ErrorReport{
		Err:             err,
		Req:             req,
		ErrorAffectsKey: key,
	}
	client.On("Report", errorreporting.Entry{
		Error: err,
		Req:   req,
		User:  key,
	})

	reporter.Report(testReport)

	client.AssertExpectations(c)
}

func (e *ErrorReportingTest) TestFlush(c *C) {
	client := &googleErrorReportingClientMock{}
	reporter := &googleErrorReporter{client: client}

	client.On("Flush")

	reporter.FlushReports()

	client.AssertExpectations(c)
}

func (e *ErrorReportingTest) TestWrapLogger_CantWrapAgain(c *C) {
	log := &errorForwardingLogger{}
	reporter := &googleErrorReporter{}

	assert.Panics(c, func() {
		_ = reporter.WrapLogger(log, "")
	})
}

func (e *ErrorReportingTest) TestDebugf_OnlyForwards(c *C) {
	client := &googleErrorReportingClientMock{}
	reporter := &googleErrorReporter{client: client}
	wrapMe := &LoggingMock{Log: &NullLogger{}}

	wrapMe.On("Debugf", "winner, winner, chicken din%s", "ner")

	wrappedLog := reporter.WrapLogger(wrapMe, "")

	wrappedLog.Debugf("winner, winner, chicken din%s", "ner")

	client.AssertExpectations(c)
	wrapMe.AssertExpectations(c)
}

func (e *ErrorReportingTest) TestInfof_OnlyForwards(c *C) {
	client := &googleErrorReportingClientMock{}
	reporter := &googleErrorReporter{client: client}
	wrapMe := &LoggingMock{Log: &NullLogger{}}

	wrapMe.On("Infof", "winner, winner, chicken din%s", "ner")

	wrappedLog := reporter.WrapLogger(wrapMe, "")

	wrappedLog.Infof("winner, winner, chicken din%s", "ner")

	client.AssertExpectations(c)
	wrapMe.AssertExpectations(c)
}

func (e *ErrorReportingTest) TestWarningf_OnlyForwards(c *C) {
	client := &googleErrorReportingClientMock{}
	reporter := &googleErrorReporter{client: client}
	wrapMe := &LoggingMock{Log: &NullLogger{}}

	wrapMe.On("Warningf", "winner, winner, chicken din%s", "ner")

	wrappedLog := reporter.WrapLogger(wrapMe, "")

	wrappedLog.Warningf("winner, winner, chicken din%s", "ner")

	client.AssertExpectations(c)
	wrapMe.AssertExpectations(c)
}

func (e *ErrorReportingTest) TestErrorf_ForwardsAndReports(c *C) {
	wrapMe := &LoggingMock{Log: &NullLogger{}}
	mockReporter := &ErrorReporterMock{}
	forwardLog := &errorForwardingLogger{
		wrappedLogger:     wrapMe,
		errorReporter:     mockReporter,
		errorAffectsLabel: "",
		labelsLock:        &sync.RWMutex{},
		labels:            make(map[string]string),
	}

	wrapMe.On("Errorf", "silly %s. you broke it", "goose")
	mockReporter.On("Report", ErrorReport{
		Err:             forwardedError{msg: "silly goose. you broke it"},
		Req:             nil,
		ErrorAffectsKey: "",
	})
	forwardLog.Errorf("silly %s. you broke it", "goose")

	wrapMe.AssertExpectations(c)
	mockReporter.AssertExpectations(c)
}

func (e *ErrorReportingTest) TestCriticalf_ForwardsAndReports(c *C) {
	wrapMe := &LoggingMock{Log: &NullLogger{}}
	mockReporter := &ErrorReporterMock{}
	forwardLog := &errorForwardingLogger{
		wrappedLogger:     wrapMe,
		errorReporter:     mockReporter,
		errorAffectsLabel: "",
		labelsLock:        &sync.RWMutex{},
		labels:            make(map[string]string),
	}

	wrapMe.On("Criticalf", "silly %s. you broke it", "goose")
	mockReporter.On("Report", ErrorReport{
		Err:             forwardedError{msg: "silly goose. you broke it"},
		Req:             nil,
		ErrorAffectsKey: "",
	})
	forwardLog.Criticalf("silly %s. you broke it", "goose")

	wrapMe.AssertExpectations(c)
	mockReporter.AssertExpectations(c)
}

func (e *ErrorReportingTest) TestAddLabels_ErrorAffectsKeyIsSetToValue(c *C) {
	wrapMe := &LoggingMock{Log: &NullLogger{}}
	mockReporter := &ErrorReporterMock{}
	forwardLog := &errorForwardingLogger{
		wrappedLogger:     wrapMe,
		errorReporter:     mockReporter,
		errorAffectsLabel: "pendo.io/sub_name",
		labelsLock:        &sync.RWMutex{},
		labels:            make(map[string]string),
	}

	labelsOne := map[string]string{
		"pendo.io/sub_name": "testSub",
		"another":           "label",
	}
	labelsTwo := map[string]string{
		"foo":     "bar",
		"another": "override",
	}

	wrapMe.On("AddLabels", labelsOne)
	wrapMe.On("AddLabels", labelsTwo)
	wrapMe.On("Errorf", "broken things!")
	mockReporter.On("Report", ErrorReport{
		Err:             forwardedError{msg: "broken things!"},
		Req:             nil,
		ErrorAffectsKey: "testSub",
	})

	err := forwardLog.AddLabels(labelsOne)
	c.Assert(err, IsNil)

	err = forwardLog.AddLabels(labelsTwo)
	c.Assert(err, IsNil)

	forwardLog.Errorf("broken things!")

	wrapMe.AssertExpectations(c)
	mockReporter.AssertExpectations(c)
	c.Assert(len(forwardLog.labels), Equals, 3)
}

func (e *ErrorReportingTest) TestRequest(c *C) {
	wrapMe := &LoggingMock{Log: &NullLogger{}}
	mockReporter := &ErrorReporterMock{}
	forwardLog := &errorForwardingLogger{
		wrappedLogger:     wrapMe,
		errorReporter:     mockReporter,
		errorAffectsLabel: "",
		labelsLock:        &sync.RWMutex{},
		labels:            make(map[string]string),
	}

	wrapMe.On("Criticalf", "bummer")
	wrapMe.On("Request", http.MethodPut, "https://127.0.0.1", "url", "fun")

	expectedRequest, err := http.NewRequest(http.MethodPut, "https://127.0.0.1", nil)
	if err != nil {
		c.FatalError(err)
	}

	mockReporter.On("Report", ErrorReport{
		Err:             forwardedError{msg: "bummer"},
		Req:             expectedRequest,
		ErrorAffectsKey: "",
	})
	forwardLog.Request(http.MethodPut, "https://127.0.0.1", "url", "fun")
	forwardLog.Criticalf("bummer")

	wrapMe.AssertExpectations(c)
	mockReporter.AssertExpectations(c)
}
