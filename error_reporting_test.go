package appwrap

import (
	. "gopkg.in/check.v1"
	"context"
	"github.com/stretchr/testify/mock"
	"cloud.google.com/go/errorreporting"
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

func (e *ErrorReportingTest) TestCreateNewErrorReporter(c *C) {
	testAppInfo := &AppengineInfoMock{}
	testAppInfo.On("DataProjectID").Return("dataProject")
	testAppInfo.On("ModuleName").Return("moduleName")
	testAppInfo.On("VersionID").Return("version")

	reporter, err := NewGoogleErrorReporter(testAppInfo, context.Background(), &NullLogger{})

	c.Assert(err, IsNil)
	c.Assert(reporter, NotNil)
	c.Assert(reporter.client, NotNil)
}

