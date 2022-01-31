package appwrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"github.com/stretchr/testify/mock"
	. "gopkg.in/check.v1"
)

type AppengineInfoMock struct {
	mock.Mock
}

func (mock *AppengineInfoMock) InstanceID() string {
	return mock.Called().String(0)
}

func (mock *AppengineInfoMock) ModuleName() string {
	return mock.Called().String(0)
}

func (mock *AppengineInfoMock) NumInstances(name, version string) (int, error) {
	args := mock.Called(name, version)
	return args.Int(0), args.Error(1)
}

func (mock *AppengineInfoMock) VersionID() string {
	return mock.Called().String(0)
}

func (mock *AppengineInfoMock) ModuleDefaultVersionID(moduleName string) (string, error) {
	args := mock.Called(moduleName)
	return args.String(0), args.Error(1)
}

func (mock *AppengineInfoMock) ModuleHostname(version, module, app string) (string, error) {
	args := mock.Called(version, module, app)
	return args.String(0), args.Error(1)
}

func (mock *AppengineInfoMock) ModuleHasTraffic(moduleName, moduleVersion string) (bool, error) {
	args := mock.Called(moduleName, moduleVersion)
	return args.Bool(0), args.Error(1)
}

func (mock *AppengineInfoMock) DataProjectID() string {
	args := mock.Called()
	return args.String(0)
}

func (mock *AppengineInfoMock) NativeProjectID() string {
	args := mock.Called()
	return args.String(0)
}

func (mock *AppengineInfoMock) Zone() string {
	return mock.Called().String(0)
}

type ClientMock struct {
	mock.Mock
}

func (m ClientMock) Logger(logID string, opts ...logging.LoggerOption) LoggerInterface {
	args := m.Mock.Called(logID, opts)
	return args.Get(0).(LoggerInterface)
}

func (m ClientMock) Close() error {
	args := m.Mock.Called()
	return args.Error(0)
}

func (m ClientMock) Ping(ctx context.Context) error {
	args := m.Mock.Called()
	return args.Error(0)
}

func (m ClientMock) SetUpOnError() {
	m.Mock.Called()
}

type LoggerMock struct {
	mock.Mock
}

func (m LoggerMock) Log(entry logging.Entry) {
	m.Called(entry)
}

func (m LoggerMock) Flush() error {
	args := m.Mock.Called()
	return args.Error(0)
}

type LoggingServiceTestFixture struct {
	appInfoMock *AppengineInfoMock
	log         Logging
	clientMock  *ClientMock
}

func (f *LoggingServiceTestFixture) assertMocks(c *C) {
	f.appInfoMock.AssertExpectations(c)
	f.clientMock.AssertExpectations(c)
}

type StackdriverLoggingServiceTests struct{}

var _ = Suite(&StackdriverLoggingServiceTests{})

func (s *StackdriverLoggingServiceTests) SetUpTest(c *C) {
}

func (s *StackdriverLoggingServiceTests) SetUpFixture(c *C) LoggingServiceTestFixture {
	return LoggingServiceTestFixture{
		appInfoMock: &AppengineInfoMock{},
		log:         NewWriterLogger(os.Stdout),
		clientMock:  &ClientMock{},
	}
}

func (s *StackdriverLoggingServiceTests) TestLogImplementsSimpleLogging(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}

	f.clientMock.On("SetUpOnError").Return().Once()
	f.appInfoMock.On("ModuleName").Return("my-module").Once()
	f.appInfoMock.On("NativeProjectID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version").Once()
	service := newStackdriverLoggingService(f.clientMock, f.appInfoMock, f.log).(*StackdriverLoggingService)

	c.Assert(service, NotNil)
	c.Assert(service.resourceOptions, DeepEquals, basicAppEngineOptions("my-module", "my-project", "my-version"))
	f.assertMocks(c)
}

func (s *StackdriverLoggingServiceTests) TestLogCommonAppEngineLabels(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}

	f.clientMock.On("SetUpOnError").Return().Once()
	f.appInfoMock.On("ModuleName").Return("my-module").Once()
	f.appInfoMock.On("NativeProjectID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version.12345").Once()
	service := newStackdriverLoggingService(f.clientMock, f.appInfoMock, f.log).(*StackdriverLoggingService)

	c.Assert(service, NotNil)
	c.Assert(service.resourceOptions, DeepEquals, basicAppEngineOptions("my-module", "my-project", "my-version"))
	f.assertMocks(c)
}

func (s *StackdriverLoggingServiceTests) TestLogBaseVersion(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}

	f.clientMock.On("SetUpOnError").Return().Once()
	f.appInfoMock.On("ModuleName").Return("my-module").Once()
	f.appInfoMock.On("NativeProjectID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version.12345").Once()

	service := newStackdriverLoggingService(f.clientMock, f.appInfoMock, f.log).(*StackdriverLoggingService)

	c.Assert(service, NotNil)
	c.Assert(service.resourceOptions, DeepEquals, basicAppEngineOptions("my-module", "my-project", "my-version"))
	f.assertMocks(c)
}

func (s *StackdriverLoggingServiceTests) TestLogServiceProcessLogAndClose(c *C) {
	f := s.SetUpFixture(c)

	start := time.Now()
	f.clientMock.On("SetUpOnError").Return().Once()
	f.appInfoMock.On("ModuleName").Return("my-module").Once()
	f.appInfoMock.On("NativeProjectID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version.12345").Once()

	loggerMock1 := &LoggerMock{}
	f.clientMock.On("Logger", "test1", mock.AnythingOfType("[]logging.LoggerOption")).Return(loggerMock1).Once()
	f.clientMock.On("Logger", requestLogPath, mock.AnythingOfType("[]logging.LoggerOption")).Return(loggerMock1).Once()
	loggerMock1.On("Log", mock.MatchedBy(func(log logging.Entry) bool {
		if log.Timestamp.Before(start) {
			return false
		}
		if log.Severity != logging.Info {
			return false
		}
		if log.Payload != "test1-message" {
			return false
		}
		if log.Trace != "test1-trace" {
			return false
		}
		return true
	})).Return().Once()

	f.clientMock.On("Close").Return(nil).Once()

	service := newStackdriverLoggingService(f.clientMock, f.appInfoMock, f.log).(*StackdriverLoggingService)
	logger := service.CreateLog(nil, "test1", nil, "test1-trace").(*StackdriverLogging)
	logger.Infof("test1-message")
	logger.parentLogger.(*LoggerMock).On("Flush").Return(nil).Once()
	logger.childLogger.(*LoggerMock).On("Flush").Return(nil).Once()
	logger.Close(nil)
	service.Close()

	f.assertMocks(c)
	loggerMock1.AssertExpectations(c)
}

func (s *StackdriverLoggingServiceTests) TestLogServiceClose(c *C) {
	f := s.SetUpFixture(c)

	f.clientMock.On("SetUpOnError").Return().Once()
	f.appInfoMock.On("ModuleName").Return("my-module").Once()
	f.appInfoMock.On("NativeProjectID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version.12345").Once()
	f.clientMock.On("Close").Return(nil).Once()

	service := newStackdriverLoggingService(f.clientMock, f.appInfoMock, f.log).(*StackdriverLoggingService)

	service.Close()

	f.assertMocks(c)
}
