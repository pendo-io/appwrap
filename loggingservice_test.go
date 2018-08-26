package appwrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"

	"cloud.google.com/go/logging"
	"github.com/pendo-io/appwrap"
	"github.com/stretchr/testify/mock"

	. "gopkg.in/check.v1"
	"pendo.io/goisms"
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

func (mock *AppengineInfoMock) AppID() string {
	args := mock.Called()
	return args.String(0)
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
	log         goisms.SimpleLogging
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
		log:         appwrap.NewWriterLogger(os.Stdout),
		clientMock:  &ClientMock{},
	}
}

func (s *StackdriverLoggingServiceTests) TestLogImplementsSimpleLogging(c *C) {
	f := s.SetUpFixture(c)
	r := httptest.NewRequest(http.MethodGet, "http://localhost", strings.NewReader("Hello"))
	r.Header[headerCloudTraceContext] = []string{"id-to-connect-logs"}

	f.clientMock.On("SetUpOnError").Return().Once()
	f.appInfoMock.On("ModuleName").Return("my-module").Once()
	f.appInfoMock.On("AppID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version").Once()
	logCh := make(chan LogMessage)
	service := NewStackdriverLoggingService(f.clientMock, f.appInfoMock, logCh, f.log).(*StackdriverLoggingService)

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
	f.appInfoMock.On("AppID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version.12345").Once()
	logCh := make(chan LogMessage)
	service := NewStackdriverLoggingService(f.clientMock, f.appInfoMock, logCh, f.log).(*StackdriverLoggingService)

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
	f.appInfoMock.On("AppID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version.12345").Once()

	logCh := make(chan LogMessage)
	service := NewStackdriverLoggingService(f.clientMock, f.appInfoMock, logCh, f.log).(*StackdriverLoggingService)

	c.Assert(service, NotNil)
	c.Assert(service.resourceOptions, DeepEquals, basicAppEngineOptions("my-module", "my-project", "my-version"))
	f.assertMocks(c)
}

func (s *StackdriverLoggingServiceTests) TestLogServiceClose(c *C) {
	f := s.SetUpFixture(c)

	f.clientMock.On("SetUpOnError").Return().Once()
	f.appInfoMock.On("ModuleName").Return("my-module").Once()
	f.appInfoMock.On("AppID").Return("my-project").Once()
	f.appInfoMock.On("VersionID").Return("my-version.12345").Once()
	f.clientMock.On("Close").Return(nil).Once()

	logCh := make(chan LogMessage)
	service := NewStackdriverLoggingService(f.clientMock, f.appInfoMock, logCh, f.log).(*StackdriverLoggingService)

	service.Close()

	f.assertMocks(c)
}
