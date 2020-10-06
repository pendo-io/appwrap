package appwrap

import (
	"net/http"

	"github.com/stretchr/testify/mock"
)

type LoggingMock struct {
	mock.Mock
	Log Logging // all log calls are passed through to this logger (NullLogger may be used)
}

func (m *LoggingMock) ResetMock() {
	m.Calls = make([]mock.Call, 0)
	m.ExpectedCalls = make([]*mock.Call, 0)
}

func (m *LoggingMock) Debugf(format string, args ...interface{}) {
	m.Mock.Called(append([]interface{}{format}, args...)...)
	m.Log.Debugf(format, args...)
}

func (m *LoggingMock) Infof(format string, args ...interface{}) {
	m.Mock.Called(append([]interface{}{format}, args...)...)
	m.Log.Infof(format, args...)
}

func (m *LoggingMock) Warningf(format string, args ...interface{}) {
	m.Mock.Called(append([]interface{}{format}, args...)...)
	m.Log.Warningf(format, args...)
}

func (m *LoggingMock) Errorf(format string, args ...interface{}) {
	m.Mock.Called(append([]interface{}{format}, args...)...)
	m.Log.Errorf(format, args...)
}

func (m *LoggingMock) Criticalf(format string, args ...interface{}) {
	m.Mock.Called(append([]interface{}{format}, args...)...)
	m.Log.Criticalf(format, args...)
}

func (m *LoggingMock) Request(method, url, format string, args ...interface{}) {
	m.Mock.Called(append([]interface{}{method, url, format}, args...)...)
	m.Log.Request(method, url, format, args...)
}

func (m *LoggingMock) AddLabels(labels map[string]string) error {
	m.Mock.Called(labels)
	return m.Log.AddLabels(labels)
}

type LogServiceMock struct {
	mock.Mock
}

func (m *LogServiceMock) CreateLog(labels map[string]string, logName LogName, r *http.Request, traceId string) Logging {
	args := m.Mock.Called(labels, logName, r, traceId)
	return args.Get(0).(Logging)
}

func (m *LogServiceMock) Close() {
	m.Mock.Called()
}
