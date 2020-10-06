package appwrap

import "github.com/stretchr/testify/mock"

type ErrorReporterMock struct {
	mock.Mock
}

func (m *ErrorReporterMock) Report(errReport ErrorReport) {
	m.Called(errReport)
}

func (m *ErrorReporterMock) FlushReports() {
	m.Called()
}

func (m *ErrorReporterMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *ErrorReporterMock) WrapLogger(logging Logging, errorAffectsLabel string) Logging {
	args := m.Called(logging, errorAffectsLabel)
	return args.Get(0).(Logging)
}
