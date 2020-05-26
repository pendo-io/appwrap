package appwrap

import (
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"
	"net/http"
	"net/url"
)

type TaskqueueMock struct {
	mock.Mock
}

func (m *TaskqueueMock) Add(c context.Context, task CloudTask, queueName string) (CloudTask, error) {
	args := m.Mock.Called(c, task, queueName)
	return args.Get(0).(CloudTask), args.Error(1)
}

func (m *TaskqueueMock) AddMulti(c context.Context, tasks []CloudTask, queueName string) ([]CloudTask, error) {
	args := m.Mock.Called(c, tasks, queueName)
	return args.Get(0).([]CloudTask), args.Error(1)
}

func (m *TaskqueueMock) DeleteMulti(c context.Context, tasks []AppEngineTask, queueName string) error {
	args := m.Mock.Called(c, tasks, queueName)
	return args.Error(0)
}

func (m *TaskqueueMock) Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]AppEngineTask, error) {
	args := m.Mock.Called(c, maxTasks, queueName, leaseTime)
	return args.Get(0).([]AppEngineTask), args.Error(1)
}

func (m *TaskqueueMock) LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]AppEngineTask, error) {
	args := m.Mock.Called(c, maxTasks, queueName, leaseTime, tag)
	return args.Get(0).([]AppEngineTask), args.Error(1)
}

func (m *TaskqueueMock) ModifyLease(c context.Context, task AppEngineTask, queueName string, leaseTime int) error {
	args := m.Mock.Called(c, task, queueName, leaseTime)
	return args.Error(0)
}

func (m *TaskqueueMock) NewAppEngineCloudTask(path string, params url.Values) AppEngineTask {
	args := m.Mock.Called(path, params)
	return args.Get(0).(AppEngineTask)
}

func (m *TaskqueueMock) NewHttpCloudTask(serviceAccount string, url string, method string, data []byte, headers http.Header) HttpTask {
	args := m.Mock.Called(serviceAccount, url, method, data, headers)
	return args.Get(0).(HttpTask)
}
