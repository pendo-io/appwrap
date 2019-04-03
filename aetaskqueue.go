// +build !cloudtasks

package appwrap

import (
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/taskqueue"
)

type taskImpl struct {
	task *taskqueue.Task
}

func (t *taskImpl) isTask() {}

func (t *taskImpl) Copy() Task {
	payloadCopy := make([]byte, len(t.task.Payload))
	copy(payloadCopy, t.task.Payload)

	headerCopy := make(http.Header, len(t.task.Header))
	for k, v := range t.task.Header {
		v2 := make([]string, len(v))
		copy(v2, v)
		headerCopy[k] = v2
	}

	var retryOptions *taskqueue.RetryOptions
	if t.task.RetryOptions == nil {
		retryOptions = nil
	} else {
		retryCopy := *t.task.RetryOptions
		retryOptions = &retryCopy
	}
	return &taskImpl{
		task: &taskqueue.Task{
			Path:         t.task.Path,
			Payload:      payloadCopy,
			Header:       headerCopy,
			Method:       t.task.Method,
			Name:         t.task.Name,
			Delay:        t.task.Delay,
			ETA:          t.task.ETA,
			RetryCount:   t.task.RetryCount,
			Tag:          t.task.Tag,
			RetryOptions: retryOptions,
		},
	}
}

type QueueStatistics = taskqueue.QueueStatistics

func (t *taskImpl) Delay() time.Duration {
	return t.task.Delay
}

func (t *taskImpl) SetDelay(delay time.Duration) {
	t.task.Delay = delay
}

func (t *taskImpl) Header() http.Header {
	return t.task.Header
}

func (t *taskImpl) SetHeader(header http.Header) {
	t.task.Header = header
}

func (t *taskImpl) Host() string {
	return t.task.Header.Get("Host")
}

func (t *taskImpl) SetHost(hostname string) {
	innerTask := t.task
	if innerTask.Header == nil {
		innerTask.Header = make(http.Header)
	}
	innerTask.Header.Set("Host", hostname)
}

func (t *taskImpl) Method() string {
	return t.task.Method
}

func (t *taskImpl) SetMethod(method string) {
	t.task.Method = method
}

func (t *taskImpl) Name() string {
	return t.task.Name
}

func (t *taskImpl) SetName(name string) {
	t.task.Name = name
}

func (t *taskImpl) Path() string {
	return t.task.Path
}

func (t *taskImpl) SetPath(path string) {
	t.task.Path = path
}

func (t *taskImpl) Payload() []byte {
	return t.task.Payload
}

func (t *taskImpl) SetPayload(payload []byte) {
	t.task.Payload = payload
}

func (t *taskImpl) RetryCount() int32 {
	return t.task.RetryCount
}

func (t *taskImpl) SetRetryCount(count int32) {
	t.task.RetryCount = count
}

func (t *taskImpl) Tag() string {
	return t.task.Tag
}

func (t *taskImpl) SetTag(tag string) {
	t.task.Tag = tag
}

func NewTask() Task {
	return &taskImpl{
		task: &taskqueue.Task{},
	}
}

type appengineTaskqueue struct {
	c context.Context
}

func NewTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	return appengineTaskqueue{c}
}

func (t appengineTaskqueue) Add(c context.Context, task Task, queueName string) (Task, error) {
	innerTask, err := taskqueue.Add(c, task.(*taskImpl).task, queueName)
	return &taskImpl{
		task: innerTask,
	}, err
}

func (t appengineTaskqueue) getInnerTasks(tasks []Task) []*taskqueue.Task {
	innerTasks := make([]*taskqueue.Task, len(tasks))
	for i, t := range tasks {
		innerTasks[i] = t.(*taskImpl).task
	}
	return innerTasks
}

func (t appengineTaskqueue) wrapTasks(tasks []*taskqueue.Task) []Task {
	wrappedTasks := make([]Task, len(tasks))
	for i, t := range tasks {
		wrappedTasks[i] = &taskImpl{
			task: t,
		}
	}
	return wrappedTasks
}

func (t appengineTaskqueue) AddMulti(c context.Context, tasks []Task, queueName string) ([]Task, error) {
	innerTasks := t.getInnerTasks(tasks)
	newTasks, err := taskqueue.AddMulti(c, innerTasks, queueName)
	return t.wrapTasks(newTasks), err
}

func (t appengineTaskqueue) DeleteMulti(c context.Context, tasks []Task, queueName string) error {
	innerTasks := t.getInnerTasks(tasks)
	return taskqueue.DeleteMulti(c, innerTasks, queueName)
}

func (t appengineTaskqueue) Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]Task, error) {
	tasks, err := taskqueue.Lease(c, maxTasks, queueName, leaseTime)
	return t.wrapTasks(tasks), err
}

func (t appengineTaskqueue) LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]Task, error) {
	tasks, err := taskqueue.LeaseByTag(c, maxTasks, queueName, leaseTime, tag)
	return t.wrapTasks(tasks), err
}

func (t appengineTaskqueue) ModifyLease(c context.Context, task Task, queueName string, leaseTime int) error {
	return taskqueue.ModifyLease(c, task.(*taskImpl).task, queueName, leaseTime)
}

func (t appengineTaskqueue) NewPOSTTask(path string, params url.Values) Task {
	return &taskImpl{
		task: taskqueue.NewPOSTTask(path, params),
	}
}

func (t appengineTaskqueue) QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error) {
	return taskqueue.QueueStats(c, queueNames)
}
