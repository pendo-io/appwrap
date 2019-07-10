package appwrap

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/taskqueue"
)

type aeTaskImpl struct {
	task    *taskqueue.Task
	service string
	version string
}

func (t *aeTaskImpl) isTask() {}

func (t *aeTaskImpl) Copy() Task {
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
	return &aeTaskImpl{
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
		service: t.service,
		version: t.version,
	}
}

type QueueStatistics = taskqueue.QueueStatistics

func (t *aeTaskImpl) Delay() time.Duration {
	return t.task.Delay
}

func (t *aeTaskImpl) SetDelay(delay time.Duration) {
	t.task.Delay = delay
}

func (t *aeTaskImpl) Header() http.Header {
	return t.task.Header
}

func (t *aeTaskImpl) SetHeader(header http.Header) {
	t.task.Header = header
}

func (t *aeTaskImpl) host() string {
	return t.task.Header.Get("Host")
}

func (t *aeTaskImpl) setHost(hostname string) {
	innerTask := t.task
	if innerTask.Header == nil {
		innerTask.Header = make(http.Header)
	}
	innerTask.Header.Set("Host", hostname)
}

func (t *aeTaskImpl) Method() string {
	return t.task.Method
}

func (t *aeTaskImpl) SetMethod(method string) {
	t.task.Method = method
}

func (t *aeTaskImpl) Name() string {
	return t.task.Name
}

func (t *aeTaskImpl) SetName(name string) {
	t.task.Name = name
}

func (t *aeTaskImpl) Path() string {
	return t.task.Path
}

func (t *aeTaskImpl) SetPath(path string) {
	t.task.Path = path
}

func (t *aeTaskImpl) Payload() []byte {
	return t.task.Payload
}

func (t *aeTaskImpl) SetPayload(payload []byte) {
	t.task.Payload = payload
}

func (t *aeTaskImpl) RetryCount() int32 {
	return t.task.RetryCount
}

func (t *aeTaskImpl) SetRetryCount(count int32) {
	t.task.RetryCount = count
}

func (t *aeTaskImpl) Service() string {
	return t.service
}

func (t *aeTaskImpl) SetService(service string) {
	t.service = service
}

func (t *aeTaskImpl) Tag() string {
	return t.task.Tag
}

func (t *aeTaskImpl) SetTag(tag string) {
	t.task.Tag = tag
}

func (t *aeTaskImpl) Version() string {
	return t.version
}

func (t *aeTaskImpl) SetVersion(version string) {
	t.version = version
}

func newAeTask() Task {
	return &aeTaskImpl{
		task: &taskqueue.Task{},
	}
}

type appengineTaskqueue struct {
	c context.Context
}

func newAeTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	return appengineTaskqueue{c}
}

func (t appengineTaskqueue) populateTaskHosts(tasks []Task) {
	appInfo := NewAppengineInfoFromContext(t.c)
	project := appInfo.AppID()
	u := fmt.Sprintf("%s.appspot.com", project)
	for _, tIntf := range tasks {
		u := u
		task := tIntf.(*aeTaskImpl)
		if task.service != "" {
			u = task.service + "." + u
		}
		if task.version != "" {
			u = task.version + "." + u
		}
		task.setHost(u)
	}
}

func (t appengineTaskqueue) Add(c context.Context, task Task, queueName string) (Task, error) {
	t.populateTaskHosts([]Task{task})
	innerTask, err := taskqueue.Add(c, task.(*aeTaskImpl).task, queueName)
	return &aeTaskImpl{
		task: innerTask,
	}, err
}

func (t appengineTaskqueue) getInnerTasks(tasks []Task) []*taskqueue.Task {
	innerTasks := make([]*taskqueue.Task, len(tasks))
	for i, t := range tasks {
		innerTasks[i] = t.(*aeTaskImpl).task
	}
	return innerTasks
}

func (t appengineTaskqueue) wrapTasks(tasks []*taskqueue.Task) []Task {
	wrappedTasks := make([]Task, len(tasks))
	for i, t := range tasks {
		wrappedTasks[i] = &aeTaskImpl{
			task: t,
		}
	}
	return wrappedTasks
}

func (t appengineTaskqueue) AddMulti(c context.Context, tasks []Task, queueName string) ([]Task, error) {
	t.populateTaskHosts(tasks)
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
	return taskqueue.ModifyLease(c, task.(*aeTaskImpl).task, queueName, leaseTime)
}

func (t appengineTaskqueue) NewPOSTTask(path string, params url.Values) Task {
	return &aeTaskImpl{
		task: taskqueue.NewPOSTTask(path, params),
	}
}

func (t appengineTaskqueue) QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error) {
	return taskqueue.QueueStats(c, queueNames)
}
