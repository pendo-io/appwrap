// +build cloudtasks

package appwrap

import (
	"context"
	"fmt"
	"github.com/googleapis/gax-go/v2"
	"net/http"
	"net/url"
	"sync"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2beta3"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/appengine"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta3"
	"math/rand"
	"strconv"
)

type taskImpl struct {
	task *taskspb.Task
}

type QueueStatistics struct{}

func NewTask() Task {
	return &taskImpl{
		task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{},
			},
		},
	}
}

func (t *taskImpl) isTask() {}

func (t *taskImpl) Copy() Task {
	innerCopy := *t.task.GetHttpRequest()
	bodyCopy := make([]byte, len(innerCopy.Body))
	copy(bodyCopy, innerCopy.Body)
	headerCopy := make(map[string]string, len(innerCopy.Headers))
	for k, v := range innerCopy.Headers {
		headerCopy[k] = v
	}
	taskCopy := &taskImpl{
		task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:                 innerCopy.Url,
					HttpMethod:          innerCopy.HttpMethod,
					Headers:             headerCopy,
					Body:                bodyCopy,
					AuthorizationHeader: innerCopy.AuthorizationHeader,
				},
			},
		},
	}
	return taskCopy
}

func (t *taskImpl) Delay() (delay time.Duration) {
	if sched := t.task.ScheduleTime; sched == nil {
	} else {
		delay = time.Unix(sched.Seconds, int64(sched.Nanos)).Sub(time.Now())
	}
	if delay < 0 {
		return time.Duration(0)
	}
	return
}

func (t *taskImpl) SetDelay(delay time.Duration) {
	eta := time.Now().Add(delay)
	t.SetEta(eta)
}

func (t *taskImpl) SetEta(eta time.Time) {
	t.task.ScheduleTime = &timestamp.Timestamp{
		Seconds: eta.Unix(),
		Nanos:   int32(eta.Nanosecond()),
	}
}

func (t *taskImpl) Header() http.Header {
	req := t.task.GetHttpRequest()
	header := make(http.Header, len(req.Headers))
	if req.Headers == nil {
		return nil
	}
	for key, value := range req.Headers {
		header[key] = []string{value}
	}
	return header
}

func (t *taskImpl) SetHeader(header http.Header) {
	req := t.task.GetHttpRequest()
	reqHeader := make(map[string]string, len(header))
	for key, value := range header {
		reqHeader[key] = value[0]
	}
	req.Headers = reqHeader
}

func (t *taskImpl) Host() (host string) {
	if req := t.task.GetHttpRequest(); req == nil {
	} else if u, err := url.Parse(req.Url); err != nil {
		panic(fmt.Sprintf("invalid stored task url: %s", req.Url))
	} else {
		host = u.Hostname()
	}
	return
}

func (t *taskImpl) SetHost(host string) {
	verifyHost(host)
	req := t.task.GetHttpRequest()
	var u *url.URL
	var err error
	if req.Url == "" {
		u = &url.URL{}
	} else if u, err = url.Parse(req.Url); err != nil {
		panic(fmt.Sprintf("invalid stored task url: %s, error: %s", req.Url, err))
	}
	u.Host = host
	if host == "" {
		u.Scheme = ""
	} else {
		u.Scheme = "https"
	}
	req.Url = u.String()
}

func verifyHost(host string) {
	u := &url.URL{Host: host}
	_, err := url.Parse(u.String())
	if err != nil {
		panic(fmt.Sprintf("invalid host: %s", host))
	}
}

func (t *taskImpl) Method() string {
	req := t.task.GetHttpRequest()
	return taskspb.HttpMethod_name[int32(req.HttpMethod)]
}

func (t *taskImpl) SetMethod(method string) {
	if val := taskspb.HttpMethod_value[method]; val != 0 {
		req := t.task.GetHttpRequest()
		req.HttpMethod = taskspb.HttpMethod(val)
	} else {
		panic(fmt.Sprintf("invalid task method: %s", method))
	}
}

func (t *taskImpl) Name() string {
	return t.task.Name
}

func (t *taskImpl) SetName(name string) {
	t.task.Name = name
}

func (t *taskImpl) Path() (path string) {
	if req := t.task.GetHttpRequest(); req == nil {
	} else if u, err := url.Parse(req.Url); err != nil {
		panic(fmt.Sprintf("invalid stored task url: %s", req.Url))
	} else {
		path = u.Path
	}
	return
}

func (t *taskImpl) SetPath(path string) {
	verifyPath(path)
	req := t.task.GetHttpRequest()
	var u *url.URL
	var err error
	if req.Url == "" {
		u = &url.URL{}
	} else if u, err = url.Parse(req.Url); err != nil {
		panic(fmt.Sprintf("invalid stored task url: %s, error: %s", req.Url, err))
	}
	u.Path = path
	req.Url = u.String()
}

func verifyPath(path string) {
	u := &url.URL{Path: path}
	_, err := url.Parse(u.String())
	if err != nil {
		panic(fmt.Sprintf("invalid path: %s", path))
	}
}

func (t *taskImpl) Payload() []byte {
	req := t.task.GetHttpRequest()
	return req.Body
}

func (t *taskImpl) SetPayload(payload []byte) {
	req := t.task.GetHttpRequest()
	req.Body = payload
}

func (t *taskImpl) RetryCount() int32 {
	return t.task.DispatchCount
}

func (t *taskImpl) SetRetryCount(count int32) {
	t.task.DispatchCount = count
}

func (t *taskImpl) Tag() (tag string) {
	panic("not implemented for CloudTasks")
}

func (t *taskImpl) SetTag(tag string) {
	panic("not implemented for CloudTasks")
}

type cloudTaskqueue struct {
	ctx      context.Context
	client   cloudTasksClient
	location CloudTasksLocation
	project  string
}

type cloudTasksClient interface {
	CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error)
}

var tqClient cloudTasksClient = nil
var tqClientMtx = &sync.Mutex{}

const (
	queuePathFmt = "projects/%s/locations/%s/queues/%s"
	taskNameFmt  = "projects/%s/locations/%s/queues/%s/tasks/%s"
)

func NewTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	if c == nil {
		return newDevTaskqueue()
	}
	var err error
	if tqClient == nil {
		tqClientMtx.Lock()
		defer tqClientMtx.Unlock()
		if tqClient == nil {
			if tqClient, err = cloudtasks.NewClient(c); err != nil {
				panic(fmt.Sprintf("creating taskqueue client: %s", err))
			}
		}
	}

	aeInfo := NewAppengineInfoFromContext(c)

	return cloudTaskqueue{
		client:   tqClient,
		ctx:      c,
		project:  aeInfo.AppID(),
		location: loc,
	}
}

func newDevTaskqueue() Taskqueue {
	return cloudTaskqueue{}
}

func (t cloudTaskqueue) getFullQueueName(queueName string) string {
	return fmt.Sprintf(queuePathFmt, t.project, t.location, queueName)
}

func (t cloudTaskqueue) getTaskName(queueName string) string {
	taskName := strconv.Itoa(rand.Intn(1 << 31))
	return fmt.Sprintf(taskNameFmt, t.project, t.location, queueName, taskName)
}

func (t cloudTaskqueue) Add(c context.Context, task Task, queueName string) (Task, error) {
	taskCopy := task.Copy().(*taskImpl)
	taskName := t.getTaskName(queueName)
	taskCopy.SetName(taskName)
	newTask, err := t.client.CreateTask(c, &taskspb.CreateTaskRequest{
		Task:   taskCopy.task,
		Parent: t.getFullQueueName(queueName),
	})
	return &taskImpl{
		task: newTask,
	}, err
}

func (t cloudTaskqueue) AddMulti(c context.Context, tasks []Task, queueName string) ([]Task, error) {
	errList := make(appengine.MultiError, len(tasks))
	addedTasks := make([]Task, len(tasks))
	var haveErr bool
	for i, task := range tasks {
		addedTasks[i], errList[i] = t.Add(c, task, queueName)
		if errList[i] != nil {
			haveErr = true
		}
	}
	if !haveErr {
		errList = nil
	}
	return addedTasks, errList
}

func (t cloudTaskqueue) DeleteMulti(c context.Context, tasks []Task, queueName string) error {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]Task, error) {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]Task, error) {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) ModifyLease(c context.Context, task Task, queueName string, leaseTime int) error {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) NewPOSTTask(path string, params url.Values) Task {
	task := NewTask()
	h := make(http.Header)
	h.Set("Content-Type", "application/x-www-form-urlencoded")
	task.SetMethod("POST")
	task.SetPayload([]byte(params.Encode()))
	task.SetHeader(h)
	task.SetPath(path)
	return task
}

func (t cloudTaskqueue) QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error) {
	panic("not implemented for CloudTasks")
}
