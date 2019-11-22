package appwrap

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"

	"cloud.google.com/go/cloudtasks/apiv2"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/appengine"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

type cloudTaskImpl struct {
	task *taskspb.Task
}

func newCloudTask() Task {
	return &cloudTaskImpl{
		task: &taskspb.Task{
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					AppEngineRouting: &taskspb.AppEngineRouting{},
				},
			},
		},
	}
}

func (t *cloudTaskImpl) isTask() {}

func (t *cloudTaskImpl) Copy() Task {
	innerCopy := *t.task.GetAppEngineHttpRequest()
	bodyCopy := make([]byte, len(innerCopy.Body))
	copy(bodyCopy, innerCopy.Body)
	headerCopy := make(map[string]string, len(innerCopy.Headers))
	for k, v := range innerCopy.Headers {
		headerCopy[k] = v
	}
	taskCopy := &cloudTaskImpl{
		task: &taskspb.Task{
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					AppEngineRouting: &taskspb.AppEngineRouting{
						Service:  innerCopy.AppEngineRouting.Service,
						Version:  innerCopy.AppEngineRouting.Version,
						Instance: innerCopy.AppEngineRouting.Instance,
						Host:     innerCopy.AppEngineRouting.Host,
					},
					HttpMethod:  innerCopy.HttpMethod,
					Headers:     headerCopy,
					Body:        bodyCopy,
					RelativeUri: innerCopy.RelativeUri,
				},
			},
		},
	}
	return taskCopy
}

func (t *cloudTaskImpl) Delay() (delay time.Duration) {
	if sched := t.task.ScheduleTime; sched == nil {
	} else {
		delay = time.Unix(sched.Seconds, int64(sched.Nanos)).Sub(time.Now())
	}
	if delay < 0 {
		return time.Duration(0)
	}
	return
}

func (t *cloudTaskImpl) SetDelay(delay time.Duration) {
	eta := time.Now().Add(delay)
	t.SetEta(eta)
}

func (t *cloudTaskImpl) SetEta(eta time.Time) {
	t.task.ScheduleTime = &timestamp.Timestamp{
		Seconds: eta.Unix(),
		Nanos:   int32(eta.Nanosecond()),
	}
}

func (t *cloudTaskImpl) Header() http.Header {
	req := t.task.GetAppEngineHttpRequest()
	header := make(http.Header, len(req.Headers))
	if req.Headers == nil {
		return nil
	}
	for key, value := range req.Headers {
		header[key] = []string{value}
	}
	return header
}

func (t *cloudTaskImpl) SetHeader(header http.Header) {
	req := t.task.GetAppEngineHttpRequest()
	reqHeader := make(map[string]string, len(header))
	for key, value := range header {
		reqHeader[key] = value[0]
	}
	req.Headers = reqHeader
}

func (t *cloudTaskImpl) Method() string {
	req := t.task.GetAppEngineHttpRequest()
	return taskspb.HttpMethod_name[int32(req.HttpMethod)]
}

func (t *cloudTaskImpl) SetMethod(method string) {
	if val := taskspb.HttpMethod_value[method]; val != 0 {
		req := t.task.GetAppEngineHttpRequest()
		req.HttpMethod = taskspb.HttpMethod(val)
	} else {
		panic(fmt.Sprintf("invalid task method: %s", method))
	}
}

func (t *cloudTaskImpl) Name() string {
	return t.task.Name
}

func (t *cloudTaskImpl) SetName(name string) {
	t.task.Name = name
}

func (t *cloudTaskImpl) Path() (path string) {
	req := t.task.GetAppEngineHttpRequest()
	return req.RelativeUri
}

func (t *cloudTaskImpl) SetPath(path string) {
	req := t.task.GetAppEngineHttpRequest()
	req.RelativeUri = path
}

func (t *cloudTaskImpl) Payload() []byte {
	req := t.task.GetAppEngineHttpRequest()
	return req.Body
}

func (t *cloudTaskImpl) SetPayload(payload []byte) {
	req := t.task.GetAppEngineHttpRequest()
	req.Body = payload
}

func (t *cloudTaskImpl) RetryCount() int32 {
	return t.task.DispatchCount
}

func (t *cloudTaskImpl) SetRetryCount(count int32) {
	t.task.DispatchCount = count
}

func (t *cloudTaskImpl) Service() (service string) {
	routing := t.task.GetAppEngineHttpRequest().GetAppEngineRouting()
	return routing.Service
}

func (t *cloudTaskImpl) SetService(service string) {
	routing := t.task.GetAppEngineHttpRequest().GetAppEngineRouting()
	routing.Service = service
}

func (t *cloudTaskImpl) Tag() (tag string) {
	panic("not implemented for CloudTasks")
}

func (t *cloudTaskImpl) SetTag(tag string) {
	panic("not implemented for CloudTasks")
}

func (t *cloudTaskImpl) Version() string {
	routing := t.task.GetAppEngineHttpRequest().GetAppEngineRouting()
	return routing.Version
}

func (t *cloudTaskImpl) SetVersion(version string) {
	routing := t.task.GetAppEngineHttpRequest().GetAppEngineRouting()
	routing.Version = version
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
	concurrentReq = 12
	queuePathFmt  = "projects/%s/locations/%s/queues/%s"
	taskNameFmt   = "projects/%s/locations/%s/queues/%s/tasks/%s"
)

func newCloudTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	if c == nil {
		return newDevTaskqueue()
	}
	var err error
	if tqClient == nil {
		tqClientMtx.Lock()
		defer tqClientMtx.Unlock()
		if tqClient == nil {
			totalConnPool := runtime.GOMAXPROCS(0) * concurrentReq
			o := []option.ClientOption{
				// Options borrowed from construction of the datastore client
				option.WithGRPCConnectionPool(totalConnPool),
			}
			if tqClient, err = cloudtasks.NewClient(c, o...); err != nil {
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

func (t cloudTaskqueue) Add(c context.Context, task Task, queueName string) (Task, error) {
	taskCopy := task.Copy().(*cloudTaskImpl)
	newTask, err := t.client.CreateTask(c, &taskspb.CreateTaskRequest{
		Task:   taskCopy.task,
		Parent: t.getFullQueueName(queueName),
	})
	return &cloudTaskImpl{
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
