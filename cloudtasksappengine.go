package appwrap

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

type cloudTaskAppEngineImpl struct {
	task *taskspb.Task
}

func newAppEngineCloudTask() AppEngineTask {
	return &cloudTaskAppEngineImpl{
		task: &taskspb.Task{
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					AppEngineRouting: &taskspb.AppEngineRouting{},
				},
			},
		},
	}
}

func (t cloudTaskAppEngineImpl) Copy() CloudTask {
	innerCopy := *t.task.GetAppEngineHttpRequest()
	bodyCopy := make([]byte, len(innerCopy.Body))
	copy(bodyCopy, innerCopy.Body)
	headerCopy := make(map[string]string, len(innerCopy.Headers))
	for k, v := range innerCopy.Headers {
		headerCopy[k] = v
	}
	taskCopy := &cloudTaskAppEngineImpl{
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

func (t *cloudTaskAppEngineImpl) isTask() {}

func (t *cloudTaskAppEngineImpl) Delay() (delay time.Duration) {
	if sched := t.task.ScheduleTime; sched == nil {
	} else {
		delay = time.Unix(sched.Seconds, int64(sched.Nanos)).Sub(time.Now())
	}
	if delay < 0 {
		return time.Duration(0)
	}
	return
}

func (t *cloudTaskAppEngineImpl) getTask() *taskspb.Task {
	return t.task
}

func (t *cloudTaskAppEngineImpl) SetDelay(delay time.Duration) {
	eta := time.Now().Add(delay)
	t.SetEta(eta)
}

func (t *cloudTaskAppEngineImpl) SetEta(eta time.Time) {
	t.task.ScheduleTime = &timestamp.Timestamp{
		Seconds: eta.Unix(),
		Nanos:   int32(eta.Nanosecond()),
	}
}

func (t *cloudTaskAppEngineImpl) Name() string {
	return t.task.Name
}

func (t *cloudTaskAppEngineImpl) SetName(name string) {
	t.task.Name = name
}

func (t *cloudTaskAppEngineImpl) RetryCount() int32 {
	return t.task.DispatchCount
}

func (t *cloudTaskAppEngineImpl) SetRetryCount(count int32) {
	t.task.DispatchCount = count
}

func (t *cloudTaskAppEngineImpl) Tag() (tag string) {
	panic("not implemented for CloudTasks")
}

func (t *cloudTaskAppEngineImpl) SetTag(tag string) {
	panic("not implemented for CloudTasks")
}

func (t *cloudTaskAppEngineImpl) Header() http.Header {
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

func (t *cloudTaskAppEngineImpl) SetHeader(header http.Header) {
	req := t.task.GetAppEngineHttpRequest()
	reqHeader := make(map[string]string, len(header))
	for key, value := range header {
		reqHeader[key] = value[0]
	}
	req.Headers = reqHeader
}

func (t *cloudTaskAppEngineImpl) Method() string {
	req := t.task.GetAppEngineHttpRequest()
	return taskspb.HttpMethod_name[int32(req.HttpMethod)]
}

func (t *cloudTaskAppEngineImpl) SetMethod(method string) {
	if val := taskspb.HttpMethod_value[method]; val != 0 {
		req := t.task.GetAppEngineHttpRequest()
		req.HttpMethod = taskspb.HttpMethod(val)
	} else {
		panic(fmt.Sprintf("invalid task method: %s", method))
	}
}

func (t *cloudTaskAppEngineImpl) Path() (path string) {
	req := t.task.GetAppEngineHttpRequest()
	return req.RelativeUri
}

func (t *cloudTaskAppEngineImpl) SetPath(path string) {
	req := t.task.GetAppEngineHttpRequest()
	req.RelativeUri = path
}

func (t *cloudTaskAppEngineImpl) Payload() []byte {
	req := t.task.GetAppEngineHttpRequest()
	return req.Body
}

func (t *cloudTaskAppEngineImpl) SetPayload(payload []byte) {
	req := t.task.GetAppEngineHttpRequest()
	req.Body = payload
}

func (t *cloudTaskAppEngineImpl) Service() (service string) {
	routing := t.task.GetAppEngineHttpRequest().GetAppEngineRouting()
	return routing.Service
}

func (t *cloudTaskAppEngineImpl) SetService(service string) {
	routing := t.task.GetAppEngineHttpRequest().GetAppEngineRouting()
	routing.Service = service
}

func (t *cloudTaskAppEngineImpl) Version() string {
	routing := t.task.GetAppEngineHttpRequest().GetAppEngineRouting()
	return routing.Version
}

func (t *cloudTaskAppEngineImpl) SetVersion(version string) {
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

var (
	// This needs to be a pointer to guarantee atomic reads/writes to the value inside newCloudTaskqueue
	tqClient    *cloudTasksClient
	tqClientMtx sync.Mutex
)

const (
	concurrentReq = 12
	queuePathFmt  = "projects/%s/locations/%s/queues/%s"
	taskNameFmt   = "projects/%s/locations/%s/queues/%s/tasks/%s"
)

func newCloudTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	if c == nil {
		return newDevTaskqueue()
	}
	if tqClient == nil {
		tqClientMtx.Lock()
		defer tqClientMtx.Unlock()
		if tqClient == nil {
			totalConnPool := runtime.GOMAXPROCS(0) * concurrentReq
			o := []option.ClientOption{
				// Options borrowed from construction of the datastore client
				option.WithGRPCConnectionPool(totalConnPool),
			}
			if rawClient, err := cloudtasks.NewClient(c, o...); err != nil {
				panic(fmt.Sprintf("creating taskqueue client: %s", err))
			} else {
				var client cloudTasksClient = rawClient // convert to cloudTasksClient interface
				tqClient = &client
			}
		}
	}

	aeInfo := NewAppengineInfoFromContext(c)

	return cloudTaskqueue{
		client:   *tqClient,
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

func (t cloudTaskqueue) Add(c context.Context, task CloudTask, queueName string) (CloudTask, error) {
	taskCopy := task.Copy()

	newTask, err := t.client.CreateTask(context.Background(), &taskspb.CreateTaskRequest{
		Task:   taskCopy.getTask(),
		Parent: t.getFullQueueName(queueName),
	})
	switch task.(type) {
	case *cloudTaskAppEngineImpl:
		return &cloudTaskAppEngineImpl{
			task: newTask,
		}, err
	case *cloudTaskHttpImpl:
		return &cloudTaskHttpImpl{
			task: newTask,
		}, err
	}
	panic("Only AppEngine and Http target tasks are supported")
}

func (t cloudTaskqueue) AddMulti(c context.Context, tasks []CloudTask, queueName string) ([]CloudTask, error) {
	errList := make(MultiError, len(tasks))
	addedTasks := make([]CloudTask, len(tasks))
	var haveErr bool
	for i, task := range tasks {
		addedTasks[i], errList[i] = t.Add(c, task, queueName)
		if errList[i] != nil {
			haveErr = true
		}
	}
	if haveErr {
		return addedTasks, errList
	}
	return addedTasks, nil
}

func (t cloudTaskqueue) DeleteMulti(c context.Context, tasks []AppEngineTask, queueName string) error {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]AppEngineTask, error) {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]AppEngineTask, error) {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) ModifyLease(c context.Context, task AppEngineTask, queueName string, leaseTime int) error {
	panic("not implemented for CloudTasks")
}

func (t cloudTaskqueue) NewAppEngineCloudTask(path string, params url.Values) AppEngineTask {
	task := NewAppEngineTask()
	h := make(http.Header)
	h.Set("Content-Type", "application/x-www-form-urlencoded")
	task.SetMethod("POST")
	task.SetPayload([]byte(params.Encode()))
	task.SetHeader(h)
	task.SetPath(path)
	return task
}
