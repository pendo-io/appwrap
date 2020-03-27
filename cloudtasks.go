package appwrap

import (
	"context"
	"fmt"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"net/http"
	"net/url"
	"runtime"
	"sync"

	"cloud.google.com/go/cloudtasks/apiv2"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

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

func (t cloudTaskqueue) Add(c context.Context, task AppEngineTask, queueName string) (AppEngineTask, error) {
	taskCopy := task.Copy().(*cloudTaskAppEngineImpl)
	newTask, err := t.client.CreateTask(context.Background(), &taskspb.CreateTaskRequest{
		Task:   taskCopy.task,
		Parent: t.getFullQueueName(queueName),
	})
	return &cloudTaskAppEngineImpl{
		task: newTask,
	}, err
}

func (t cloudTaskqueue) AddMulti(c context.Context, tasks []AppEngineTask, queueName string) ([]AppEngineTask, error) {
	errList := make(MultiError, len(tasks))
	addedTasks := make([]AppEngineTask, len(tasks))
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

func (t cloudTaskqueue) NewPOSTTask(path string, params url.Values) AppEngineTask {
	task := NewAppEngineTask()
	h := make(http.Header)
	h.Set("Content-Type", "application/x-www-form-urlencoded")
	task.SetMethod("POST")
	task.SetPayload([]byte(params.Encode()))
	task.SetHeader(h)
	task.SetPath(path)
	return task
}
