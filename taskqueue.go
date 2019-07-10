package appwrap

import (
	"net/http"
	"net/url"
	"os"
	"time"

	"golang.org/x/net/context"
)

type Taskqueue interface {
	Add(c context.Context, task Task, queueName string) (Task, error)
	AddMulti(c context.Context, tasks []Task, queueName string) ([]Task, error)
	DeleteMulti(c context.Context, tasks []Task, queueName string) error
	Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]Task, error)
	LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]Task, error)
	ModifyLease(c context.Context, task Task, queueName string, leaseTime int) error
	NewPOSTTask(path string, params url.Values) Task
	QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error)
}

// This is so the calling code cannot create task structs directly.
// This is important for cloud tasks, where the fields of the struct have to be populated properly
// with blank values for the various pointer fields.
type Task interface {
	// private method guarantees that caller can't imitate the struct
	isTask()
	Copy() Task
	Delay() time.Duration
	SetDelay(delay time.Duration)
	Header() http.Header
	SetHeader(header http.Header)
	Method() string
	SetMethod(method string)
	Name() string
	SetName(name string)
	Path() string
	SetPath(path string)
	Payload() []byte
	SetPayload(payload []byte)
	RetryCount() int32
	SetRetryCount(count int32)
	Service() string
	SetService(service string)
	Tag() string
	SetTag(tag string)
	Version() string
	SetVersion(version string)
}

type CloudTasksLocation string

var useCloudTasks bool

func NewTask() Task {
	if useCloudTasks {
		return newCloudTask()
	} else if loc := os.Getenv("cloud_tasks_location"); loc != "" {
		return newCloudTask()
	} else {
		return newAeTask()
	}
}

func NewTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	if loc == "" {
		return newAeTaskqueue(c, loc)
	} else {
		useCloudTasks = true
		return newCloudTaskqueue(c, loc)
	}
}
