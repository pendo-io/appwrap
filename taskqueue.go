package appwrap

import (
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/context"
)

type Taskqueue interface {
	Add(c context.Context, task AppEngineTask, queueName string) (AppEngineTask, error)
	AddMulti(c context.Context, tasks []AppEngineTask, queueName string) ([]AppEngineTask, error)
	DeleteMulti(c context.Context, tasks []AppEngineTask, queueName string) error
	Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]AppEngineTask, error)
	LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]AppEngineTask, error)
	ModifyLease(c context.Context, task AppEngineTask, queueName string, leaseTime int) error
	NewPOSTTask(path string, params url.Values) AppEngineTask
}

// This is so the calling code cannot create task structs directly.
// This is important for cloud tasks, where the fields of the struct have to be populated properly
// with blank values for the various pointer fields.
type AppEngineTask interface {
	// private method guarantees that caller can't imitate the struct
	isTask()
	Copy() AppEngineTask
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

func NewAppEngineTask() AppEngineTask {
	return newAppEngineCloudTask()
}

func NewTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	if IsDevAppServer {
		return cloudTaskqueue{}
	}
	return newCloudTaskqueue(c, loc)
}
