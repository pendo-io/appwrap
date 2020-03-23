package appwrap

import (
	"golang.org/x/net/context"
	"net/http"
	"net/url"
	"time"
)

type Taskqueue interface {
	Add(c context.Context, task AppEngineTask, queueName string) (AppEngineTask, error)
	AddHttpTask(c context.Context, task HttpTask, queueName string) (HttpTask, error)
	AddMulti(c context.Context, tasks []AppEngineTask, queueName string) ([]AppEngineTask, error)
	DeleteMulti(c context.Context, tasks []AppEngineTask, queueName string) error
	Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]AppEngineTask, error)
	LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]AppEngineTask, error)
	ModifyLease(c context.Context, task AppEngineTask, queueName string, leaseTime int) error
	NewAppEngineCloudTask(path string, params url.Values) AppEngineTask
	NewHttpCloudTask(url string, data []byte, headers http.Header) HttpTask
}

// This is so the calling code cannot create task structs directly.
// This is important for cloud tasks, where the fields of the struct have to be populated properly
// with blank values for the various pointer fields.
type CloudTask interface {
	// private method guarantees that caller can't imitate the struct
	isTask()
	Delay() time.Duration
	SetDelay(delay time.Duration)
	Name() string
	SetName(name string)
	RetryCount() int32
	SetRetryCount(count int32)
	Tag() string
	SetTag(tag string)
}

type AppEngineTask interface {
	CloudTask
	Copy() AppEngineTask
	Header() http.Header
	SetHeader(header http.Header)
	Method() string
	SetMethod(method string)
	Path() string
	SetPath(path string)
	Payload() []byte
	SetPayload(payload []byte)
	Service() string
	SetService(service string)
	Version() string
	SetVersion(version string)
}

type HttpTask interface {
	CloudTask
	Copy() HttpTask
	Header() http.Header
	SetHeader(header http.Header)
	Method() string
	SetMethod(method string)
	Payload() []byte
	SetPayload(payload []byte)
	Url() string
	SetUrl(url string)
}

type CloudTasksLocation string

func NewAppEngineTask() AppEngineTask {
	return newCloudTask()
}

func NewHttpCloudTask() HttpTask {
	return newHttpCloudTask()
}

func NewTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	if IsDevAppServer {
		return cloudTaskqueue{}
	}
	return newCloudTaskqueue(c, loc)
}
