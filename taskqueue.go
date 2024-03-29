package appwrap

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

type Taskqueue interface {
	Add(c context.Context, task CloudTask, queueName string) (CloudTask, error)
	AddMulti(c context.Context, tasks []CloudTask, queueName string) ([]CloudTask, error)
	NewAppEngineCloudTask(path string, params url.Values) AppEngineTask
	NewHttpCloudTask(serviceAccount string, url string, method string, data []byte, headers http.Header) HttpTask
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
	Copy() CloudTask
	Header() http.Header
	SetHeader(header http.Header)
	Method() string
	SetMethod(method string)
	Payload() []byte
	SetPayload(payload []byte)
}

type AppEngineTask interface {
	CloudTask
	Path() string
	SetPath(path string)
	Service() string
	SetService(service string)
	Version() string
	SetVersion(version string)
}

type HttpTask interface {
	CloudTask
	Url() string
	SetUrl(url string)
}

type CloudTasksLocation string

func NewAppEngineTask() AppEngineTask {
	return newAppEngineCloudTask()
}

func NewHttpCloudTask(serviceAccount string) HttpTask {
	return newHttpCloudTask(serviceAccount)
}

func NewTaskqueue(c context.Context, loc CloudTasksLocation) Taskqueue {
	if IsDevAppServer {
		return cloudTaskqueue{}
	}
	return newCloudTaskqueue(c, loc)
}
