package appwrap

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
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
