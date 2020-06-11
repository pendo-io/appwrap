package appwrap

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/timestamp"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

type cloudTaskHttpImpl struct {
	task *taskspb.Task
}

func newHttpCloudTask(serviceAccount string) HttpTask {
	return &cloudTaskHttpImpl{
		task: &taskspb.Task{
			DispatchDeadline: &duration.Duration{Seconds: 1800},
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: serviceAccount,
						},
					},
				},
			},
		},
	}
}

func (t cloudTaskHttpImpl) Copy() CloudTask {
	innerCopy := *t.task.GetHttpRequest()
	bodyCopy := make([]byte, len(innerCopy.Body))
	copy(bodyCopy, innerCopy.Body)
	headerCopy := make(map[string]string, len(innerCopy.Headers))
	for k, v := range innerCopy.Headers {
		headerCopy[k] = v
	}
	deadlineCopy := t.task.DispatchDeadline
	serviceAccountCopy := make([]byte, len(innerCopy.GetOidcToken().ServiceAccountEmail))
	copy(serviceAccountCopy, innerCopy.GetOidcToken().ServiceAccountEmail)
	taskCopy := &cloudTaskHttpImpl{
		task: &taskspb.Task{
			DispatchDeadline: deadlineCopy,
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					HttpMethod: innerCopy.HttpMethod,
					Headers:    headerCopy,
					Body:       bodyCopy,
					Url:        innerCopy.Url,
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: string(serviceAccountCopy),
						},
					},
				},
			},
		},
	}

	return taskCopy
}

func (t *cloudTaskHttpImpl) isTask() {}

func (t *cloudTaskHttpImpl) Delay() (delay time.Duration) {
	if sched := t.task.ScheduleTime; sched == nil {
	} else {
		delay = time.Unix(sched.Seconds, int64(sched.Nanos)).Sub(time.Now())
	}
	if delay < 0 {
		return time.Duration(0)
	}
	return
}

func (t *cloudTaskHttpImpl) SetDelay(delay time.Duration) {
	eta := time.Now().Add(delay)
	t.SetEta(eta)
}

func (t *cloudTaskHttpImpl) SetEta(eta time.Time) {
	t.task.ScheduleTime = &timestamp.Timestamp{
		Seconds: eta.Unix(),
		Nanos:   int32(eta.Nanosecond()),
	}
}

func (t *cloudTaskHttpImpl) Name() string {
	return t.task.Name
}

func (t *cloudTaskHttpImpl) SetName(name string) {
	t.task.Name = name
}

func (t *cloudTaskHttpImpl) RetryCount() int32 {
	return t.task.DispatchCount
}

func (t *cloudTaskHttpImpl) SetRetryCount(count int32) {
	t.task.DispatchCount = count
}

func (t *cloudTaskHttpImpl) Tag() (tag string) {
	panic("not implemented for CloudTasks")
}

func (t *cloudTaskHttpImpl) SetTag(tag string) {
	panic("not implemented for CloudTasks")
}

func (t *cloudTaskHttpImpl) Header() http.Header {
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

func (t *cloudTaskHttpImpl) SetHeader(header http.Header) {
	req := t.task.GetHttpRequest()
	reqHeader := make(map[string]string, len(header))
	for key, value := range header {
		reqHeader[key] = value[0]
	}
	req.Headers = reqHeader
}

func (t *cloudTaskHttpImpl) Method() string {
	req := t.task.GetHttpRequest()
	return taskspb.HttpMethod_name[int32(req.HttpMethod)]
}

func (t *cloudTaskHttpImpl) SetMethod(method string) {
	if val := taskspb.HttpMethod_value[method]; val != 0 {
		req := t.task.GetHttpRequest()
		req.HttpMethod = taskspb.HttpMethod(val)
	} else {
		panic(fmt.Sprintf("invalid task method: %s", method))
	}
}

func (t *cloudTaskHttpImpl) Url() string {
	req := t.task.GetHttpRequest()
	return req.Url
}

func (t *cloudTaskHttpImpl) SetUrl(url string) {
	req := t.task.GetHttpRequest()
	req.Url = url
}

func (t *cloudTaskHttpImpl) Payload() []byte {
	req := t.task.GetHttpRequest()
	return req.Body
}

func (t *cloudTaskHttpImpl) SetPayload(payload []byte) {
	req := t.task.GetHttpRequest()
	req.Body = payload
}

func (t cloudTaskqueue) NewHttpCloudTask(queueName string, url string, method string, data []byte, headers http.Header) HttpTask {
	task := NewHttpCloudTask(queueName)
	headers.Set("Content-Type", "application/json")
	task.SetMethod(method)
	task.SetPayload(data)
	task.SetHeader(headers)
	task.SetUrl(url)
	return task
}
