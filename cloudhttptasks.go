package appwrap

import (
	"fmt"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"net/http"
	"os"
)

type cloudHttpTaskImpl struct {
	cloudTaskImpl
}

func newHttpCloudTask() HttpTask {
	return &cloudHttpTaskImpl{
		cloudTaskImpl: cloudTaskImpl{
			task: &taskspb.Task{
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{
						AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
							OidcToken: &taskspb.OidcToken{
								ServiceAccountEmail: os.Getenv("FDBK_CLOUD_TASKS_SERVICE_ACCOUNT"),
							},
						},
					},
				},
			},
		},
	}
}

func (t *cloudHttpTaskImpl) Copy() HttpTask {
	innerCopy := *t.task.GetHttpRequest()
	bodyCopy := make([]byte, len(innerCopy.Body))
	copy(bodyCopy, innerCopy.Body)
	headerCopy := make(map[string]string, len(innerCopy.Headers))
	for k, v := range innerCopy.Headers {
		headerCopy[k] = v
	}
	taskCopy := &cloudHttpTaskImpl{
		cloudTaskImpl: cloudTaskImpl{
			task: &taskspb.Task{
				MessageType: &taskspb.Task_HttpRequest{
					HttpRequest: &taskspb.HttpRequest{
						HttpMethod:          innerCopy.HttpMethod,
						Headers:             headerCopy,
						Body:                bodyCopy,
						Url:                 innerCopy.Url,
						AuthorizationHeader: innerCopy.AuthorizationHeader,
					},
				},
			},
		},
	}
	return taskCopy
}

func (t *cloudHttpTaskImpl) Header() http.Header {
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

func (t *cloudHttpTaskImpl) SetHeader(header http.Header) {
	req := t.task.GetHttpRequest()
	reqHeader := make(map[string]string, len(header))
	for key, value := range header {
		reqHeader[key] = value[0]
	}
	req.Headers = reqHeader
}

func (t *cloudHttpTaskImpl) Method() string {
	req := t.task.GetHttpRequest()
	return taskspb.HttpMethod_name[int32(req.HttpMethod)]
}

func (t *cloudHttpTaskImpl) SetMethod(method string) {
	if val := taskspb.HttpMethod_value[method]; val != 0 {
		req := t.task.GetHttpRequest()
		req.HttpMethod = taskspb.HttpMethod(val)
	} else {
		panic(fmt.Sprintf("invalid task method: %s", method))
	}
}

func (t *cloudHttpTaskImpl) Url() string {
	req := t.task.GetHttpRequest()
	return req.Url
}

func (t *cloudHttpTaskImpl) SetUrl(url string) {
	req := t.task.GetHttpRequest()
	req.Url = url
}

func (t *cloudHttpTaskImpl) Payload() []byte {
	req := t.task.GetHttpRequest()
	return req.Body
}

func (t *cloudHttpTaskImpl) SetPayload(payload []byte) {
	req := t.task.GetHttpRequest()
	req.Body = payload
}

func (t cloudTaskqueue) NewHttpPOSTTask(url string, data []byte, headers http.Header) HttpTask {
	task := NewHttpCloudTask()
	headers.Set("Content-Type", "application/json")
	task.SetMethod("POST")
	task.SetPayload(data)
	task.SetHeader(headers)
	task.SetUrl(url)
	return task
}
