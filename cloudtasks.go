// +build cloudtasks

package appwrap

import (
	"context"
	"net/http"
	"net/url"
	"time"

	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
)

type Task = taskspb.Task

type QueueStatistics struct{}

func TaskDelay(task *Task) time.Duration {
	return time.Duration(0)
}

func TaskSetDelay(task *Task, delay time.Duration) {

}

func TaskHeader(task *Task) http.Header {
	return nil
}

func TaskHost(task *Task) string {
	return ""
}

func TaskSetHost(task *Task, hostname string) {

}

func TaskMethod(task *Task) string {
	return ""
}

func TaskSetMethod(task *Task, method string) {

}

func TaskName(task *Task) string {
	return task.Name
}

func TaskSetName(task *Task, name string) {

}

func TaskPath(task *Task) string {
	return ""
}

func TaskPayload(task *Task) []byte {
	return nil
}

func TaskSetPayload(task *Task, payload []byte) {

}

func TaskRetryCount(task *Task) int32 {
	return 0
}

func TaskSetRetryCount(task *Task, count int32) {

}

func TaskTag(task *Task) string {
	return ""
}

func TaskSetTag(task *Task, tag string) {

}

type cloudTaskqueue struct {
	ctx context.Context
}

func NewTaskqueue(c context.Context) Taskqueue {
	return cloudTaskqueue{ctx: c}
}

func (t cloudTaskqueue) Add(c context.Context, task *Task, queueName string) (*Task, error) {
	return nil, nil
}

func (t cloudTaskqueue) AddMulti(c context.Context, tasks []*Task, queueName string) ([]*Task, error) {
	return nil, nil
}

func (t cloudTaskqueue) DeleteMulti(c context.Context, tasks []*Task, queueName string) error {
	return nil
}

func (t cloudTaskqueue) Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]*Task, error) {
	return nil, nil
}

func (t cloudTaskqueue) LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]*Task, error) {
	return nil, nil
}

func (t cloudTaskqueue) ModifyLease(c context.Context, task *Task, queueName string, leaseTime int) error {
	return nil
}

func (t cloudTaskqueue) NewPOSTTask(path string, params url.Values) *Task {
	return nil
}

func (t cloudTaskqueue) QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error) {
	return nil, nil
}
