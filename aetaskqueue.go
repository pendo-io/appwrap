// +build !cloudtasks

package appwrap

import (
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/taskqueue"
)

type QueueStatistics = taskqueue.QueueStatistics
type Task = taskqueue.Task

func TaskDelay(task *Task) time.Duration {
	return task.Delay
}

func TaskSetDelay(task *Task, delay time.Duration) {
	task.Delay = delay
}

func TaskHeader(task *Task) http.Header {
	return task.Header
}

func TaskHost(task *Task) string {
	return task.Header.Get("Host")
}

func TaskSetHost(task *Task, hostname string) {
	if task.Header == nil {
		task.Header = make(http.Header)
	}
	task.Header.Set("Host", hostname)
}

func TaskMethod(task *Task) string {
	return task.Method
}

func TaskSetMethod(task *Task, method string) {
	task.Method = method
}

func TaskName(task *Task) string {
	return task.Name
}

func TaskSetName(task *Task, name string) {
	task.Name = name
}

func TaskPath(task *Task) string {
	return task.Path
}

func TaskPayload(task *Task) []byte {
	return task.Payload
}

func TaskSetPayload(task *Task, payload []byte) {
	task.Payload = payload
}

func TaskRetryCount(task *Task) int32 {
	return task.RetryCount
}

func TaskSetRetryCount(task *Task, count int32) {
	task.RetryCount = count
}

func TaskTag(task *Task) string {
	return task.Tag
}

func TaskSetTag(task *Task, tag string) {
	task.Tag = tag
}

type appengineTaskqueue struct {
	c context.Context
}

func NewTaskqueue(c context.Context) Taskqueue {
	return appengineTaskqueue{c}
}

func (t appengineTaskqueue) Add(c context.Context, task *Task, queueName string) (*Task, error) {
	return taskqueue.Add(c, task, queueName)
}

func (t appengineTaskqueue) AddMulti(c context.Context, tasks []*Task, queueName string) ([]*Task, error) {
	return taskqueue.AddMulti(c, tasks, queueName)
}

func (t appengineTaskqueue) DeleteMulti(c context.Context, tasks []*Task, queueName string) error {
	return taskqueue.DeleteMulti(c, tasks, queueName)
}

func (t appengineTaskqueue) Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]*Task, error) {
	return taskqueue.Lease(c, maxTasks, queueName, leaseTime)
}

func (t appengineTaskqueue) LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]*Task, error) {
	return taskqueue.LeaseByTag(c, maxTasks, queueName, leaseTime, tag)
}

func (t appengineTaskqueue) ModifyLease(c context.Context, task *Task, queueName string, leaseTime int) error {
	return taskqueue.ModifyLease(c, task, queueName, leaseTime)
}

func (t appengineTaskqueue) NewPOSTTask(path string, params url.Values) *Task {
	return taskqueue.NewPOSTTask(path, params)
}

func (t appengineTaskqueue) QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error) {
	return taskqueue.QueueStats(c, queueNames)
}
