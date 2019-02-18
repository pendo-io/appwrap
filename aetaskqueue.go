// +build !cloudtasks

package appwrap

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine/taskqueue"
	"net/url"
)

type QueueStatistics = taskqueue.QueueStatistics
type Task = taskqueue.Task

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
