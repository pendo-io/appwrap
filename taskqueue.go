// +build !cloudtasks

package appwrap

import (
	"net/url"

	"golang.org/x/net/context"
)

type Taskqueue interface {
	Add(c context.Context, task *Task, queueName string) (*Task, error)
	AddMulti(c context.Context, tasks []*Task, queueName string) ([]*Task, error)
	DeleteMulti(c context.Context, tasks []*Task, queueName string) error
	Lease(c context.Context, maxTasks int, queueName string, leaseTime int) ([]*Task, error)
	LeaseByTag(c context.Context, maxTasks int, queueName string, leaseTime int, tag string) ([]*Task, error)
	ModifyLease(c context.Context, task *Task, queueName string, leaseTime int) error
	NewPOSTTask(path string, params url.Values) *Task
	QueueStats(c context.Context, queueNames []string) ([]QueueStatistics, error)
}
