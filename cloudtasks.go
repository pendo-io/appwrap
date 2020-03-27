package appwrap

import (
	"github.com/golang/protobuf/ptypes/timestamp"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"time"
)

type cloudTaskImpl struct {
	task *taskspb.Task
}

func (t *cloudTaskImpl) isTask() {}

func (t *cloudTaskImpl) Delay() (delay time.Duration) {
	if sched := t.task.ScheduleTime; sched == nil {
	} else {
		delay = time.Unix(sched.Seconds, int64(sched.Nanos)).Sub(time.Now())
	}
	if delay < 0 {
		return time.Duration(0)
	}
	return
}

func (t *cloudTaskImpl) getTask() *taskspb.Task {
	return t.task
}

func (t *cloudTaskImpl) Copy() CloudTask {
	panic("Copy should be implemented in the specific task")
}

func (t *cloudTaskImpl) SetDelay(delay time.Duration) {
	eta := time.Now().Add(delay)
	t.SetEta(eta)
}

func (t *cloudTaskImpl) SetEta(eta time.Time) {
	t.task.ScheduleTime = &timestamp.Timestamp{
		Seconds: eta.Unix(),
		Nanos:   int32(eta.Nanosecond()),
	}
}

func (t *cloudTaskImpl) Name() string {
	return t.task.Name
}

func (t *cloudTaskImpl) SetName(name string) {
	t.task.Name = name
}

func (t *cloudTaskImpl) RetryCount() int32 {
	return t.task.DispatchCount
}

func (t *cloudTaskImpl) SetRetryCount(count int32) {
	t.task.DispatchCount = count
}

func (t *cloudTaskImpl) Tag() (tag string) {
	panic("not implemented for CloudTasks")
}

func (t *cloudTaskImpl) SetTag(tag string) {
	panic("not implemented for CloudTasks")
}
