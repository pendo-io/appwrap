package appwrap

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/appengine/taskqueue"
	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) TestTaskCopy(c *C) {
	task := &aeTaskImpl{
		task: &taskqueue.Task{
			Path:    "/path",
			Payload: []byte("payload"),
			Header: http.Header{
				"key": []string{"value"},
			},
			Method:       "POST",
			Name:         "name",
			Delay:        time.Duration(1),
			ETA:          time.Now(),
			RetryCount:   1,
			Tag:          "abc",
			RetryOptions: &taskqueue.RetryOptions{},
		},
		service: "service",
		version: "version",
	}

	taskCopy := task.Copy().(*aeTaskImpl)
	c.Assert(taskCopy, DeepEquals, task)
	// Pointers should not be the same
	c.Assert(task.task.Payload, Not(Equals), taskCopy.task.Payload)
	c.Assert(task.task.Header, Not(Equals), taskCopy.task.Header)
	c.Assert(task.task.Header["key"], Not(Equals), taskCopy.task.Header["key"])
	c.Assert(task.task.RetryOptions, Not(Equals), taskCopy.task.RetryOptions)
}

func (s *AppengineInterfacesTest) TestPopulateTaskHost(c *C) {
	task := newAeTask().(*aeTaskImpl)
	task.service = "test-service"
	tq := newAeTaskqueue(context.TODO(), "").(appengineTaskqueue)
	tq.populateTaskHosts([]Task{task})
	// no project id because we can't mock it, but that's fine for this
	c.Assert(task.host(), Equals, "test-service..appspot.com")

	task = newAeTask().(*aeTaskImpl)
	task.version = "test-version"
	tq.populateTaskHosts([]Task{task})
	c.Assert(task.host(), Equals, "test-version..appspot.com")

	task = newAeTask().(*aeTaskImpl)
	task2 := newAeTask().(*aeTaskImpl)
	task.service = "test-service"
	task.version = "test-version"
	tq.populateTaskHosts([]Task{task, task2})
	c.Assert(task.host(), Equals, "test-version.test-service..appspot.com")
	c.Assert(task2.host(), Equals, ".appspot.com")
}
