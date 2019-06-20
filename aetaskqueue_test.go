package appwrap

import (
	"google.golang.org/appengine/taskqueue"
	. "gopkg.in/check.v1"
	"net/http"
	"time"
)

func (s *AppengineInterfacesTest) TestTaskCopy(c *C) {
	task := &taskImpl{
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
			RetryOptions: nil,
		},
	}

	taskCopy := task.Copy().(*taskImpl)
	c.Assert(task, DeepEquals, taskCopy)
	// Pointers should not be the same
	c.Assert(task.task.Payload, Not(Equals), taskCopy.task.Payload)
	c.Assert(task.task.Header, Not(Equals), taskCopy.task.Header)
	c.Assert(task.task.Header["key"], Not(Equals), taskCopy.task.Header["key"])
}
