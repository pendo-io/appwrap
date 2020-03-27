package appwrap

import (
	"bytes"
	"net/http"
	"time"

	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	. "gopkg.in/check.v1"
)

type CloudTasksAppEngineTest struct{}

var _ = Suite(&CloudTasksAppEngineTest{})

func (s *CloudTasksAppEngineTest) SetUpTest(c *C) {
	var client cloudTasksClient = &tqClientMock{}
	tqClient = &client
}

func (s *CloudTasksAppEngineTest) TestCloudTaskCopy(c *C) {
	task := &cloudTaskAppEngineImpl{
		cloudTaskImpl{
			task: &taskspb.Task{
				MessageType: &taskspb.Task_AppEngineHttpRequest{
					AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
						AppEngineRouting: &taskspb.AppEngineRouting{
							Service:  "service",
							Version:  "version",
							Instance: "instance",
							Host:     "host",
						},
						HttpMethod: taskspb.HttpMethod_GET,
						Headers: map[string]string{
							"key": "value",
						},
						Body:        []byte("body"),
						RelativeUri: "/path",
					},
				},
			},
		},
	}

	taskCopy := task.Copy().(*cloudTaskAppEngineImpl)
	c.Assert(task, DeepEquals, taskCopy)
	// verify that all pointers and slices are different locations in memory
	c.Assert(task, Not(Equals), taskCopy)
	c.Assert(task.task, Not(Equals), taskCopy.task)
	c.Assert(task.task.GetMessageType(), Not(Equals), taskCopy.task.GetMessageType())
	c.Assert(task.task.GetAppEngineHttpRequest(), Not(Equals), taskCopy.task.GetAppEngineHttpRequest())
	c.Assert(sameMemory(task.task.GetAppEngineHttpRequest().Headers, taskCopy.task.GetAppEngineHttpRequest().Headers), IsFalse)
	c.Assert(sameMemory(task.task.GetAppEngineHttpRequest().Body, taskCopy.task.GetAppEngineHttpRequest().Body), IsFalse)
	c.Assert(task.task.GetAppEngineHttpRequest().GetAppEngineRouting(), Not(Equals), taskCopy.task.GetAppEngineHttpRequest().GetAppEngineRouting())

	// modifying one shouldn't touch the other
	task.task.GetAppEngineHttpRequest().Body[0] = 'a'
	c.Assert(task.task.GetAppEngineHttpRequest().Body, DeepEquals, []byte("aody"))
	c.Assert(taskCopy.task.GetAppEngineHttpRequest().Body, DeepEquals, []byte("body"))
}

func (s *CloudTasksAppEngineTest) TestNewTask(c *C) {
	task := NewAppEngineTask().(*cloudTaskAppEngineImpl)
	c.Assert(task.task, DeepEquals, &taskspb.Task{
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
				AppEngineRouting: &taskspb.AppEngineRouting{},
			},
		}},
	)
	c.Assert(task.task.GetAppEngineHttpRequest(), DeepEquals, &taskspb.AppEngineHttpRequest{AppEngineRouting: &taskspb.AppEngineRouting{}})
	c.Assert(task.task.GetAppEngineHttpRequest().GetAppEngineRouting(), DeepEquals, &taskspb.AppEngineRouting{})
}

func (s *CloudTasksAppEngineTest) TestTaskDelay(c *C) {
	task := NewAppEngineTask().(*cloudTaskAppEngineImpl)

	storedDelay := task.Delay()
	c.Assert(storedDelay, Equals, time.Duration(0))

	delay := 10 * time.Second
	task.SetDelay(delay)
	<-time.After(time.Millisecond)

	storedDelay = task.Delay()
	c.Assert(storedDelay > 9*time.Second, IsTrue)
	c.Assert(storedDelay < 10*time.Second, IsTrue)

	task.SetEta(time.Now().Add(20 * time.Second))
	<-time.After(time.Millisecond)
	storedDelay = task.Delay()
	c.Assert(storedDelay > 19*time.Second, IsTrue)
	c.Assert(storedDelay < 20*time.Second, IsTrue)

	delay = -10 * time.Second
	task.SetDelay(delay)
	c.Assert(task.Delay(), Equals, time.Duration(0))
}

func (s *CloudTasksAppEngineTest) TestTaskHeader(c *C) {
	task := NewAppEngineTask()

	storedHeader := task.Header()
	c.Assert(storedHeader, DeepEquals, http.Header(nil))

	header := http.Header{}
	task.SetHeader(header)

	storedHeader = task.Header()
	c.Assert(storedHeader, DeepEquals, http.Header{})

	header = http.Header{
		"fruit":          {"apple"},
		"multiple-fruit": {"blueberry", "raspberry"},
	}

	// don't support multiple values, should take first value of each set
	expectedHeader := http.Header{
		"fruit":          {"apple"},
		"multiple-fruit": {"blueberry"},
	}

	task.SetHeader(header)
	storedHeader = task.Header()

	c.Assert(storedHeader, DeepEquals, expectedHeader)
}

func (s *CloudTasksAppEngineTest) TestTaskMethod(c *C) {
	task := NewAppEngineTask()

	storedMethod := task.Method()
	c.Assert(storedMethod, Equals, "HTTP_METHOD_UNSPECIFIED")

	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"} {
		task.SetMethod(method)
		storedMethod = task.Method()
		c.Assert(storedMethod, Equals, method)
	}

	shouldPanic := func() {
		task.SetMethod("BANANA")
	}
	c.Assert(shouldPanic, PanicMatches, "invalid task method: BANANA")
	// should still be last set method
	storedMethod = task.Method()
	c.Assert(storedMethod, Equals, "OPTIONS")
}

func (s *CloudTasksAppEngineTest) TestTaskName(c *C) {
	task := NewAppEngineTask()

	storedName := task.Name()
	c.Assert(storedName, Equals, "")

	task.SetName("names are hard")
	storedName = task.Name()
	c.Assert(storedName, Equals, "names are hard")
}

func (s *CloudTasksAppEngineTest) TestTaskPath(c *C) {
	task := NewAppEngineTask()

	storedPath := task.Path()
	c.Assert(storedPath, Equals, "")

	task.SetPath("/store/fruit/banana")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/store/fruit/banana")

	task.SetPath("/what/the/pineapple")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/what/the/pineapple")

	// path escapes and keeps what it can
	task.SetPath("/http://this-is-not-a-path.com")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/http://this-is-not-a-path.com")

	// set back to something normal
	task.SetPath("/pineapple")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/pineapple")

	//clear
	task.SetPath("")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "")
}

func (s *CloudTasksAppEngineTest) TestTaskPayload(c *C) {
	task := NewAppEngineTask()

	storedPayload := task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte{}), IsTrue)

	task.SetPayload([]byte("buzz buzz I'm a bee"))
	storedPayload = task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte("buzz buzz I'm a bee")), IsTrue)
}

func (s *CloudTasksAppEngineTest) TestTaskRetryCount(c *C) {
	task := NewAppEngineTask()

	storedCount := task.RetryCount()
	c.Assert(storedCount, Equals, int32(0))

	task.SetRetryCount(int32(93))
	storedCount = task.RetryCount()
	c.Assert(storedCount, Equals, int32(93))
}
