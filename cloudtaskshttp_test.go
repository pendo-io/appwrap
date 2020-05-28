package appwrap

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/googleapis/gax-go/v2"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	. "gopkg.in/check.v1"
)

type HttpCloudTasksTest struct{}

var _ = Suite(&HttpCloudTasksTest{})

func (s *HttpCloudTasksTest) SetUpTest(c *C) {
	var client cloudTasksClient = &tqClientMock{}
	tqClient = &client
}

func (s *HttpCloudTasksTest) TestCloudTaskCopy(c *C) {
	task := &cloudTaskHttpImpl{
		task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					HttpMethod: taskspb.HttpMethod_POST,
					Headers: map[string]string{
						"key": "value",
					},
					Body: []byte("body"),
					Url:  "https://api.example.com/vegetables/potato",
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: "feedback@service.account",
						},
					},
				},
			},
		},
	}

	taskCopy := task.Copy().(*cloudTaskHttpImpl)
	//c.Assert(task, DeepEquals, taskCopy)
	// verify that all pointers and slices are different locations in memory
	c.Assert(task, Not(Equals), taskCopy)
	c.Assert(task.task, Not(Equals), taskCopy.task)
	c.Assert(task.task.GetHttpRequest().AuthorizationHeader, Not(Equals), taskCopy.task.GetHttpRequest().AuthorizationHeader)
	c.Assert(task.task.GetHttpRequest().GetOidcToken(), Not(Equals), taskCopy.task.GetHttpRequest().GetOidcToken())
	c.Assert(task.task.GetMessageType(), Not(Equals), taskCopy.task.GetMessageType())
	c.Assert(task.task.GetHttpRequest(), Not(Equals), taskCopy.task.GetHttpRequest())
	c.Assert(sameMemory(task.task.GetHttpRequest().Headers, taskCopy.task.GetHttpRequest().Headers), IsFalse)
	c.Assert(sameMemory(task.task.GetHttpRequest().Body, taskCopy.task.GetHttpRequest().Body), IsFalse)

	// modifying one shouldn't touch the other
	task.task.GetHttpRequest().Body[0] = 'a'
	c.Assert(task.task.GetHttpRequest().Body, DeepEquals, []byte("aody"))
	c.Assert(taskCopy.task.GetHttpRequest().Body, DeepEquals, []byte("body"))
}

func (s *HttpCloudTasksTest) TestNewHttpTask(c *C) {
	task := NewHttpCloudTask("foo@example.com").(*cloudTaskHttpImpl)
	c.Assert(task.task, DeepEquals, &taskspb.Task{
		MessageType: &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{
				AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
					OidcToken: &taskspb.OidcToken{
						ServiceAccountEmail: "foo@example.com",
					},
				},
			},
		}},
	)
}

func (s *HttpCloudTasksTest) TestHttpTaskDelay(c *C) {
	task := NewHttpCloudTask("foo@example.com").(*cloudTaskHttpImpl)

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

func (s *HttpCloudTasksTest) TestHttpTaskHeader(c *C) {
	task := NewHttpCloudTask("feedback@service.account")

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

func (s *HttpCloudTasksTest) TestHttpTaskMethod(c *C) {
	task := NewHttpCloudTask("feedback@service.account")

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

func (s *HttpCloudTasksTest) TestHttpTaskName(c *C) {
	task := NewHttpCloudTask("feedback@service.account")

	storedName := task.Name()
	c.Assert(storedName, Equals, "")

	task.SetName("names are hard")
	storedName = task.Name()
	c.Assert(storedName, Equals, "names are hard")
}

func (s *HttpCloudTasksTest) TestHttpTaskPayload(c *C) {
	task := NewHttpCloudTask("feedback@service.account")

	storedPayload := task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte{}), IsTrue)

	task.SetPayload([]byte("buzz buzz I'm a bee"))
	storedPayload = task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte("buzz buzz I'm a bee")), IsTrue)
}

func (s *HttpCloudTasksTest) TestHttpTaskRetryCount(c *C) {
	task := NewHttpCloudTask("feedback@service.account")

	storedCount := task.RetryCount()
	c.Assert(storedCount, Equals, int32(0))

	task.SetRetryCount(int32(93))
	storedCount = task.RetryCount()
	c.Assert(storedCount, Equals, int32(93))
}

func (m *tqClientMock) CreateHttpTask(ctx context.Context, req *taskspb.CreateTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	args := m.Called(ctx, req, opts)
	return args.Get(0).(*taskspb.Task), args.Error(1)
}

func (s *HttpCloudTasksTest) TestNewHttpPOSTTask(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	task := tq.NewHttpCloudTask("foo@example.com", "https://api.example.com/vegetables/potato", http.MethodPost, []byte("{ vegetables: [{'type': 'Russet', 'tasty': true] }"), headers).(*cloudTaskHttpImpl)
	c.Assert(task, DeepEquals, &cloudTaskHttpImpl{
		task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        "https://api.example.com/vegetables/potato",
					HttpMethod: taskspb.HttpMethod_POST,
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
					Body: []byte("{ vegetables: [{'type': 'Russet', 'tasty': true] }"),
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: "foo@example.com",
						},
					},
				},
			},
		},
	})
}

func (s *HttpCloudTasksTest) TestHttpAdd(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	tq.project = "shopping"
	clientMock := tq.client.(*tqClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	data := "{ vegetables: [{'type': 'Russet', 'tasty': true] }"
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	task := tq.NewHttpCloudTask("foo@example.com", "https://api.example.com/vegetables/potato", http.MethodPost, []byte(data), headers).(*cloudTaskHttpImpl)
	expectTask := task.Copy().(*cloudTaskHttpImpl)

	clientMock.On("CreateTask", context.Background(), &taskspb.CreateTaskRequest{
		Task:   task.task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, []gax.CallOption(nil)).Return(task.task, nil).Once()

	added, err := tq.Add(ctx, task, "grocery-store")
	c.Assert(added, Not(Equals), expectTask) // not same pointer (copied)...
	_, isHttpTask := added.(HttpTask)
	c.Assert(isHttpTask, IsTrue)
	c.Assert(added.(*cloudTaskHttpImpl).task, DeepEquals, expectTask.task) // ...but has same content
	c.Assert(err, IsNil)
	checkMocks()
}
