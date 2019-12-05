package appwrap

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/mock"
	"google.golang.org/appengine"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	. "gopkg.in/check.v1"
)

type CloudTasksTest struct{}

var _ = Suite(&CloudTasksTest{})

func (s *CloudTasksTest) SetUpTest(c *C) {
	var client cloudTasksClient = &tqClientMock{}
	tqClient = &client
	useCloudTasks = true
}

func (s *CloudTasksTest) TestCloudTaskCopy(c *C) {
	task := &cloudTaskImpl{
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
	}

	taskCopy := task.Copy().(*cloudTaskImpl)
	c.Assert(task, DeepEquals, taskCopy)
	// verify that all pointers and slices are different locations in memory
	c.Assert(task, Not(Equals), taskCopy)
	c.Assert(task.task, Not(Equals), taskCopy.task)
	c.Assert(task.task.MessageType, Not(Equals), taskCopy.task.MessageType)
	c.Assert(task.task.GetAppEngineHttpRequest(), Not(Equals), taskCopy.task.GetAppEngineHttpRequest())
	c.Assert(task.task.GetAppEngineHttpRequest().Headers, Not(Equals), taskCopy.task.GetAppEngineHttpRequest().Headers)
	c.Assert(task.task.GetAppEngineHttpRequest().Body, Not(Equals), taskCopy.task.GetAppEngineHttpRequest().Body)
	c.Assert(task.task.GetAppEngineHttpRequest().GetAppEngineRouting(), Not(Equals), taskCopy.task.GetAppEngineHttpRequest().GetAppEngineRouting())

	// modifying one shouldn't touch the other
	task.task.GetAppEngineHttpRequest().Body[0] = 'a'
	c.Assert(task.task.GetAppEngineHttpRequest().Body, DeepEquals, []byte("aody"))
	c.Assert(taskCopy.task.GetAppEngineHttpRequest().Body, DeepEquals, []byte("body"))
}

func (s *CloudTasksTest) TestNewTask(c *C) {
	task := NewTask().(*cloudTaskImpl)
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

func (s *CloudTasksTest) TestTaskDelay(c *C) {
	task := NewTask().(*cloudTaskImpl)

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

func (s *CloudTasksTest) TestTaskHeader(c *C) {
	task := NewTask()

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

func (s *CloudTasksTest) TestTaskMethod(c *C) {
	task := NewTask()

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

func (s *CloudTasksTest) TestTaskName(c *C) {
	task := NewTask()

	storedName := task.Name()
	c.Assert(storedName, Equals, "")

	task.SetName("names are hard")
	storedName = task.Name()
	c.Assert(storedName, Equals, "names are hard")
}

func (s *CloudTasksTest) TestTaskPath(c *C) {
	task := NewTask()

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

func (s *CloudTasksTest) TestTaskPayload(c *C) {
	task := NewTask()

	storedPayload := task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte{}), IsTrue)

	task.SetPayload([]byte("buzz buzz I'm a bee"))
	storedPayload = task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte("buzz buzz I'm a bee")), IsTrue)
}

func (s *CloudTasksTest) TestTaskRetryCount(c *C) {
	task := NewTask()

	storedCount := task.RetryCount()
	c.Assert(storedCount, Equals, int32(0))

	task.SetRetryCount(int32(93))
	storedCount = task.RetryCount()
	c.Assert(storedCount, Equals, int32(93))
}

type tqClientMock struct {
	mock.Mock
}

func (m *tqClientMock) CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	args := m.Called(ctx, req, opts)
	return args.Get(0).(*taskspb.Task), args.Error(1)
}

func (s *CloudTasksTest) TestNewPOSTTask(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	task := tq.NewPOSTTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}})
	c.Assert(task, DeepEquals, &cloudTaskImpl{
		task: &taskspb.Task{
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					AppEngineRouting: &taskspb.AppEngineRouting{},
					RelativeUri:      "/vegetables/potato",
					HttpMethod:       taskspb.HttpMethod_POST,
					Headers: map[string]string{
						"Content-Type": "application/x-www-form-urlencoded",
					},
					Body: []byte("types=Russet&types=Red&types=White&types=Sweet"),
				},
			},
		},
	})
}

func (s *CloudTasksTest) TestAdd(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	tq.project = "shopping"
	clientMock := tq.client.(*tqClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	task := tq.NewPOSTTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}}).(*cloudTaskImpl)
	expectTask := task.Copy().(*cloudTaskImpl)

	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   task.task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, []gax.CallOption(nil)).Return(task.task, nil).Once()

	added, err := tq.Add(ctx, task, "grocery-store")
	c.Assert(added, Not(Equals), expectTask) // not same pointer (copied)...
	c.Assert(added, DeepEquals, expectTask)  // ...but has same content
	c.Assert(err, IsNil)
	checkMocks()
}

func (s *CloudTasksTest) TestAddMulti(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	tq.project = "shopping"
	clientMock := tq.client.(*tqClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	tasks := []Task{
		tq.NewPOSTTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}}),
		tq.NewPOSTTask("/fruits/apple", url.Values{"types": []string{"Granny Smith", "Red Delicious", "Golden Delicious"}}),
	}
	expectTasks := []Task{
		tasks[0].Copy(),
		tasks[1].Copy(),
	}

	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   tasks[0].(*cloudTaskImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(tasks[0].(*cloudTaskImpl).task, nil).Once()
	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   tasks[1].(*cloudTaskImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(tasks[1].(*cloudTaskImpl).task, nil).Once()

	added, err := tq.AddMulti(ctx, tasks, "grocery-store")
	c.Assert(added[0], Not(Equals), expectTasks[0]) // not same pointer (copied)...
	c.Assert(added[1], Not(Equals), expectTasks[1]) //...
	c.Assert(added, DeepEquals, expectTasks)        // ...but have same content
	c.Assert(err, IsNil)
	checkMocks()

	// error case on one task
	fatalErr := errors.New("disney land < disney world")

	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   tasks[0].(*cloudTaskImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(tasks[0].(*cloudTaskImpl).task, nil).Once()
	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   tasks[1].(*cloudTaskImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return((*taskspb.Task)(nil), fatalErr).Once()

	expectTasks = []Task{
		tasks[0].Copy(),
		&cloudTaskImpl{},
	}

	expectedErr := appengine.MultiError{
		nil,
		fatalErr,
	}
	added, err = tq.AddMulti(ctx, tasks, "grocery-store")
	c.Assert(added[0], Not(Equals), expectTasks[0])
	c.Assert(added, DeepEquals, expectTasks)
	c.Assert(err, DeepEquals, expectedErr)
	checkMocks()
}
