// +build cloudtasks

package appwrap

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/mock"
	"google.golang.org/appengine"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta3"
	. "gopkg.in/check.v1"
)

func (s *AppengineInterfacesTest) TestTaskCopy(c *C) {
	task := &taskImpl{
		task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        "url",
					HttpMethod: taskspb.HttpMethod_GET,
					Headers: map[string]string{
						"key": "value",
					},
					Body:                []byte("body"),
					AuthorizationHeader: nil,
				},
			},
		},
	}

	taskCopy := task.Copy().(*taskImpl)
	c.Assert(task, DeepEquals, taskCopy)
	// verify that all pointers and slices are different locations in memory
	c.Assert(task, Not(Equals), taskCopy)
	c.Assert(task.task, Not(Equals), taskCopy.task)
	c.Assert(task.task.PayloadType, Not(Equals), taskCopy.task.PayloadType)
	c.Assert(task.task.GetHttpRequest(), Not(Equals), taskCopy.task.GetHttpRequest())
	c.Assert(task.task.GetHttpRequest().Headers, Not(Equals), taskCopy.task.GetHttpRequest().Headers)
	c.Assert(task.task.GetHttpRequest().Body, Not(Equals), taskCopy.task.GetHttpRequest().Body)

	// modifying one shouldn't touch the other
	task.task.GetHttpRequest().Body[0] = 'a'
	c.Assert(task.task.GetHttpRequest().Body, DeepEquals, []byte("aody"))
	c.Assert(taskCopy.task.GetHttpRequest().Body, DeepEquals, []byte("body"))
}

func (s *AppengineInterfacesTest) TestNewTask(c *C) {
	task := NewTask().(*taskImpl)
	c.Assert(task.task, DeepEquals, &taskspb.Task{
		PayloadType: &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{},
		}},
	)
	c.Assert(task.task.GetHttpRequest(), DeepEquals, &taskspb.HttpRequest{})
}

func (s *AppengineInterfacesTest) TestTaskDelay(c *C) {
	task := NewTask().(*taskImpl)

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

func (s *AppengineInterfacesTest) TestTaskHeader(c *C) {
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

func (s *AppengineInterfacesTest) TestTaskHost(c *C) {
	task := NewTask().(*taskImpl)

	storedHost := task.Host()
	c.Assert(storedHost, Equals, "")

	task.SetHost("pendo.io")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "pendo.io")
	// should set scheme to https as well
	storedRawUrl := task.task.GetHttpRequest().GetUrl()
	u, err := url.Parse(storedRawUrl)
	c.Assert(err, IsNil)
	c.Assert(u.Scheme, Equals, "https")

	task.SetHost("not-the-best.com")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "not-the-best.com")
	// scheme should still be https
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	u, err = url.Parse(storedRawUrl)
	c.Assert(err, IsNil)
	c.Assert(u.Scheme, Equals, "https")

	// if you do this, you're gonna have a bad time
	shouldPanic := func() {
		task.SetHost("https://all-the-special-ch@racters.com")
	}
	c.Assert(shouldPanic, PanicMatches, "invalid host: https://all-the-special-ch@racters.com")
	// shouldn't have changed
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "not-the-best.com")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	u, err = url.Parse(storedRawUrl)
	c.Assert(err, IsNil)
	c.Assert(u.Scheme, Equals, "https")

	// scheme should be cleared when host is cleared, to allow for relative paths
	task.SetHost("")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	u, err = url.Parse(storedRawUrl)
	c.Assert(err, IsNil)
	c.Assert(u.Scheme, Equals, "")
}

func (s *AppengineInterfacesTest) TestTaskMethod(c *C) {
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

func (s *AppengineInterfacesTest) TestTaskName(c *C) {
	task := NewTask()

	storedName := task.Name()
	c.Assert(storedName, Equals, "")

	task.SetName("names are hard")
	storedName = task.Name()
	c.Assert(storedName, Equals, "names are hard")
}

func (s *AppengineInterfacesTest) TestTaskPath(c *C) {
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

func (s *AppengineInterfacesTest) TestTaskHostAndPath(c *C) {
	// this test focuses on making sure Host and Path don't clobber each other,
	// since they're modifying the same underlying url string

	// set host, then path
	task := NewTask().(*taskImpl)
	task.SetHost("potato.com")
	task.SetPath("/harvest")
	storedHost := task.Host()
	c.Assert(storedHost, Equals, "potato.com")
	storedPath := task.Path()
	c.Assert(storedPath, Equals, "/harvest")
	storedRawUrl := task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com/harvest")

	// set path, then host
	task = NewTask().(*taskImpl)
	task.SetPath("/harvest")
	task.SetHost("potato.com")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "potato.com")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/harvest")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com/harvest")

	// set path and host, then modify path
	task = NewTask().(*taskImpl)
	task.SetPath("/harvest")
	task.SetHost("potato.com")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "potato.com")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/harvest")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com/harvest")
	task.SetPath("/plant")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/plant")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com/plant")

	// set path and host, then modify host
	task = NewTask().(*taskImpl)
	task.SetPath("/harvest")
	task.SetHost("potato.com")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "potato.com")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/harvest")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com/harvest")
	task.SetHost("blueberry.com")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "blueberry.com")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://blueberry.com/harvest")

	// set path and host, clear host
	task = NewTask().(*taskImpl)
	task.SetPath("/harvest")
	task.SetHost("potato.com")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "potato.com")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/harvest")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com/harvest")
	task.SetHost("")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "/harvest")

	// set path and host, clear path
	task = NewTask().(*taskImpl)
	task.SetPath("/harvest")
	task.SetHost("potato.com")
	storedHost = task.Host()
	c.Assert(storedHost, Equals, "potato.com")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "/harvest")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com/harvest")
	task.SetPath("")
	storedPath = task.Path()
	c.Assert(storedPath, Equals, "")
	storedRawUrl = task.task.GetHttpRequest().GetUrl()
	c.Assert(storedRawUrl, Equals, "https://potato.com")
}

func (s *AppengineInterfacesTest) TestTaskPayload(c *C) {
	task := NewTask()

	storedPayload := task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte{}), IsTrue)

	task.SetPayload([]byte("buzz buzz I'm a bee"))
	storedPayload = task.Payload()
	c.Assert(bytes.Equal(storedPayload, []byte("buzz buzz I'm a bee")), IsTrue)
}

func (s *AppengineInterfacesTest) TestTaskRetryCount(c *C) {
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

type CloudTasksTest struct{}

var _ = Suite(&CloudTasksTest{})

func (s *CloudTasksTest) SetUpTest(c *C) {
	tqClient = &tqClientMock{}
}

func (s *CloudTasksTest) TestNewPOSTTask(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	task := tq.NewPOSTTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}})
	c.Assert(task, DeepEquals, &taskImpl{
		task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        "/vegetables/potato",
					HttpMethod: taskspb.HttpMethod_POST,
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
	rand.Seed(0)
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	tq.project = "shopping"
	clientMock := tq.client.(*tqClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	task := tq.NewPOSTTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}}).(*taskImpl)
	taskCopy := task.Copy()

	expectedInnerTask := *task.task
	expectedInnerTask.Name = "projects/shopping/locations/disney-world/queues/grocery-store/tasks/1123914753"
	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   &expectedInnerTask,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(&expectedInnerTask, nil).Once()
	expectedTask := &taskImpl{
		task: &expectedInnerTask,
	}

	added, err := tq.Add(ctx, task, "grocery-store")
	c.Assert(added, DeepEquals, expectedTask)
	c.Assert(err, IsNil)
	// make sure task wasn't modified during add
	c.Assert(task, DeepEquals, taskCopy)
	checkMocks()

	expectedInnerTask = *task.task
	expectedInnerTask.Name = "projects/shopping/locations/disney-world/queues/grocery-store/tasks/2144551360"
	fatalErr := errors.New("disney land < disney world")
	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   &expectedInnerTask,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return((*taskspb.Task)(nil), fatalErr).Once()

	added, err = tq.Add(ctx, task, "grocery-store")
	c.Assert(added, DeepEquals, &taskImpl{})
	c.Assert(err, Equals, fatalErr)
	c.Assert(task, DeepEquals, taskCopy)
	checkMocks()
}

func (s *CloudTasksTest) TestAddMulti(c *C) {
	rand.Seed(0)
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

	tasksCopy := []Task{
		tq.NewPOSTTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}}),
		tq.NewPOSTTask("/fruits/apple", url.Values{"types": []string{"Granny Smith", "Red Delicious", "Golden Delicious"}}),
	}

	expectedInnerTasks := []taskspb.Task{
		*tasks[0].(*taskImpl).task,
		*tasks[1].(*taskImpl).task,
	}
	expectedInnerTasks[0].Name = "projects/shopping/locations/disney-world/queues/grocery-store/tasks/1123914753"
	expectedInnerTasks[1].Name = "projects/shopping/locations/disney-world/queues/grocery-store/tasks/2144551360"

	expectedTasks := []Task{
		&taskImpl{
			task: &expectedInnerTasks[0],
		},
		&taskImpl{
			task: &expectedInnerTasks[1],
		},
	}

	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   &expectedInnerTasks[0],
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(&expectedInnerTasks[0], nil).Once()
	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   &expectedInnerTasks[1],
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(&expectedInnerTasks[1], nil).Once()

	added, err := tq.AddMulti(ctx, tasks, "grocery-store")
	c.Assert(added, DeepEquals, expectedTasks)
	c.Assert(err, IsNil)
	// make sure task wasn't modified during add
	c.Assert(tasks, DeepEquals, tasksCopy)
	checkMocks()

	// error case on one task
	fatalErr := errors.New("disney land < disney world")

	expectedInnerTasks[0].Name = "projects/shopping/locations/disney-world/queues/grocery-store/tasks/1332660339"
	expectedInnerTasks[1].Name = "projects/shopping/locations/disney-world/queues/grocery-store/tasks/1760470370"

	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   &expectedInnerTasks[0],
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(&expectedInnerTasks[0], nil).Once()
	clientMock.On("CreateTask", ctx, &taskspb.CreateTaskRequest{
		Task:   &expectedInnerTasks[1],
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return((*taskspb.Task)(nil), fatalErr).Once()

	expectedTasks = []Task{
		expectedTasks[0],
		&taskImpl{},
	}

	expectedErr := appengine.MultiError{
		nil,
		fatalErr,
	}
	added, err = tq.AddMulti(ctx, tasks, "grocery-store")
	c.Assert(added, DeepEquals, expectedTasks)
	c.Assert(err, DeepEquals, expectedErr)
	// make sure task wasn't modified during add
	c.Assert(tasks, DeepEquals, tasksCopy)
	checkMocks()
}
