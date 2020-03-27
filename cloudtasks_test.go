package appwrap

import (
	"context"
	"errors"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/mock"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	. "gopkg.in/check.v1"
	"net/url"
)

type CloudTasksTest struct{}

var _ = Suite(&CloudTasksTest{})

func (s *CloudTasksTest) SetUpTest(c *C) {
	var client cloudTasksClient = &tqClientMock{}
	tqClient = &client
}

type tqClientMock struct {
	mock.Mock
}

func (m *tqClientMock) CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	args := m.Called(ctx, req, opts)
	return args.Get(0).(*taskspb.Task), args.Error(1)
}

func (s *CloudTasksTest) TestNewAppEngineCloudTask(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	task := tq.NewAppEngineCloudTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}})
	c.Assert(task, DeepEquals, &cloudTaskAppEngineImpl{
		cloudTaskImpl{
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
		},
	})
}

func (s *CloudTasksTest) TestAddAppEngineTask(c *C) {
	location := CloudTasksLocation("disney-world")
	ctx := context.Background()

	tq := NewTaskqueue(ctx, location).(cloudTaskqueue)
	tq.project = "shopping"
	clientMock := tq.client.(*tqClientMock)

	checkMocks := func() {
		clientMock.AssertExpectations(c)
	}

	task := tq.NewAppEngineCloudTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}}).(*cloudTaskAppEngineImpl)
	expectTask := task.Copy().(*cloudTaskAppEngineImpl)

	clientMock.On("CreateTask", context.Background(), &taskspb.CreateTaskRequest{
		Task:   task.task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, []gax.CallOption(nil)).Return(task.task, nil).Once()

	added, err := tq.Add(ctx, task, "grocery-store")
	c.Assert(added, Not(Equals), expectTask)                    // not same pointer (copied)...
	c.Assert(added.getTask(), DeepEquals, expectTask.getTask()) // ...but has same content
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

	tasks := []CloudTask{
		tq.NewAppEngineCloudTask("/vegetables/potato", url.Values{"types": []string{"Russet", "Red", "White", "Sweet"}}),
		tq.NewAppEngineCloudTask("/fruits/apple", url.Values{"types": []string{"Granny Smith", "Red Delicious", "Golden Delicious"}}),
	}
	expectTasks := []CloudTask{
		tasks[0].Copy(),
		tasks[1].Copy(),
	}

	clientMock.On("CreateTask", context.Background(), &taskspb.CreateTaskRequest{
		Task:   tasks[0].(*cloudTaskAppEngineImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(tasks[0].(*cloudTaskAppEngineImpl).task, nil).Once()
	clientMock.On("CreateTask", context.Background(), &taskspb.CreateTaskRequest{
		Task:   tasks[1].(*cloudTaskAppEngineImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(tasks[1].(*cloudTaskAppEngineImpl).task, nil).Once()

	added, err := tq.AddMulti(ctx, tasks, "grocery-store")
	c.Assert(added[0], Not(Equals), expectTasks[0]) // not same pointer (copied)...
	c.Assert(added[1], Not(Equals), expectTasks[1]) //...
	for i, addedTask := range added {
		c.Assert(addedTask.getTask(), DeepEquals, expectTasks[i].getTask()) // ...but have same content
	}
	c.Assert(err, IsNil)
	// IsNil isn't enough - need to make sure it's not a nil slice (since MultiError is a slice type)
	c.Assert(err, Equals, nil)
	checkMocks()

	// error case on one task
	fatalErr := errors.New("disney land < disney world")

	clientMock.On("CreateTask", context.Background(), &taskspb.CreateTaskRequest{
		Task:   tasks[0].(*cloudTaskAppEngineImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return(tasks[0].(*cloudTaskAppEngineImpl).task, nil).Once()
	clientMock.On("CreateTask", context.Background(), &taskspb.CreateTaskRequest{
		Task:   tasks[1].(*cloudTaskAppEngineImpl).task,
		Parent: "projects/shopping/locations/disney-world/queues/grocery-store",
	}, ([]gax.CallOption)(nil)).Return((*taskspb.Task)(nil), fatalErr).Once()

	expectTasks = []CloudTask{
		tasks[0].Copy(),
		&cloudTaskAppEngineImpl{},
	}

	expectedErr := MultiError{
		nil,
		fatalErr,
	}
	added, err = tq.AddMulti(ctx, tasks, "grocery-store")
	c.Assert(added[0], Not(Equals), expectTasks[0])
	for i, task := range added {
		c.Assert(task.getTask(), DeepEquals, expectTasks[i].getTask())
	}
	c.Assert(err, DeepEquals, expectedErr)
	checkMocks()
}
