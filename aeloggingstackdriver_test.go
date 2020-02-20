package appwrap

import (
	"context"
	. "gopkg.in/check.v1"
)

type AeLoggingStackdriverTests struct {
	ctx   context.Context
	log   Logging
	sdLog stackdriverLogging
}

var _ = Suite(&AeLoggingStackdriverTests{})

func (s *AeLoggingStackdriverTests) SetUpTest(c *C) {
	logCtxVal := &loggingCtxValue{
		labels: map[string]string{
			"label1": "pink",
			"label2": "white",
		},
	}

	s.ctx = context.WithValue(StubContext(), loggingCtxKey, logCtxVal)
	s.sdLog = stackdriverLogging{c: s.ctx}
	s.log = NewLevelLogger(LogLevelDebug, s.sdLog)
}

func (s *AeLoggingStackdriverTests) TestAddLoggingLabels(c *C) {
	// adding a new key will keep the previous keys
	err := AddStackdriverLoggingLabels(s.log, map[string]string{"label3": "black"})
	c.Assert(err, IsNil)
	logCtxVal := s.sdLog.c.Value(loggingCtxKey).(*loggingCtxValue)
	c.Assert(logCtxVal.labels, DeepEquals, map[string]string{
		"label1": "pink",
		"label2": "white",
		"label3": "black",
	})

	// using an existing key will override the original value
	err = AddStackdriverLoggingLabels(s.log, map[string]string{"label2": "black"})
	c.Assert(err, IsNil)
	logCtxVal = s.sdLog.c.Value(loggingCtxKey).(*loggingCtxValue)
	c.Assert(logCtxVal.labels, DeepEquals, map[string]string{
		"label1": "pink",
		"label2": "black",
		"label3": "black",
	})
}

func (s *AeLoggingStackdriverTests) TestAddLoggingLabelsErrors(c *C) {
	errorLogger1 := stackdriverLogging{c: context.WithValue(StubContext(), loggingCtxKey, nil)}
	err := AddStackdriverLoggingLabels(errorLogger1, map[string]string{"label1": "pink"})
	c.Assert(err.Error(), Equals, "failed to add log labels, log needs to be a level logger")

	errorLogger2 := NewLevelLogger(LogLevelDebug, NullLogger{})
	err = AddStackdriverLoggingLabels(errorLogger2, map[string]string{"label1": "pink"})
	c.Assert(err.Error(), Equals, "failed to add log labels, wrapped log needs to be a stackdriver logger")

	errorLogger3 := NewLevelLogger(LogLevelDebug,  stackdriverLogging{c: context.WithValue(StubContext(), loggingCtxKey, nil)})
	err = AddStackdriverLoggingLabels(errorLogger3, map[string]string{"label1": "pink"})
	c.Assert(err.Error(), Equals, "failed to add log labels, needs to have a logging context")
}
