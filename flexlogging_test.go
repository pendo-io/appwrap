package appwrap

import (
	"context"

	. "gopkg.in/check.v1"
)

type FlexLoggingTests struct {
	ctx   context.Context
	sdLog stackdriverLogging
}

var _ = Suite(&FlexLoggingTests{})

func (s *FlexLoggingTests) SetUpTest(c *C) {
	logCtxVal := &loggingCtxValue{
		labels: map[string]string{
			"label1": "pink",
			"label2": "white",
		},
	}

	s.ctx = context.WithValue(StubContext(), loggingCtxKey, logCtxVal)
	s.sdLog = stackdriverLogging{c: s.ctx}
}

func (s *FlexLoggingTests) TestAddLoggingLabels(c *C) {
	// adding a new key will keep the previous keys
	err := s.sdLog.AddLabels(map[string]string{"label3": "black"})
	c.Assert(err, IsNil)
	logCtxVal := s.sdLog.c.Value(loggingCtxKey).(*loggingCtxValue)
	c.Assert(logCtxVal.labels, DeepEquals, map[string]string{
		"label1": "pink",
		"label2": "white",
		"label3": "black",
	})

	// using an existing key will override the original value
	err = s.sdLog.AddLabels(map[string]string{"label2": "black"})
	c.Assert(err, IsNil)
	logCtxVal = s.sdLog.c.Value(loggingCtxKey).(*loggingCtxValue)
	c.Assert(logCtxVal.labels, DeepEquals, map[string]string{
		"label1": "pink",
		"label2": "black",
		"label3": "black",
	})
}

func (s *FlexLoggingTests) TestAddLoggingLabelsErrors(c *C) {
	errorLogger := stackdriverLogging{c: context.WithValue(StubContext(), loggingCtxKey, nil)}
	err := errorLogger.AddLabels(map[string]string{"label1": "pink"})
	c.Assert(err.Error(), Equals, "failed to add log labels, needs to have a logging context")
}
