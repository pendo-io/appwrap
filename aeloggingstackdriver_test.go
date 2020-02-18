package appwrap

import (
	"context"
	. "gopkg.in/check.v1"
)

type AeLoggingStackdriverTests struct {
	ctx         context.Context
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
}

func (s *AeLoggingStackdriverTests) TestAddLoggingLabels(c *C) {
	// adding a new key will keep the previous keys
	err := AddLoggingLabels(&s.ctx, map[string]string{"label3": "black"})
	c.Assert(err, IsNil)
	logCtxVal := s.ctx.Value(loggingCtxKey).(*loggingCtxValue)
	c.Assert(logCtxVal.labels, DeepEquals, map[string]string{
		"label1": "pink",
		"label2": "white",
		"label3": "black",
	})

	// using an existing key will override the original value
	err = AddLoggingLabels(&s.ctx, map[string]string{"label2": "black"})
	c.Assert(err, IsNil)
	logCtxVal = s.ctx.Value(loggingCtxKey).(*loggingCtxValue)
	c.Assert(logCtxVal.labels, DeepEquals, map[string]string{
		"label1": "pink",
		"label2": "black",
		"label3": "black",
	})
}

func (s *AeLoggingStackdriverTests) TestAddLoggingLabelsErrors(c *C) {
	badCtx := context.WithValue(StubContext(), loggingCtxKey, nil)
	err := AddLoggingLabels(&badCtx, map[string]string{"label1": "pink"})
	c.Assert(err.Error(), Equals, "failed to add log labels, need to wrap http handler to use stackdriver logger")
}
