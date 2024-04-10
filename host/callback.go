package host

import "context"

var _ DataStream = (*CallbackDataStream)(nil)

// CallbackDataStream is a simple implementation of DataStream that holds a callback function.
// Users of CallbackDataStream should ensure that the callback function does not block,
// as blocking can delay or disrupt the processing of subsequent events.
type CallbackDataStream struct {
	cb func(ctx context.Context, event *TraceEvent)

	stopped bool
}

// NewCallbackDataStream creates a new instance of CallbackDataStream.
func NewCallbackDataStream() *CallbackDataStream {
	return &CallbackDataStream{
		cb: func(ctx context.Context, event *TraceEvent) {
			// no-op
		},
		stopped: true,
	}
}

// Type returns the type of the data stream, which is DataStreamTypeCallback.
func (c *CallbackDataStream) Type() DataStreamType {
	return DataStreamTypeCallback
}

// OnEvent sets the callback function that will be called when an event is received.
func (c *CallbackDataStream) OnEvent(onRecord func(ctx context.Context, event *TraceEvent)) {
	c.cb = onRecord
}

// Start begins the data stream's operations.
func (c *CallbackDataStream) Start(ctx context.Context) error {
	c.stopped = false

	return ctx.Err()
}

// Stop ends the data stream's operation.
func (c *CallbackDataStream) Stop(ctx context.Context) error {
	c.stopped = true

	return ctx.Err()
}

// PutEvent sends an event to the callback if the stream has not been stopped.
func (c *CallbackDataStream) PutEvent(ctx context.Context, event *TraceEvent) error {
	if c.stopped {
		return ctx.Err()
	}

	if c.cb != nil && event != nil {
		c.cb(ctx, event)

		return nil
	}

	return ctx.Err()
}
