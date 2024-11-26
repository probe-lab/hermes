package host

import (
	"context"
	"encoding/json"
	"fmt"
)

type TraceLogger struct{}

var _ DataStream = (*TraceLogger)(nil)

func (t *TraceLogger) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (t *TraceLogger) Stop(ctx context.Context) error {
	return nil
}

func (t *TraceLogger) PutRecord(ctx context.Context, event *TraceEvent) error {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	fmt.Println(string(jsonBytes))
	return nil
}

func (t *TraceLogger) Type() DataStreamType {
	return DataStreamTypeLogger
}

// OutputType returns the output type to be used by this data stream.
func (t *TraceLogger) OutputType() DataStreamOutputType {
	return DataStreamOutputTypeFull
}
