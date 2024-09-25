package host

import (
	"context"
	"encoding/json"
	"fmt"
)

type TraceLogger struct{}

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