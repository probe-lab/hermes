package host

import (
	"context"
	"log/slog"
)

type DataStream interface {
	Start(ctx context.Context)
	Stop()
	Put(data []byte, partitionKey string) error
}

type NoopDataStream struct{}

var _ DataStream = (*NoopDataStream)(nil)

func (n NoopDataStream) Start(ctx context.Context) {}

func (n NoopDataStream) Stop() {}

func (n NoopDataStream) Put(data []byte, partitionKey string) error {
	slog.Info(string(data), "partition_key", partitionKey)

	return nil
}
