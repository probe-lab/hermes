package eth

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/prysmaticlabs/prysm/v4/config/params"

	"github.com/libp2p/go-libp2p/core/network"
)

type StreamHandler func(ctx context.Context, s network.Stream)

type StreamHandlerConfig struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (h *StreamHandlerConfig) newHandler(ctx context.Context) StreamHandler {
	return func(ctx context.Context, s network.Stream) {
	}
}

func handlerMiddleware(ctx context.Context, next StreamHandler) func(s network.Stream) {
	return func(s network.Stream) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Recovered from panic", "err", r, "stack", debug.Stack())
			}
		}()

		// Resetting after closing is a no-op so defer a reset in case something goes wrong.
		// It's up to the handler to Close the stream (send an EOF) if
		// it successfully writes a response. We don't blindly call
		// Close here because we may have only written a partial
		// response.
		defer resetStream(s)

		ctx, cancel := context.WithTimeout(ctx, params.BeaconConfig().TtfbTimeoutDuration()) // TODO: don't take TTFB from global config but pass in
		defer cancel()

		next(ctx, s)
	}
}

func handlerPing(ctx context.Context, s network.Stream) {
	defer closeStream(s)
}

func closeStream(s network.Stream) {
	if err := s.Close(); err != nil {
		slog.Warn("Failed closing stream", slog.String("err", err.Error()))
	}
}

func resetStream(s network.Stream) {
	if err := s.Reset(); err != nil {
		slog.Warn("Failed closing stream", slog.String("err", err.Error()))
	}
}
