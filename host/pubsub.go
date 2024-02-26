package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type TopicHandler = func(context.Context, *pubsub.Message) error

type TopicSubscription struct {
	Topic   string
	LocalID peer.ID
	Sub     *pubsub.Subscription
	Handler TopicHandler
}

func NoopHandler(ctx context.Context, msg *pubsub.Message) error {
	return nil
}

func (t *TopicSubscription) Serve(ctx context.Context) error {
	slog.Info("Starting pubsub subscription", "topic", t.Topic)
	defer slog.Info("Stopped pubsub subscription", "topic", t.Topic)

	defer t.Sub.Cancel()

	for {
		msg, err := t.Sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return fmt.Errorf("failed to read next gossip message for topic %s: %w", t.Sub, err)
		}

		// check if it's our own event
		if msg.ReceivedFrom == t.LocalID {
			continue
		}

		if err := t.Handler(ctx, msg); err != nil {
			return fmt.Errorf("handle gossip message for topic %s: %w", t.Sub, err)
		}
	}

	return nil
}
