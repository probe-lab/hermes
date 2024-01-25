package pubsub

import (
	"context"
	"errors"
	"log/slog"

	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Subscription struct {
	topic   *pubsub.Topic
	sub     *pubsub.Subscription
	handler MessageHandler
}

func (s *Subscription) ReadMessages(ctx context.Context, self peer.ID) {
	topicAttr := slog.String("topic", s.topic.String())
	slog.Info("Starting to read messages for topic", topicAttr)
	defer slog.Info("Stopped reading messages for topic", topicAttr)

	for {
		msg, err := s.sub.Next(ctx)
		if err != nil {
			if errors.Is(err, ctx.Err()) {
				slog.Debug("subscription was cancelled", topicAttr)
				break
			}

			slog.Warn("failed reading next message from subscription", topicAttr)
			continue
		}

		if msg.ReceivedFrom == self {
			continue
		}

		_ = s.handler(msg)

	}
}
