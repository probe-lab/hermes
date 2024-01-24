package pubsub

import (
	"context"
	"fmt"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func (h *Host) initGossipSub(ctx context.Context) (*pubsub.PubSub, error) {
	params := pubsub.DefaultGossipSubParams()

	opts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			return MsgID(s.genesisValidatorsRoot, pmsg)
		}),
		pubsub.WithSubscriptionFilter(s),
		pubsub.WithPeerOutboundQueueSize(int(s.cfg.QueueSize)),
		pubsub.WithMaxMessageSize(int(params.BeaconConfig().GossipMaxSize)),
		pubsub.WithValidateQueueSize(int(s.cfg.QueueSize)),
		pubsub.WithPeerScore(peerScoringParams()),
		pubsub.WithPeerScoreInspect(s.peerInspector, time.Minute),
		pubsub.WithGossipSubParams(params),
		pubsub.WithRawTracer(gossipTracer{host: s.host}),
	}

	gs, err := pubsub.NewGossipSub(ctx, h, opts...)
	if err != nil {
		return nil, fmt.Errorf("new gossipsub: %w", err)
	}

	return gs, nil
}
