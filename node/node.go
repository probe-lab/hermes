package node

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	conmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	noise "github.com/libp2p/go-libp2p/p2p/security/noise"
	tcp_transport "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

var ConnNotChannSize = 256

type Network interface {
	Listen(ctx context.Context, h host.Host) error
	Close() error
}

type MessageHandler func(*pubsub.Message) (PersistableMsg, error)

type PersistableMsg interface {
	IsZero() bool
}

type Node struct {
	network Network
	cfg     NodeConfig

	host     host.Host
	identify identify.IDService

	topicHandlers      map[string]MessageHandler
	topicSubscriptions map[string]*TopicSubscription
}

type NodeConfig struct {
	PrivateKey     *crypto.Secp256k1PrivateKey
	UserAgent      string
	ListenIpAddr   string
	ListenPort     int
	BootstrapPeers []peer.ID
}

func (c *NodeConfig) Validate() error {
	return nil
}

func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		ListenIpAddr: "127.0.0.1",
		ListenPort:   7600,
	}
}

func NewNode(network Network, cfg *NodeConfig) (*Node, error) {
	if cfg == nil {
		cfg = DefaultNodeConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("node config: %w", err)
	}

	// create libp2p host
	multiaddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", cfg.ListenIpAddr, cfg.ListenPort))
	if err != nil {
		return nil, fmt.Errorf("parse listen addr: %w", err)
	}

	// connection manager
	low := 5000
	hi := 7000
	graceTime := 30 * time.Minute
	conMngr, err := conmgr.NewConnManager(low, hi, conmgr.WithGracePeriod(graceTime))
	if err != nil {
		return nil, fmt.Errorf("new conn manager: %w", err)
	}

	// Generate the main Libp2p host that will be exposed to the network
	host, err := libp2p.New(
		libp2p.ListenAddrs(multiaddr),
		libp2p.Identity(cfg.PrivateKey),
		libp2p.UserAgent(cfg.UserAgent),
		libp2p.Transport(tcp_transport.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(conMngr),
	)
	if err != nil {
		return nil, err
	}
	slog.Info("created libp2p host", "maddrs", multiaddr.String(), "peerID", host.ID().String())

	// identify service
	ids, err := identify.NewIDService(
		host,
		identify.UserAgent(cfg.UserAgent),
		identify.DisableSignedPeerRecord(),
	)
	if err != nil {
		return nil, err
	}

	n := &Node{
		host:               host,
		cfg:                *cfg,
		identify:           ids,
		network:            network,
		topicHandlers:      make(map[string]MessageHandler),
		topicSubscriptions: make(map[string]*TopicSubscription),
	}

	return n, nil
}

func (n *Node) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer n.Close()

	if err := n.host.Network().Listen(); err != nil {
		return fmt.Errorf("host listen: %v", err)
	}

	if err := n.network.Listen(ctx, n.host); err != nil {
		return fmt.Errorf("network start: %v", err)
	}

	if err := n.SubscribeTopics(ctx); err != nil {
		return fmt.Errorf("subscribe topics: %v", err)
	}

	<-ctx.Done()
	return ctx.Err()
}

func (n *Node) Close() {
	// TODO: stop subscriptions
	n.network.Close()
	n.host.Close()
}

func (n *Node) AddTopicHandler(topic string, handlerFn MessageHandler) {
	n.topicHandlers[topic] = handlerFn
}

func (n *Node) SubscribeTopics(ctx context.Context) error {
	gossipParams := pubsub.DefaultGossipSubParams()

	messageIdFn := func(pmsg *pubsub_pb.Message) string {
		h := sha256.New()
		// never errors, see crypto/sha256 Go doc

		_, _ = h.Write(pmsg.Data)
		id := h.Sum(nil)
		return base64.URLEncoding.EncodeToString(id)
	}

	tr, err := NewTracer()
	if err != nil {
		return fmt.Errorf("new tracer: %v", err)
	}

	// TODO: these are based on Filecoin, adjust for Ethereum
	const (
		GossipScoreThreshold             = -500
		PublishScoreThreshold            = -1000
		GraylistScoreThreshold           = -2500
		AcceptPXScoreThreshold           = 1000
		OpportunisticGraftScoreThreshold = 3.5
	)

	bootstrappers := make(map[peer.ID]struct{})
	for _, pi := range n.cfg.BootstrapPeers {
		bootstrappers[pi] = struct{}{}
	}

	opts := []pubsub.Option{
		// Gossipsubv1.1 configuration
		// pubsub.WithFloodPublish(true),

		pubsub.WithPeerScore(
			&pubsub.PeerScoreParams{
				AppSpecificScore: func(p peer.ID) float64 {
					// return a heavy positive score for bootstrappers so that we don't unilaterally prune them
					if _, ok := bootstrappers[p]; ok {
						return 2500
					}
					return 0
				},
				AppSpecificWeight: 1,

				// This sets the IP colocation threshold to 5 peers before we apply penalties
				IPColocationFactorThreshold: 5,
				IPColocationFactorWeight:    -100,
				// IPColocationFactorWhitelist: []*net.IPNet{},

				// P7: behavioural penalties, decay after 1hr
				BehaviourPenaltyThreshold: 6,
				BehaviourPenaltyWeight:    -10,
				BehaviourPenaltyDecay:     pubsub.ScoreParameterDecay(time.Hour),

				DecayInterval: pubsub.DefaultDecayInterval,
				DecayToZero:   pubsub.DefaultDecayToZero,

				// this retains non-positive scores for 6 hours
				RetainScore: 6 * time.Hour,

				// topic parameters
				// Topics:
			},
			&pubsub.PeerScoreThresholds{
				GossipThreshold:             GossipScoreThreshold,
				PublishThreshold:            PublishScoreThreshold,
				GraylistThreshold:           GraylistScoreThreshold,
				AcceptPXThreshold:           AcceptPXScoreThreshold,
				OpportunisticGraftThreshold: OpportunisticGraftScoreThreshold,
			},
		),

		pubsub.WithMessageSigning(false),
		pubsub.WithStrictSignatureVerification(false),
		pubsub.WithMessageIdFn(messageIdFn),
		pubsub.WithGossipSubParams(gossipParams),
		pubsub.WithEventTracer(tr),
		pubsub.WithPeerScoreInspect(tr.UpdatePeerScore, 10*time.Second),
	}
	ps, err := pubsub.NewGossipSub(ctx, n.host, opts...)
	if err != nil {
		return fmt.Errorf("new gossipsub: %v", err)
	}

	for topicName, handlerFn := range n.topicHandlers {
		topic, err := ps.Join(topicName)
		if err != nil {
			slog.Error("failed to join topic", "topic", topicName, "error", err)
			continue
		}
		sub, err := topic.Subscribe()
		if err != nil {
			slog.Error("failed to subscribe to topic", "topic", topicName, "error", err)
			continue
		}

		slog.Debug("subscribed to topic", "topic", topicName)
		ts := NewTopicSubscription(ctx, topic, sub, handlerFn)
		// Add the new Topic to the list of supported/subscribed topics in GossipSub
		n.topicSubscriptions[topicName] = ts
		go ts.MessageReadingLoop(n.host.ID())
	}

	return nil
}

type TopicSubscription struct {
	ctx       context.Context
	messages  chan []byte
	topic     *pubsub.Topic
	sub       *pubsub.Subscription
	handlerFn MessageHandler
}

func NewTopicSubscription(
	ctx context.Context,
	topic *pubsub.Topic,
	sub *pubsub.Subscription,
	msgHandlerFn MessageHandler,
) *TopicSubscription {
	return &TopicSubscription{
		ctx:       ctx,
		topic:     topic,
		sub:       sub,
		messages:  make(chan []byte),
		handlerFn: msgHandlerFn,
	}
}

// MessageReadingLoop pulls messages from the pubsub topic and pushes them onto the Messages channel
// and the underlaying msg metrics.
func (c *TopicSubscription) MessageReadingLoop(selfId peer.ID) {
	logger := slog.With("topic", c.sub.Topic())
	logger.Debug("read loop started")
	subsCtx := c.ctx
	for {
		msg, err := c.sub.Next(subsCtx)
		if err != nil {
			if err == subsCtx.Err() {
				logger.Error("context of subscription has been canceled", "error", err)
				break
			}
			logger.Error("error reading next message in topic", "error", err)
		} else {
			// To avoid getting track of our own messages, check if we are the senders
			if msg.ReceivedFrom != selfId {
				logger.Info("new message", "sender", msg.ReceivedFrom)
				// use the msg handler for that specific topic that we have
				content, err := c.handlerFn(msg)
				if err != nil {
					logger.Error("failed to handle message", "error", err)
					continue
				}
				if !content.IsZero() {
					logger.Debug("msg content", "content", content)
				}
			} else {
				logger.Debug("received message from self")
			}
		}
	}
	<-subsCtx.Done()
	logger.Debug("read loop finished")
}
