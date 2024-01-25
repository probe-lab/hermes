package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

type MessageHandler func(*pubsub.Message) error

type HostConfig struct {
	UserAgent  string
	Host       string
	Port       int
	PrivateKey *crypto.Secp256k1PrivateKey
}

func (c *HostConfig) Validate() error {
	if c.UserAgent == "" {
		return fmt.Errorf("user agent must not be empty")
	}

	return nil
}

type Host struct {
	host.Host

	cfg *HostConfig
	// subs map[string]*Subscription
}

func NewHost(cfg *HostConfig) (*Host, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("host config: %w", err)
	}

	// connection manager
	low := 5000
	hi := 7000
	graceTime := 30 * time.Minute

	connMgr, err := connmgr.NewConnManager(low, hi, connmgr.WithGracePeriod(graceTime))
	if err != nil {
		return nil, fmt.Errorf("new conn manager: %w", err)
	}

	// Generate the main libp2p host that will be exposed to the network
	h, err := libp2p.New(
		libp2p.Identity(cfg.PrivateKey),
		libp2p.UserAgent(cfg.UserAgent),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", cfg.Host, cfg.Port)),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connMgr),
	)
	if err != nil {
		return nil, fmt.Errorf("new libp2p host: %w", err)
	}

	psHost := &Host{
		Host: h,
		cfg:  cfg,
	}

	slog.Info("Initialized local libp2p host",
		"maddrs", psHost.Addrs(),
		slog.String("userAgent", cfg.UserAgent),
		slog.String("peerID", psHost.ID().String()),
	)

	return psHost, nil
}

type RunConfig struct {
	PubSubOptions []pubsub.Option
	TopicHandlers map[string]MessageHandler
}

func (c *RunConfig) Validate() error {
	return nil
}

func (h *Host) Run(ctx context.Context, cfg *RunConfig) error {
	gs, err := pubsub.NewGossipSub(ctx, h, cfg.PubSubOptions...)
	if err != nil {
		return fmt.Errorf("new gossipsub: %w", err)
	}

	var wg sync.WaitGroup
	for topic, handler := range cfg.TopicHandlers {
		psTopic, err := gs.Join(topic)
		if err != nil {
			return fmt.Errorf("join topic %s: %w", topic, err)
		}

		psSub, err := psTopic.Subscribe()
		if err != nil {
			return fmt.Errorf("subscribe to topic %s: %w", topic, err)
		}

		sub := &Subscription{
			topic:   psTopic,
			sub:     psSub,
			handler: handler,
		}

		wg.Add(1)
		go func() {
			sub.ReadMessages(ctx, h.ID())
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}
