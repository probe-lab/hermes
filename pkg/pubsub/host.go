package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

type Config struct {
	UserAgent      string
	Host           string
	Port           int
	PrivateKey     *crypto.Secp256k1PrivateKey
	BootstrapPeers []peer.ID
}

func (c *Config) Validate() error {
	return nil
}

type Host struct {
	host.Host

	cfg *Config
}

func NewHost(cfg *Config) (*Host, error) {
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

func (h *Host) Start(ctx context.Context) error {
	gs, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return fmt.Errorf("new gossipsub: %w", err)
	}

	_ = gs

	return nil
}
