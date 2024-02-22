package eth

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type Node struct {
	cfg        *NodeConfig
	host       *host.Host
	supervisor *suture.Supervisor
	pool       *Pool

	// services
	peerer *Peerer
	disc   *Discovery
}

func NewNode(cfg *NodeConfig) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("node config validation failed: %w", err)
	}

	// initialize libp2p host
	opts, err := cfg.libp2pOptions()
	if err != nil {
		return nil, fmt.Errorf("build libp2p options: %w", err)
	}

	h, err := host.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("new libp2p host: %w", err)
	}
	slog.Info("Initialized new libp2p Host", tele.LogAttrPeerID(h.ID()))

	// initialize peerer client that we'll use to register this Hermes node as a
	// trusted peer with the beacon client that we delegate requests to.
	var tpc PeererClient
	if cfg.BeaconType == BeaconTypePrysm {
		addr, port, err := cfg.BeaconHostPort()
		if err != nil {
			return nil, fmt.Errorf("extract prysm api host and port: %w", err)
		}

		slog.Info("Init Prysm JSON RPC client", "addr", addr, "port", port)
		client := NewPrysmClient(addr, port)
		client.tracer = cfg.Tracer
		tpc = client
	} else {
		slog.Info("Using no-op trusted peerer client")
		tpc = NoopPeererClient{}
	}

	// initialize ethereum node
	privKey, err := cfg.ECDSAPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("extract ecdsa private key: %w", err)
	}

	disc, err := NewDiscovery(privKey, &DiscoveryConfig{
		GenesisConfig: cfg.GenesisConfig,
		NetworkConfig: cfg.NetworkConfig,
		Addr:          cfg.Devp2pAddr,
		UDPPort:       cfg.Devp2pPort,
		TCPPort:       cfg.Libp2pPort,
		Tracer:        cfg.Tracer,
		Meter:         cfg.Meter,
	})
	if err != nil {
		return nil, fmt.Errorf("new discovery service: %w", err)
	}

	// finally, initialize hermes node
	n := &Node{
		cfg:        cfg,
		host:       h,
		supervisor: suture.NewSimple("eth"),
		pool:       NewPool(),
		peerer:     NewPeerer(h, tpc),
		disc:       disc,
	}

	// register the node itself as the notifiee for network connection events
	n.host.Network().Notify(n)

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	// start peerer service that takes care of becoming a trusted peer
	// with our beacon node
	n.supervisor.Add(n.peerer)

	// start the discovery service to find peers in the discv5 DHT
	n.supervisor.Add(n.disc)

	// start the peer dialers, that consume the discovered peers from
	// the discovery service up until MaxPeers.
	for i := 0; i < n.cfg.DialerCount; i++ {
		cs := &PeerDialer{
			host:     n.host,
			pool:     n.pool,
			peerChan: n.disc.out,
			maxPeers: n.cfg.MaxPeers,
		}
		n.supervisor.Add(cs)
	}

	return n.supervisor.Serve(ctx)
}

// logDeferErr executes the given function and logs the given error message
// in case of an error.
func logDeferErr(fn func() error, onErrMsg string) {
	if err := fn(); err != nil {
		slog.Warn(onErrMsg, tele.LogAttrError(err))
	}
}

// terminateSupervisorTreeOnErr can be used like
//
//	defer func() { err = terminateSupervisorTreeOnErr(err) }()
//
// to instruct the suture supervisor to terminate the supervisor tree if
// the surrounding function returns an error.
func terminateSupervisorTreeOnErr(err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", err, suture.ErrTerminateSupervisorTree)
	}
	return nil
}

// wrapTermSupTree can be used to wrap a [suture.ErrTerminateSupervisorTree]
// error in the given error.
func wrapTermSupTree(err error) error {
	return fmt.Errorf("%s: %w", err, suture.ErrTerminateSupervisorTree)
}
