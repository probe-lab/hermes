package eth

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prysmaticlabs/prysm/v4/network/forks"
	"github.com/prysmaticlabs/prysm/v4/time/slots"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type Node struct {
	cfg        *NodeConfig
	host       *host.Host
	supervisor *suture.Supervisor
	pool       *Pool

	// only set if we know it
	beaconAddrInfo *peer.AddrInfo

	// clients
	p2pClient *P2PClient
	prrClient PeererClient
	beaClient BeaconClient

	// services
	disc *Discovery
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
	var prrClient PeererClient
	var beaClient BeaconClient
	switch cfg.BeaconType {
	case BeaconTypePrysm:
		slog.Info("Using Prysm trusted peerer client", "addr", cfg.BeaconHost, "port", cfg.BeaconPort)
		client := NewPrysmClient(cfg.BeaconHost, cfg.BeaconPort)
		client.tracer = cfg.Tracer
		prrClient = client
		beaClient = client
	case BeaconTypeOther:
		slog.Info("Using no-op trusted peerer client")
		prrClient = NoopPeererClient{}
		slog.Info("Using generic beacon api client", "addr", cfg.BeaconHost, "port", cfg.BeaconPort)
		beaClient = NewBeaconAPIClient(cfg.BeaconHost, cfg.BeaconPort)
	case BeaconTypeNone:
		slog.Info("Using no-op trusted peerer client")
		prrClient = NoopPeererClient{}
		slog.Info("Using no-op beacon api client")
		beaClient = NoopBeaconClient{}
	}

	// initialize ethereum node
	privKey, err := cfg.ECDSAPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("extract ecdsa private key: %w", err)
	}

	disc, err := NewDiscovery(privKey, &DiscoveryConfig{
		GenesisConfig: cfg.GenesisConfig,
		NetworkConfig: cfg.NetworkConfig,
		Addr:          cfg.Devp2pHost,
		UDPPort:       cfg.Devp2pPort,
		TCPPort:       cfg.Libp2pPort,
		Tracer:        cfg.Tracer,
		Meter:         cfg.Meter,
	})
	if err != nil {
		return nil, fmt.Errorf("new discovery service: %w", err)
	}

	epoch := slots.EpochsSinceGenesis(cfg.GenesisConfig.GenesisTime)
	forkDigest, err := forks.ForkDigestFromEpoch(epoch, cfg.GenesisConfig.GenesisValidatorRoot)
	if err != nil {
		return nil, err
	}

	p2pClient := NewP2PClient(h, forkDigest)

	// finally, initialize hermes node
	n := &Node{
		cfg:        cfg,
		host:       h,
		supervisor: suture.NewSimple("eth"),
		pool:       NewPool(),
		beaClient:  beaClient,
		prrClient:  prrClient,
		p2pClient:  p2pClient,
		disc:       disc,
	}

	// register the node itself as the notifiee for network connection events
	n.host.Network().Notify(n)

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	defer logDeferErr(n.host.Close, "Failed closing libp2p host")

	// give the libp2p host 1 minute to figure out its public addresses
	//timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	//defer cancel()
	//
	//_ = timeoutCtx
	//if err := n.host.WaitForPublicAddress(timeoutCtx); err != nil {
	//	return fmt.Errorf("failed waiting for public addresses: %w", err)
	//}

	// construct our own addr info
	self := peer.AddrInfo{
		ID:    n.host.ID(),
		Addrs: n.host.Addrs(),
	}

	// register ourselves as a trusted peer
	if err := n.prrClient.AddTrustedPeer(ctx, self); err != nil {
		return fmt.Errorf("failed adding ourself as trusted peer: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// unregister ourselves from the beacon node
		if err := n.prrClient.RemoveTrustedPeer(shutdownCtx, n.host.ID()); err != nil { // use new context
			slog.Warn("failed to remove ourself as a trusted peer", tele.LogAttrError(err))
		}
	}()

	// connect to the beacon node
	if n.cfg.BeaconType != BeaconTypeNone {
		addrInfo, err := n.beaconNodeAddrInfo(ctx)
		if err != nil {
			return fmt.Errorf("get beacon node p2p addr info: %w", err)
		}
		n.beaconAddrInfo = addrInfo

		slog.Info("Establishing connection to beacon node", tele.LogAttrPeerID(addrInfo.ID), "maddr", addrInfo.Addrs[0])
		if err := n.host.Connect(ctx, *addrInfo); err != nil {
			return fmt.Errorf("could not connect to beacon node: %w", err)
		}
		slog.Info("Connected to beacon node!", tele.LogAttrPeerID(addrInfo.ID), "maddr", addrInfo.Addrs[0])
	}

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

	// start all long-running services
	return n.supervisor.Serve(ctx)
}

// beaconNodeAddrInfo calls the beaconAPI /eth/v1/node/identity endpoint
// and extracts the beacon node addr info object.
func (n *Node) beaconNodeAddrInfo(ctx context.Context) (*peer.AddrInfo, error) {
	beaIdentity, err := n.beaClient.Identity(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to contact beacon node: %w", err)
	}

	beaAddrInfo := &peer.AddrInfo{Addrs: []ma.Multiaddr{}}
	for _, p2pMaddr := range beaIdentity.Data.P2PAddresses {
		maddr, err := ma.NewMultiaddr(p2pMaddr)
		if err != nil {
			return nil, fmt.Errorf("parse beacon node identity multiaddress: %w", err)
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("parse beacon node identity p2p maddr: %w", err)
		}

		if beaAddrInfo.ID.Size() != 0 && beaAddrInfo.ID != addrInfo.ID {
			return nil, fmt.Errorf("received inconsistend beacon node identity peer IDs %s != %s", beaAddrInfo.ID, addrInfo.ID)
		}

		beaAddrInfo.ID = addrInfo.ID
		beaAddrInfo.Addrs = append(beaAddrInfo.Addrs, addrInfo.Addrs...)
	}

	return beaAddrInfo, nil
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
