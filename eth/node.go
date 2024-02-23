package eth

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/prysm/v4/network/forks"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type Node struct {
	cfg        *NodeConfig
	host       *host.Host
	supervisor *suture.Supervisor
	pool       *Pool

	// The addr info of Prysm's p2p endpoint
	prysmAddrInfo *peer.AddrInfo

	// clients
	pryClient *PrysmClient

	// servers
	reqResp *ReqResp

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

	genesisRoot := cfg.GenesisConfig.GenesisValidatorRoot
	genesisTime := cfg.GenesisConfig.GenesisTime

	digest, err := forks.CreateForkDigest(genesisTime, genesisRoot)
	if err != nil {
		return nil, fmt.Errorf("create fork digest (%s, %x): %w", genesisTime, genesisRoot, err)
	}

	reqRespCfg := &ReqRespConfig{
		ForkDigest:   digest,
		ReadTimeout:  cfg.BeaconConfig.TtfbTimeoutDuration(),
		WriteTimeout: cfg.BeaconConfig.RespTimeoutDuration(),
		Tracer:       cfg.Tracer,
		Meter:        cfg.Meter,
	}

	reqResp, err := NewReqResp(h, reqRespCfg)
	if err != nil {
		return nil, fmt.Errorf("new p2p server: %w", err)
	}

	// finally, initialize hermes node
	n := &Node{
		cfg:        cfg,
		host:       h,
		supervisor: suture.NewSimple("eth"),
		pool:       NewPool(),
		reqResp:    reqResp,
		pryClient:  NewPrysmClient(cfg.PrysmHost, cfg.PrysmPort),
		disc:       disc,
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	defer logDeferErr(n.host.Close, "Failed closing libp2p host")

	// identify the beacon node. We only have the host/port of the Beacon API
	// endpoint. If we want to establish a P2P connection, we need its peer ID
	// and dedicated p2p port. We call the identity endpoint to get this information
	slog.Info("Getting Prysm P2P Identity")
	addrInfo, err := n.pryClient.AddrInfo(ctx)
	if err != nil {
		return fmt.Errorf("get prysm node p2p addr info: %w", err)
	}

	slog.Info("Prysm P2P Identity:", tele.LogAttrPeerID(addrInfo.ID))
	for i, maddr := range addrInfo.Addrs {
		slog.Info(fmt.Sprintf("[%d] %s", i, maddr.String()))
	}

	// cache the address information on the node
	n.prysmAddrInfo = addrInfo

	// Set stream handlers on our libp2p host
	n.reqResp.RegisterHandlers(ctx)

	// Create a connection signal that fires when the Prysm node has connected
	// to us. Prysm will try this periodically AFTER we have registered ourselves
	// as a trusted peer. Therefore, we register the signal first and only
	// aftewards add ourselves as a trusted peer to not miss the signal
	// https://github.com/prysmaticlabs/prysm/issues/13659
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	connSignal := n.host.ConnSignal(timeoutCtx, addrInfo.ID)

	// register ourselves as a trusted peer
	self := peer.AddrInfo{ID: n.host.ID(), Addrs: n.host.Addrs()}
	slog.Info("Adding ourselves as a trusted peer to Prysm", tele.LogAttrPeerID(self.ID), "addr", self.Addrs)
	if err := n.pryClient.AddTrustedPeer(ctx, self); err != nil {
		return fmt.Errorf("failed adding ourself as trusted peer: %w", err)
	}
	defer func() {
		// unregister ourselves as a trusted peer from prysm. Context timeout
		// is not necessary because the pryClient applies a 5s timeout to each API call
		slog.Info("Removing ourselves as a trusted peer from Prysm", tele.LogAttrPeerID(self.ID))
		if err := n.pryClient.RemoveTrustedPeer(context.Background(), n.host.ID()); err != nil { // use new context, as the old one is likely cancelled
			slog.Warn("failed to remove ourself as a trusted peer", tele.LogAttrError(err))
		}
	}()

	// register the node itself as the notifiee for network connection events
	n.host.Network().Notify(n)

	slog.Info("Proactively trying to connect to Prysm", tele.LogAttrPeerID(addrInfo.ID), "maddrs", addrInfo.Addrs)
	if err := n.host.Connect(ctx, *addrInfo); err != nil {
		slog.Info("Connection to beacon node failed", tele.LogAttrError(err))
		slog.Info("Waiting for dialback from Prysm node")
	}

	if err := <-connSignal; err != nil {
		return fmt.Errorf("failed waiting for Prysm dialback: %w", err)
	}
	slog.Info("Prysm is connected!", tele.LogAttrPeerID(addrInfo.ID), "maddr", n.host.Peerstore().Addrs(addrInfo.ID))

	slog.Info("Wait for first status information from Prysm")

LOOP:
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.Tick(time.Second):
			slog.Info("Check if status is there")
			n.reqResp.statusMu.RLock()
			isInit := n.reqResp.status != nil
			n.reqResp.statusMu.RUnlock()
			if isInit {
				break LOOP
			}
		}
	}
	slog.Info("Found valid status")

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
