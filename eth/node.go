package eth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/encoder"
	eth "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"
	"github.com/thejerf/suture/v4"
	"go.opentelemetry.io/otel/metric"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

// Node is the main entry point to listening to the Ethereum GossipSub mesh.
type Node struct {
	// The configuration that's passed in externally
	cfg *NodeConfig

	// The libp2p host, however, this is a custom Hermes wrapper host.
	host *host.Host

	// The suture supervisor that is the root of the service tree
	sup *suture.Supervisor

	// The addr info of Prysm's P2P endpoint
	pryInfo *peer.AddrInfo

	// A custom client to use various Prysm APIs
	pryClient *PrysmClient

	// The request/response protocol handlers as well as some client methods
	reqResp *ReqResp

	// The PubSub service that implements various gossipsub topics
	pubSub *PubSub

	// Peerer is another suture service, that ensures the registration as a trusted peer with the Prysm node
	peerer *Peerer

	// The discovery service, periodically querying the discv5 DHT network
	disc *Discovery

	// Metrics
	connCount   metric.Int64ObservableGauge
	connDurHist metric.Float64Histogram
}

// NewNode initializes a new [Node] using the provided configuration.
// It first validates the node configuration. Then it initializes the libp2p
// host using the libp2p options from the given configuration object. Next, it
// initializes the Ethereum node by extracting the ECDSA private key,
// creating a new discovery service, creating a new ReqResp server,
// creating a new PubSub server, and creating a new Prysm client.
// Finally, it initializes the Hermes node by setting the configuration and
// dependencies.
func NewNode(cfg *NodeConfig) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("node config validation failed: %w", err)
	}

	hostCfg := &host.Config{
		AWSConfig:     cfg.AWSConfig,
		KinesisRegion: cfg.KinesisRegion,
		KinesisStream: cfg.KinesisStream,
		Tracer:        cfg.Tracer,
		Meter:         cfg.Meter,
	}

	// initialize libp2p host
	opts, err := cfg.libp2pOptions()
	if err != nil {
		return nil, fmt.Errorf("build libp2p options: %w", err)
	}

	h, err := host.New(hostCfg, opts...)
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

	// initialize the request-response protocol handlers
	reqRespCfg := &ReqRespConfig{
		ForkDigest:   cfg.ForkDigest,
		Encoder:      encoder.SszNetworkEncoder{},
		ReadTimeout:  cfg.BeaconConfig.TtfbTimeoutDuration(),
		WriteTimeout: cfg.BeaconConfig.RespTimeoutDuration(),
		Tracer:       cfg.Tracer,
		Meter:        cfg.Meter,
	}

	reqResp, err := NewReqResp(h, reqRespCfg)
	if err != nil {
		return nil, fmt.Errorf("new p2p server: %w", err)
	}

	// initialize the pubsub topic handlers
	pubSubConfig := &PubSubConfig{
		ForkDigest: cfg.ForkDigest,
		Encoder:    encoder.SszNetworkEncoder{},
	}

	pubSub, err := NewPubSub(h, pubSubConfig)
	if err != nil {
		return nil, fmt.Errorf("new PubSub service: %w", err)
	}

	// initialize the custom Prysm client to communicate with its API
	pryClient, err := NewPrysmClient(cfg.PrysmHost, cfg.PrysmPortHTTP, cfg.PrysmPortGRPC, cfg.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("new prysm client")
	}

	// finally, initialize hermes node
	n := &Node{
		cfg:       cfg,
		host:      h,
		sup:       suture.NewSimple("eth"),
		reqResp:   reqResp,
		pubSub:    pubSub,
		pryClient: pryClient,
		peerer:    NewPeerer(h, pryClient),
		disc:      disc,
	}

	// initialize custom prometheus metrics
	if err := n.initMetrics(cfg); err != nil {
		return nil, fmt.Errorf("init metrics: %w", err)
	}

	return n, nil
}

// initMetrics initializes various prometheus metrics and stores the meters
// on the [Node] object.
func (n *Node) initMetrics(cfg *NodeConfig) (err error) {
	n.connDurHist, err = cfg.Meter.Float64Histogram(
		"connection_duration_min",
		metric.WithExplicitBucketBoundaries(0.5, 1, 5, 10, 50, 100, 500, 1000),
	)
	if err != nil {
		return fmt.Errorf("new connection_duration_min histogram: %w", err)
	}

	n.connCount, err = cfg.Meter.Int64ObservableGauge("connection_count")
	if err != nil {
		return fmt.Errorf("new connection_count gauge: %w", err)
	}

	_, err = cfg.Meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		obs.ObserveInt64(n.connCount, int64(len(n.host.Network().Peers())))
		return nil
	}, n.connCount)
	if err != nil {
		return fmt.Errorf("register connectin_count gauge callback: %w", err)
	}

	return nil
}

// Start starts the listening process.
func (n *Node) Start(ctx context.Context) error {
	defer logDeferErr(n.host.Close, "Failed closing libp2p host")

	// identify the beacon node. We only have the host/port of the Beacon API
	// endpoint. If we want to establish a P2P connection, we need its peer ID
	// and dedicated p2p port. We call the identity endpoint to get this information
	slog.Info("Getting Prysm P2P Identity...")
	addrInfo, err := n.pryClient.Identity(ctx)
	if err != nil {
		return fmt.Errorf("get prysm node p2p addr info: %w", err)
	}

	slog.Info("Prysm P2P Identity:", tele.LogAttrPeerID(addrInfo.ID))
	for i, maddr := range addrInfo.Addrs {
		slog.Info(fmt.Sprintf("  [%d] %s", i, maddr.String()))
	}

	// cache the address information on the node
	n.pryInfo = addrInfo
	n.reqResp.delegate = addrInfo.ID

	// Now we have the beacon node's identity. The next thing we need is its
	// current status. The status consists of the ForkDigest, FinalizedRoot,
	// FinalizedEpoch, HeadRoot, and HeadSlot. We need the status so that we
	// can reply with it upon status requests. This is just need for
	// bootstrapping purposes. Subsequent Status requests will be forwarded to
	// the beacon node, and the response will then be recorded and used from
	// then on in the future.
	slog.Info("Getting Prysm's chain head...")
	chainHead, err := n.pryClient.ChainHead(ctx)
	if err != nil {
		return fmt.Errorf("get finalized finality checkpoints: %w", err)
	}

	status := &eth.Status{
		ForkDigest:     n.cfg.ForkDigest[:],
		FinalizedRoot:  chainHead.FinalizedBlockRoot,
		FinalizedEpoch: chainHead.FinalizedEpoch,
		HeadRoot:       chainHead.HeadBlockRoot,
		HeadSlot:       chainHead.HeadSlot,
	}
	n.reqResp.SetStatus(status)

	// Set stream handlers on our libp2p host
	if err := n.reqResp.RegisterHandlers(ctx); err != nil {
		return fmt.Errorf("register RPC handlers: %w", err)
	}

	// initialize GossipSub
	n.pubSub.gs, err = n.host.InitGossipSub(ctx, n.cfg.pubsubOptions(n)...)
	if err != nil {
		return fmt.Errorf("init gossip sub: %w", err)
	}

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
	slog.Info("Adding ourselves as a trusted peer to Prysm", tele.LogAttrPeerID(self.ID), "maddrs", self.Addrs)
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

	// wait for the connection to Prysm, this will pass immediately if the
	// connection already exists.
	if err := <-connSignal; err != nil {
		return fmt.Errorf("failed waiting for Prysm dialback: %w", err)
	} else {
		slog.Info("Prysm is connected!", tele.LogAttrPeerID(addrInfo.ID), "maddr", n.host.Peerstore().Addrs(addrInfo.ID))
	}

	// start the discovery service to find peers in the discv5 DHT
	n.sup.Add(n.disc)

	// start the pubsub subscriptions and handlers
	n.sup.Add(n.pubSub)

	// start the peerer service that ensures our registration as a trusted peer
	n.sup.Add(n.peerer)

	// start the hermes host to trace gossipsub messages
	n.sup.Add(n.host)

	// start the peer dialers, that consume the discovered peers from
	// the discovery service up until MaxPeers.
	for i := 0; i < n.cfg.DialConcurrency; i++ {
		cs := &PeerDialer{
			host:     n.host,
			peerChan: n.disc.out,
			maxPeers: n.cfg.MaxPeers,
		}
		n.sup.Add(cs)
	}

	// start all long-running services
	return n.sup.Serve(ctx)
}

// logDeferErr executes the given function and logs the given error message
// in case of an error.
func logDeferErr(fn func() error, onErrMsg string) {
	if err := fn(); err != nil && !errors.Is(err, context.Canceled) {
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
