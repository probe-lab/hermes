package eth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	eth "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	gk "github.com/dennis-tra/go-kinesis"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/thejerf/suture/v4"
	"go.opentelemetry.io/otel/attribute"
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

	// The data stream to which we transmit data
	ds host.DataStream

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
	connCount     metric.Int64ObservableGauge
	connDurHist   metric.Float64Histogram
	connBeacon    metric.Int64ObservableGauge
	connAge       metric.Float64Histogram
	connMedianAge metric.Float64ObservableGauge

	// eventCallbacks contains a list of callbacks that are executed when an event is received
	eventCallbacks []func(ctx context.Context, event *host.TraceEvent)
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

	// configure the global variables for the network ForkVersions
	initNetworkForkVersions(cfg.BeaconConfig)

	var ds host.DataStream
	switch cfg.DataStreamType {
	case host.DataStreamTypeLogger:
		ds = new(host.TraceLogger)

	case host.DataStreamTypeKinesis:
		droppedTraces, err := cfg.Meter.Int64Counter("dropped_traces")
		if err != nil {
			return nil, fmt.Errorf("new dropped_traces counter: %w", err)
		}

		notifiee := &gk.NotifieeBundle{
			DroppedRecordF: func(ctx context.Context, record gk.Record) {
				tevt, ok := record.(*host.TraceEvent)
				if !ok {
					droppedTraces.Add(ctx, 1, metric.WithAttributes(attribute.String("evt_type", "UNKNOWN")))
				} else {
					droppedTraces.Add(ctx, 1, metric.WithAttributes(attribute.String("evt_type", tevt.Type)))
				}
				slog.Warn("Dropped record", "partition_key", record.PartitionKey(), "size", len(record.Data()))
			},
		}

		pcfg := gk.DefaultProducerConfig()
		pcfg.Log = slog.Default()
		pcfg.Meter = cfg.Meter
		pcfg.Notifiee = notifiee
		pcfg.RetryLimit = 5

		p, err := gk.NewProducer(kinesis.NewFromConfig(*cfg.AWSConfig), cfg.KinesisStream, pcfg)
		if err != nil {
			return nil, fmt.Errorf("new kinesis producer: %w", err)
		}

		ds = host.NewKinesisDataStream(p)

	case host.DataStreamTypeCallback:
		ds = host.NewCallbackDataStream()

	case host.DataStreamTypeS3:
		// get the metrics tracer and meter from the root config
		cfg.S3Config.Meter = cfg.Meter
		var err error
		ds, err = host.NewS3DataStream(*cfg.S3Config)
		if err != nil {
			return nil, fmt.Errorf("new s3 producer %w", err)
		}

	case host.DataStreamTypeNoop:
		ds = new(host.NoopDataStream)

	default:
		return nil, fmt.Errorf("not recognised data-stream (%s)", cfg.DataStreamType)
	}

	hostCfg := &host.Config{
		DataStream:            ds,
		PeerscoreSnapshotFreq: cfg.Libp2pPeerscoreSnapshotFreq,
		Tracer:                cfg.Tracer,
		Meter:                 cfg.Meter,
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
	slog.Info("Initialized new libp2p Host", tele.LogAttrPeerID(h.ID()), "maddrs", h.Addrs())

	// initialize ethereum node
	privKey, err := cfg.ECDSAPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("extract ecdsa private key: %w", err)
	}

	disc, err := NewDiscovery(privKey, &DiscoveryConfig{
		GenesisConfig:           cfg.GenesisConfig,
		NetworkConfig:           cfg.NetworkConfig,
		AttestationSubnetConfig: cfg.SubnetConfigs[p2p.GossipAttestationMessage],
		SyncSubnetConfig:        cfg.SubnetConfigs[p2p.GossipSyncCommitteeMessage],
		Addr:                    cfg.Devp2pHost,
		UDPPort:                 cfg.Devp2pPort,
		TCPPort:                 cfg.Libp2pPort,
		Tracer:                  cfg.Tracer,
		Meter:                   cfg.Meter,
	})
	if err != nil {
		return nil, fmt.Errorf("new discovery service: %w", err)
	}
	slog.Info("Initialized new devp2p Node", "enr", disc.node.Node().String())

	// initialize the request-response protocol handlers
	reqRespCfg := &ReqRespConfig{
		ForkDigest:              cfg.ForkDigest,
		Encoder:                 cfg.RPCEncoder,
		AttestationSubnetConfig: cfg.SubnetConfigs[p2p.GossipAttestationMessage],
		SyncSubnetConfig:        cfg.SubnetConfigs[p2p.GossipSyncCommitteeMessage],
		DataStream:              ds,
		ReadTimeout:             cfg.BeaconConfig.TtfbTimeoutDuration(),
		WriteTimeout:            cfg.BeaconConfig.RespTimeoutDuration(),
		Tracer:                  cfg.Tracer,
		Meter:                   cfg.Meter,
	}

	reqResp, err := NewReqResp(h, reqRespCfg)
	if err != nil {
		return nil, fmt.Errorf("new p2p server: %w", err)
	}

	// initialize the pubsub topic handlers
	pubSubConfig := &PubSubConfig{
		Topics:         cfg.getDesiredFullTopics(cfg.GossipSubMessageEncoder),
		ForkVersion:    cfg.ForkVersion,
		Encoder:        cfg.GossipSubMessageEncoder,
		SecondsPerSlot: time.Duration(cfg.BeaconConfig.SecondsPerSlot) * time.Second,
		GenesisTime:    cfg.GenesisConfig.GenesisTime,
		DataStream:     ds,
	}

	pubSub, err := NewPubSub(h, pubSubConfig)
	if err != nil {
		return nil, fmt.Errorf("new PubSub service: %w", err)
	}

	// initialize the custom Prysm client to communicate with its API
	pryClient, err := NewPrysmClientWithTLS(cfg.PrysmHost, cfg.PrysmPortHTTP, cfg.PrysmPortGRPC, cfg.PrysmUseTLS, cfg.DialTimeout, cfg.GenesisConfig)
	if err != nil {
		return nil, fmt.Errorf("new prysm client: %w", err)
	}
	// check if Prysm is valid
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	onNetwork, err := pryClient.isOnNetwork(ctx, cfg.ForkDigest)
	if err != nil {
		return nil, fmt.Errorf("prysm client: %w", err)
	}
	if !onNetwork {
		return nil, fmt.Errorf("prysm client not in correct fork_digest")
	}

	// finally, initialize hermes node
	n := &Node{
		cfg:            cfg,
		host:           h,
		ds:             ds,
		sup:            suture.NewSimple("eth"),
		reqResp:        reqResp,
		pubSub:         pubSub,
		pryClient:      pryClient,
		peerer:         NewPeerer(h, pryClient, cfg.LocalTrustedAddr),
		disc:           disc,
		eventCallbacks: []func(ctx context.Context, event *host.TraceEvent){},
	}

	if ds.Type() == host.DataStreamTypeCallback {
		cbDs := ds.(*host.CallbackDataStream)

		cbDs.OnEvent(func(ctx context.Context, event *host.TraceEvent) {
			for _, cb := range n.eventCallbacks {
				cb(ctx, event)
			}
		})

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
		return fmt.Errorf("register connection_count gauge callback: %w", err)
	}

	n.connBeacon, err = cfg.Meter.Int64ObservableGauge("beacon_connected", metric.WithDescription("Tracks the standing connection to our beacon node (1=connected, 0=disconnected)"))
	if err != nil {
		return fmt.Errorf("new beacon_connected gauge: %w", err)
	}

	_, err = cfg.Meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		if n.pryInfo != nil && len(n.host.Network().ConnsToPeer(n.pryInfo.ID)) > 0 {
			obs.ObserveInt64(n.connBeacon, 1)
		} else {
			obs.ObserveInt64(n.connBeacon, 0)
		}
		return nil
	}, n.connBeacon)
	if err != nil {
		return fmt.Errorf("register beacon_connected gauge callback: %w", err)
	}

	n.connAge, err = cfg.Meter.Float64Histogram("conn_age", metric.WithDescription("Connection age after disconnect in seconds"), metric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("new conn_age histogram: %w", err)
	}

	n.connMedianAge, err = cfg.Meter.Float64ObservableGauge("conn_median_age", metric.WithDescription("The median age of all currently active connections"), metric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("new conn_median_age gauge: %w", err)
	}
	_, err = cfg.Meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		// get a reference to all connections
		conns := n.host.Network().Conns()
		if len(conns) == 0 {
			// don't measure anything if we have no active connection
			return nil
		}

		// calculate connection ages in seconds
		ages := make([]float64, len(conns))
		for i, conn := range conns {
			ages[i] = time.Since(conn.Stat().Opened).Seconds()
		}

		// calculate median
		sort.Float64s(ages)

		if len(ages)%2 == 0 {
			lo, hi := ages[(len(ages)-1)/2], ages[len(ages)/2]
			obs.ObserveFloat64(n.connMedianAge, (lo+hi)/2)
		} else {
			obs.ObserveFloat64(n.connMedianAge, ages[len(ages)/2])
		}

		return nil
	}, n.connMedianAge)
	if err != nil {
		return fmt.Errorf("register conn_median_age gauge callback: %w", err)
	}

	return nil
}

// OnEvent registers a callback that is executed when an event is received.
func (n *Node) OnEvent(cb func(ctx context.Context, event *host.TraceEvent)) {
	n.eventCallbacks = append(n.eventCallbacks, cb)
}

// Start starts the listening process.
func (n *Node) Start(ctx context.Context) error {
	defer logDeferErr(n.host.Close, "Failed closing libp2p host")

	dsCleanupFn, err := n.startDataStream(ctx)
	if err != nil {
		return fmt.Errorf("failed starting data stream producer: %w", err)
	}
	defer dsCleanupFn()

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

	// get chain parameters for scores
	actVals, err := n.pryClient.getActiveValidatorCount(ctx)
	if err != nil {
		return fmt.Errorf("fetch active validators: %w", err)
	}

	// initialize GossipSub
	n.pubSub.gs, err = n.host.InitGossipSub(ctx, n.cfg.pubsubOptions(n, actVals)...)
	if err != nil {
		return fmt.Errorf("init gossip sub: %w", err)
	}

	// Create a connection signal that fires when the Prysm node has connected
	// to us. Prysm will try this periodically AFTER we have registered ourselves
	// as a trusted peer. Therefore, we register the signal first and only
	// afterward add ourselves as a trusted peer to not miss the signal
	// https://github.com/prysmaticlabs/prysm/issues/13659
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	connSignal := n.host.ConnSignal(timeoutCtx, addrInfo.ID)

	// register ourselves as a trusted peer by submitting our private ip address
	var trustedMaddr ma.Multiaddr
	if n.cfg.LocalTrustedAddr {
		trustedMaddr, err = n.host.LocalListenMaddr()
		if err != nil {
			return err
		}
		slog.Info("Adding ourselves as a trusted peer to Prysm", tele.LogAttrPeerID(n.host.ID()), "on local maddr", trustedMaddr)
	} else {
		trustedMaddr, err = n.host.PrivateListenMaddr()
		if err != nil {
			return err
		}
		slog.Info("Adding ourselves as a trusted peer to Prysm", tele.LogAttrPeerID(n.host.ID()), "on priv maddr", trustedMaddr)
	}

	if err := n.pryClient.AddTrustedPeer(ctx, n.host.ID(), trustedMaddr); err != nil {
		return fmt.Errorf("failed adding ourself as trusted peer: %w", err)
	}

	defer func() {
		// unregister ourselves as a trusted peer from prysm. Context timeout
		// is not necessary because the pryClient applies a 5s timeout to each API call
		slog.Info("Removing ourselves as a trusted peer from Prysm", tele.LogAttrPeerID(n.host.ID()))
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

	// protect connection to beacon node so that it's not pruned at some point
	n.host.ConnManager().Protect(addrInfo.ID, "hermes")

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

	// start public listen address watcher to keep our ENR up to date
	aw := &AddrWatcher{
		h: *n.host,
		n: n.disc.node,
	}
	n.sup.Add(aw)

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

// startDataStream starts the data stream and implements a graceful shutdown
func (n *Node) startDataStream(ctx context.Context) (func(), error) {
	backgroundCtx := context.Background()

	go func() {
		if err := n.ds.Start(backgroundCtx); err != nil {
			slog.Warn("Failed to start data stream", tele.LogAttrError(err))
		}
	}()

	cleanupFn := func() {
		n.ds.Stop(ctx)
	}

	return cleanupFn, nil
}
