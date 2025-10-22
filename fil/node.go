package fil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	gk "github.com/dennis-tra/go-kinesis"
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

	// The PubSub service that implements various gossipsub topics
	pubSub *PubSub

	// The discovery service, periodically querying the discv5 DHT network
	disc *Discovery

	// The suture supervisor that is the root of the service tree
	sup *suture.Supervisor

	// Metrics
	connCount     metric.Int64ObservableGauge
	connDurHist   metric.Float64Histogram
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
		DirectConnections:     cfg.DirectMultiaddrs(),
		PeerFilter:            cfg.PeerFilter,
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

	var disc *Discovery
	if cfg.DiscoveryActorEnabled {
		var err error
		disc, err = NewDiscovery(h.Host, &DiscoveryConfig{
			Interval: cfg.LookupInterval,
			Tracer:   cfg.Tracer,
			Meter:    cfg.Meter,
		})
		if err != nil {
			return nil, fmt.Errorf("new discovery service: %w", err)
		}
		slog.Info("Initialized new discovery service")
	} else {
		slog.Info("Discovery actor is disabled")
	}

	// initialize the pubsub topic handlers
	pubSubConfig := &PubSubConfig{
		TopicConfigs: cfg.TopicConfigs,
		DataStream:   ds,
	}

	pubSub, err := NewPubSub(h, pubSubConfig)
	if err != nil {
		return nil, fmt.Errorf("new PubSub service: %w", err)
	}

	// finally, initialize hermes node
	n := &Node{
		cfg:    cfg,
		host:   h,
		disc:   disc,
		ds:     ds,
		pubSub: pubSub,
		sup:    suture.NewSimple("fil"),
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

	// initialize GossipSub
	n.pubSub.gs, err = n.host.InitGossipSub(ctx, n.cfg.pubsubOptions(n)...)
	if err != nil {
		return fmt.Errorf("init gossip sub: %w", err)
	}

	// register the node itself as the notifiee for network connection events
	n.host.Network().Notify(n)

	// start the pubsub subscriptions and handlers
	n.sup.Add(n.pubSub)

	// start the hermes host to trace gossipsub messages
	n.sup.Add(n.host)

	// connect to bootstrappers
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 5*time.Second)
	defer timeoutCancel()

	var wg sync.WaitGroup
	var errCounter atomic.Int32
	for _, p := range n.cfg.Bootstrappers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := n.host.Connect(timeoutCtx, p); err != nil {
				slog.Warn("Failed to connect to bootstrapper", tele.LogAttrError(err), "peer", p)
				errCounter.Add(1)
			}
		}()
	}
	wg.Wait()

	if errCounter.Load() == int32(len(n.cfg.Bootstrappers)) {
		return fmt.Errorf("failed to connect to all bootstrappers")
	}

	if n.disc != nil {
		slog.Info("Starting discovery actor")
		n.sup.Add(n.disc)
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
