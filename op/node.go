package op

import (
	"context"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/event"
	"log/slog"
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

	// The suture supervisor that is the root of the service tree
	sup *suture.Supervisor

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

	sub, err := h.EventBus().Subscribe([]any{new(event.EvtPeerIdentificationCompleted), new(event.EvtPeerIdentificationFailed)})
	if err != nil {
		return nil, fmt.Errorf("new peer identification completed event subscription: %w", err)
	}

	go func() {
		for evt := range sub.Out() {
			switch tevt := evt.(type) {
			case event.EvtPeerIdentificationFailed:
				fmt.Println("ID COMPLETED:", tevt)
			case event.EvtPeerIdentificationCompleted:
				fmt.Println(tevt)
			default:
				panic("asasdfsdf")
			}

		}
	}()

	// initialize the pubsub topic handlers
	pubSubConfig := &PubSubConfig{
		ChainID:    cfg.ChainID,
		DataStream: ds,
	}

	pubSub, err := NewPubSub(h, pubSubConfig)
	if err != nil {
		return nil, fmt.Errorf("new PubSub service: %w", err)
	}

	// finally, initialize hermes node
	n := &Node{
		cfg:    cfg,
		host:   h,
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

	return n, nil
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
