package eth

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/types"
	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	hermeshost "github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type ReqRespConfig struct {
	Encoder      encoder.NetworkEncoding
	DataStream   hermeshost.DataStream
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Chain related info
	Chain *Chain

	// Telemetry accessors
	Tracer trace.Tracer
	Meter  metric.Meter
}

// ReqResp implements the request response domain of the eth2 RPC spec:
// https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md
type ReqResp struct {
	host     host.Host
	cfg      *ReqRespConfig
	delegate peer.ID // peer ID that we delegate requests to

	statusLim *rate.Limiter

	// metrics
	meterRequestCounter metric.Int64Counter
	latencyHistogram    metric.Float64Histogram
	goodbyeCounter      metric.Int64Counter
}

type ContextStreamHandler func(context.Context, network.Stream) (map[string]any, error)

func NewReqResp(h host.Host, cfg *ReqRespConfig) (*ReqResp, error) {
	if cfg == nil {
		return nil, fmt.Errorf("req resp server config must not be nil")
	}

	p := &ReqResp{
		host:      h,
		cfg:       cfg,
		statusLim: rate.NewLimiter(1, 5),
	}

	var err error
	p.meterRequestCounter, err = cfg.Meter.Int64Counter("rpc_requests")
	if err != nil {
		return nil, fmt.Errorf("new rpc_requests counter: %w", err)
	}

	p.latencyHistogram, err = cfg.Meter.Float64Histogram(
		"rpc_latency_ms",
		metric.WithExplicitBucketBoundaries(10, 50, 100, 500, 1000, 5000, 10000),
	)
	if err != nil {
		return nil, fmt.Errorf("new request_latency histogram: %w", err)
	}

	p.goodbyeCounter, err = cfg.Meter.Int64Counter(
		"goodbye_messages",
		metric.WithDescription("Counter for goodbye messages received"),
	)
	if err != nil {
		return nil, fmt.Errorf("new goodbye_messages counter: %w", err)
	}

	return p, nil
}

// RegisterHandlers registers all RPC handlers. It checks first if all
// preconditions are met. This includes valid initial status and metadata
// values.
func (r *ReqResp) RegisterHandlers(ctx context.Context) error {
	init, cause := r.cfg.Chain.IsInit()
	if !init {
		return fmt.Errorf("reqresp: chain module is not init (%s)", cause)
	}

	handlers := map[string]ContextStreamHandler{
		p2p.RPCPingTopicV1:     r.pingHandler,
		p2p.RPCGoodByeTopicV1:  r.goodbyeHandler,
		p2p.RPCStatusTopicV1:   r.statusHandler,
		p2p.RPCStatusTopicV2:   r.statusV2Handler,
		p2p.RPCMetaDataTopicV1: r.metadataV1Handler,
		p2p.RPCMetaDataTopicV2: r.metadataV2Handler,
		p2p.RPCMetaDataTopicV3: r.metadataV3Handler,
		// TODO: get this back online
		// p2p.RPCBlocksByRangeTopicV2:       r.blocksByRangeV2Handler,
		// p2p.RPCBlocksByRootTopicV2:        r.blocksByRootV2Handler,
		// p2p.RPCBlobSidecarsByRangeTopicV1: r.blobsByRangeV2Handler,
		// p2p.RPCBlobSidecarsByRootTopicV1: r.blobsByRootV2Handler,
		p2p.RPCDataColumnSidecarsByRangeTopicV1: r.DataColumnsByRangeV1Handler,
		p2p.RPCDataColumnSidecarsByRootTopicV1:  r.DataColumnsByRootV1Handler,
	}

	for id, handler := range handlers {
		protocolID := r.protocolID(id)
		slog.Info("Register protocol handler", "protocol", protocolID)
		r.host.SetStreamHandler(protocolID, r.wrapStreamHandler(ctx, string(protocolID), handler))
	}

	return nil
}

func (r *ReqResp) protocolID(topic string) protocol.ID {
	return protocol.ID(topic + r.cfg.Encoder.ProtocolSuffix())
}

func (r *ReqResp) wrapStreamHandler(ctx context.Context, name string, handler ContextStreamHandler) network.StreamHandler {
	attrs := []attribute.KeyValue{attribute.String("handler", name)}
	mattrs := metric.WithAttributes(attrs...)
	tattrs := trace.WithAttributes(attrs...)

	return func(s network.Stream) {
		// handle the request
		start := time.Now()
		traceData, err := handler(ctx, s)
		if err != nil {
			slog.Warn("failed handling rpc", "protocol", s.Protocol(), tele.LogAttrError(err), tele.LogAttrPeerID(s.Conn().RemotePeer()))
		}
		end := time.Now()

		rawVal, err := r.host.Peerstore().Get(s.Conn().RemotePeer(), "AgentVersion")
		agentVersion := "n.a."
		if err == nil {
			if av, ok := rawVal.(string); ok {
				agentVersion = normalizeAgentVersion(av)
			}
		}

		slog.Debug("Stream Opened", tele.LogAttrPeerID(s.Conn().RemotePeer()), "protocol", s.Protocol(), "agent", agentVersion)

		// Start request tracing
		ctx, span := r.cfg.Tracer.Start(ctx, "rpc", tattrs)
		span.SetAttributes(attribute.String("peer_id", s.Conn().RemotePeer().String()))
		span.SetAttributes(attribute.String("agent", agentVersion))
		defer span.End()

		traceType := "HANDLE_STREAM"
		protocol := string(s.Protocol())

		// Usual protocol string: /eth2/beacon_chain/req/metadata/2/ssz_snappy
		parts := strings.Split(protocol, "/")
		if len(parts) > 4 {
			traceType = hermeshost.EventTypeFromBeaconChainProtocol(protocol)
		}

		commonData := map[string]any{
			"PeerID":     s.Conn().RemotePeer(),
			"ProtocolID": s.Protocol(),
			"LatencyS":   end.Sub(start).Seconds(),
		}

		if err != nil {
			commonData["Error"] = err.Error()
		} else {
			commonData["Error"] = nil
		}

		maps.Copy(commonData, traceData)

		traceEvt := &hermeshost.TraceEvent{
			Type:      traceType,
			PeerID:    r.host.ID(),
			Timestamp: time.Now(),
			Payload:   commonData,
		}

		if err := r.cfg.DataStream.PutRecord(ctx, traceEvt); err != nil {
			slog.Warn("failed to put record", tele.LogAttrError(err))
		}

		// update meters
		r.meterRequestCounter.Add(ctx, 1, mattrs)
		r.latencyHistogram.Record(ctx, float64(end.Sub(start).Milliseconds()), mattrs)
	}
}

func (r *ReqResp) pingHandler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	req := new(primitives.SSZUint64)
	if err := r.readRequest(stream, req); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("read sequence number: %w", err)
	}

	sq := r.cfg.Chain.CurrentSeqNumber()
	if err := r.writeResponse(stream, &sq); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write sequence number: %w", err)
	}

	traceData := map[string]any{
		"ReceivedSeq": req,
		"SentSeq":     sq,
	}

	_ = stream.Close()

	return traceData, nil
}

func (r *ReqResp) goodbyeHandler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	req := primitives.SSZUint64(0)
	if err := r.readRequest(stream, &req); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("read sequence number: %w", err)
	}

	msg, found := types.GoodbyeCodeMessages[req]
	if found {
		if _, err := r.host.Peerstore().Get(stream.Conn().RemotePeer(), peerstoreKeyIsHandshaked); err == nil {
			var (
				agentVersion = "unknown"
				reason       = "unknown"
			)

			// Get agent version (client) for the peer.
			rawVal, err := r.host.Peerstore().Get(stream.Conn().RemotePeer(), "AgentVersion")
			if err == nil {
				if av, ok := rawVal.(string); ok {
					agentVersion = normalizeAgentVersion(av)
				}
			}

			// This will be one of GoodbyeCodeMessages.
			if found {
				reason = msg
			}

			r.goodbyeCounter.Add(ctx, 1, metric.WithAttributes(
				[]attribute.KeyValue{
					attribute.Int64("code", int64(req)),
					attribute.String("reason", reason),
					attribute.String("agent", agentVersion),
				}...,
			))

			slog.Info("Received goodbye message", tele.LogAttrPeerID(stream.Conn().RemotePeer()), "msg", msg)
		} else {
			slog.Debug("Received goodbye message", tele.LogAttrPeerID(stream.Conn().RemotePeer()), "msg", msg)
		}
	}

	traceData := map[string]any{
		"Code":   req,
		"Reason": msg,
	}

	_ = stream.Close()

	return traceData, nil
}

func (r *ReqResp) statusHandler(ctx context.Context, upstream network.Stream) (map[string]any, error) {
	statusTraceData := func(status *pb.Status) map[string]any {
		return map[string]any{
			"ForkDigest":     hex.EncodeToString(status.ForkDigest),
			"HeadRoot":       hex.EncodeToString(status.HeadRoot),
			"HeadSlot":       status.HeadSlot,
			"FinalizedRoot":  hex.EncodeToString(status.FinalizedRoot),
			"FinalizedEpoch": status.FinalizedEpoch,
		}
	}

	// first, read the status from the remote peer
	req := &pb.Status{}
	if err := r.readRequest(upstream, req); err != nil {
		upstream.Reset()
		return nil, fmt.Errorf("read status data from remote peer: %w", err)
	}

	// let the upstream peer (who initiated the request) know the latest status
	localStI := r.cfg.Chain.GetStatus(statusV0)
	localSt, _ := localStI.(*pb.Status)
	if err := r.writeResponse(upstream, localSt); err != nil {
		upstream.Reset()
		return nil, fmt.Errorf("respond status to upstream: %w", err)
	}

	traceData := map[string]any{
		"Request":  statusTraceData(req),
		"Response": statusTraceData(localSt),
	}

	slog.Debug(
		"status v1 response",
		"peer", upstream.Conn().RemotePeer().String(),
		"trusted-peer", upstream.Conn().RemotePeer() == r.delegate,
		"head-slot", localSt.HeadSlot,
		"fork_digest", hex.EncodeToString(localSt.ForkDigest),
	)

	_ = upstream.Close()

	return traceData, nil
}

// statusV2Handler handles StatusV2 protocol requests
func (r *ReqResp) statusV2Handler(ctx context.Context, upstream network.Stream) (map[string]any, error) {
	statusTraceData := func(status *pb.StatusV2) map[string]any {
		return map[string]any{
			"ForkDigest":            hex.EncodeToString(status.ForkDigest),
			"HeadRoot":              hex.EncodeToString(status.HeadRoot),
			"HeadSlot":              status.HeadSlot,
			"FinalizedRoot":         hex.EncodeToString(status.FinalizedRoot),
			"FinalizedEpoch":        status.FinalizedEpoch,
			"EarliestAvailableSlot": status.EarliestAvailableSlot,
		}
	}

	// first, read the status from the remote peer
	req := &pb.StatusV2{}
	if err := r.readRequest(upstream, req); err != nil {
		upstream.Reset()
		return nil, fmt.Errorf("read status V2 data from peer: %w", err)
	}

	// create response status from memory status
	localStI := r.cfg.Chain.GetStatus(statusV1)
	localSt, _ := localStI.(*pb.StatusV2)

	// let the upstream peer (who initiated the request) know the latest status
	if err := r.writeResponse(upstream, localSt); err != nil {
		upstream.Reset()
		return nil, fmt.Errorf("respond status V2 to upstream: %w", err)
	}

	traceData := map[string]any{
		"Request":  statusTraceData(req),
		"Response": statusTraceData(localSt),
	}

	slog.Debug(
		"status v2 response",
		"peer", upstream.Conn().RemotePeer().String(),
		"trusted-peer", upstream.Conn().RemotePeer() == r.delegate,
		"head-slot", localSt.HeadSlot,
		"fork_digest", hex.EncodeToString(localSt.ForkDigest),
	)

	_ = upstream.Close()

	return traceData, nil
}

func (r *ReqResp) metadataV1Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	mdI := r.cfg.Chain.GetMetadata(metadataV0)
	metaData, _ := mdI.(*pb.MetaDataV0)

	if err := r.writeResponse(stream, metaData); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write meta data v1: %w", err)
	}

	traceData := map[string]any{
		"SeqNumber": metaData.SeqNumber,
		"Attnets":   hex.EncodeToString(metaData.Attnets.Bytes()),
	}

	slog.Info(
		"metadata response",
		"peer", stream.Conn().RemotePeer().String(),
		"attnets", metaData.Attnets,
	)

	_ = stream.Close()

	return traceData, nil
}

func (r *ReqResp) metadataV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	mdI := r.cfg.Chain.GetMetadata(metadataV1)
	metaData, _ := mdI.(*pb.MetaDataV1)

	if err := r.writeResponse(stream, metaData); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write meta data v1: %w", err)
	}

	if err := r.writeResponse(stream, metaData); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write meta data v2: %w", err)
	}

	traceData := map[string]any{
		"SeqNumber": metaData.SeqNumber,
		"Attnets":   hex.EncodeToString(metaData.Attnets.Bytes()),
		"Syncnets":  hex.EncodeToString(metaData.Syncnets.Bytes()),
	}
	slog.Info(
		"metadata response",
		"peer", stream.Conn().RemotePeer().String(),
		"attnets", metaData.Attnets,
		"synccommittees", metaData.Syncnets,
	)

	_ = stream.Close()

	return traceData, nil
}

// metadataV3Handler handles MetadataV3 protocol requests (returns MetaDataV2 type)
func (r *ReqResp) metadataV3Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	mdI := r.cfg.Chain.GetMetadata(metadataV2)
	metaData, _ := mdI.(*pb.MetaDataV2)
	if metaData == nil {
		return nil, fmt.Errorf("no metadata available")
	}

	if err := r.writeResponse(stream, metaData); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write meta data v3: %w", err)
	}

	traceData := map[string]any{
		"SeqNumber":         metaData.SeqNumber,
		"Attnets":           hex.EncodeToString(metaData.Attnets.Bytes()),
		"CustodyGroupCount": metaData.CustodyGroupCount,
	}

	if metaData.Syncnets != nil {
		traceData["Syncnets"] = hex.EncodeToString(metaData.Syncnets.Bytes())
	}

	slog.Info(
		"metadata V3 response",
		"attnets", metaData.Attnets,
		"synccommittees", metaData.Syncnets,
		"custody_group_count", metaData.CustodyGroupCount,
	)

	_ = stream.Close()
	return traceData, nil
}

func (r *ReqResp) DataColumnsByRangeV1Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	req := new(pb.DataColumnSidecarsByRangeRequest)
	err := r.readRequest(stream, req)
	if err != nil {
		stream.Reset()
		return nil, fmt.Errorf("error to read data-column-by-range request %w", err)
	}

	// TODO: handle this correctly
	// remote this and close correctly
	stream.Reset()

	traceData := map[string]any{
		"PeerID":    stream.Conn().RemotePeer().String(),
		"StartSlot": req.StartSlot,
		"Count":     req.Count,
		"Columns":   req.Columns,
	}

	slog.Info(
		"data-columns-by-range",
		"PeerID", stream.Conn().RemotePeer().String(),
		"Columns", req.Columns,
	)

	// _ = stream.Close()

	return traceData, nil
}

func (r *ReqResp) DataColumnsByRootV1Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	req := new(types.DataColumnsByRootIdentifiers)
	err := r.readRequest(stream, req)
	if err != nil {
		return nil, fmt.Errorf("error to read data-column-by-root request %w", err)
	}

	// TODO: handle this correctly
	// remote this and close correctly
	stream.Reset()

	traceData := map[string]any{
		"PeerID": stream.Conn().RemotePeer().String(),
		"Roots":  strconv.Itoa(len(*req)),
	}

	slog.Info(
		"data-columns-by-root",
		"PeerID", stream.Conn().RemotePeer().String(),
		"Roots", len(*req),
	)

	_ = stream.Close()

	return traceData, nil
}

//lint:ignore U1000 ignore the unused litner error - testing
func (r *ReqResp) blocksByRangeV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blocks by range request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

//lint:ignore U1000 ignore the unused litner error - testing
func (r *ReqResp) blocksByRootV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blocks by root request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

//lint:ignore U1000 ignore the unused litner error - testing
func (r *ReqResp) blobsByRangeV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blobs by range request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

//lint:ignore U1000 ignore the unused litner error - testing
func (r *ReqResp) blobsByRootV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blobs by root request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

//lint:ignore U1000 ignore the unused litner error - testing
func (r *ReqResp) delegateStream(ctx context.Context, upstream network.Stream) error {
	dialCtx := network.WithForceDirectDial(ctx, "prevent backoff")
	downstream, err := r.host.NewStream(dialCtx, r.delegate, upstream.Protocol())
	if err != nil {
		return fmt.Errorf("new stream to downstream host: %w", err)
	}
	defer logDeferErr(downstream.Reset, "failed resetting downstream stream")

	// send blocksByRange request to downstream peer. This will stop as soon as the
	// upstream has closed its writer side.
	if _, err = io.Copy(downstream, upstream); err != nil {
		return fmt.Errorf("copy data from upstream to downstream: %w", err)
	}

	// The upstream is done, so also tell downstream that we're done
	if err = downstream.CloseWrite(); err != nil {
		return fmt.Errorf("failed to close writing side of stream: %w", err)
	}

	// We won't read anything from upstream from this point on
	if err = upstream.CloseRead(); err != nil {
		return fmt.Errorf("failed to close reading side of stream: %w", err)
	}

	// set timeout for reading from our delegated node
	if err = downstream.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return fmt.Errorf("failed setting read deadline on stream: %w", err)
	}

	// set timeout for writing to the upstream remote peer
	if err = upstream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("failed setting read deadline on stream: %w", err)
	}

	// read response from downstream peer but simultaneously pass it through
	// to the upstream peer
	if _, err = io.Copy(downstream, upstream); err != nil {
		return fmt.Errorf("copy data from downstream to upstream: %w", err)
	}

	// properly close both sides of the stream
	downCloseErr := downstream.Close()
	upCloseErr := upstream.Close()
	if upCloseErr != nil {
		return upCloseErr
	} else if downCloseErr != nil {
		return downCloseErr
	}

	return nil
}

// Status requests V1 status from a peer
func (r *ReqResp) Status(ctx context.Context, pid peer.ID) (status *pb.Status, err error) {
	defer func() {
		av, err := r.host.Peerstore().Get(pid, "AgentVersion")
		if err != nil {
			av = "unknown"
		}

		reqData := map[string]any{
			"AgentVersion": av,
			"PeerID":       pid.String(),
		}
		if status != nil {
			reqData["ForkDigest"] = hex.EncodeToString(status.ForkDigest)
			reqData["HeadRoot"] = hex.EncodeToString(status.HeadRoot)
			reqData["HeadSlot"] = status.HeadSlot
			reqData["FinalizedRoot"] = hex.EncodeToString(status.FinalizedRoot)
			reqData["FinalizedEpoch"] = status.FinalizedEpoch
		}

		if err != nil {
			reqData["Error"] = err.Error()
		}

		traceEvt := &hermeshost.TraceEvent{
			Type:      "REQUEST_STATUS",
			PeerID:    r.host.ID(),
			Timestamp: time.Now(),
			Payload:   reqData,
		}

		traceCtx := context.Background()
		if err := r.cfg.DataStream.PutRecord(traceCtx, traceEvt); err != nil {
			slog.Warn("failed to put record", tele.LogAttrError(err))
		}

		attrs := []attribute.KeyValue{
			attribute.String("rpc", "status"),
			attribute.Bool("success", err == nil),
		}
		r.meterRequestCounter.Add(traceCtx, 1, metric.WithAttributes(attrs...))
	}()

	slog.Info("Perform status V1 request", tele.LogAttrPeerID(pid))
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCStatusTopicV1))
	if err != nil {
		return nil, fmt.Errorf("new stream to peer %s: %w", pid, err)
	}

	// actually write the data to the stream
	stI := r.cfg.Chain.GetStatus(statusV0)
	stReq, _ := stI.(*pb.Status)

	if err := r.writeRequest(stream, stReq); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write status request: %w", err)
	}

	// read and decode status response
	resp := &pb.Status{}
	if err := r.readResponse(stream, resp); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("read status response: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return resp, nil
}

// StatusV2 requests V2 status from a peer
func (r *ReqResp) StatusV2(ctx context.Context, pid peer.ID) (status *pb.StatusV2, err error) {
	defer func() {
		av, err := r.host.Peerstore().Get(pid, "AgentVersion")
		if err != nil {
			av = "unknown"
		}

		reqData := map[string]any{
			"AgentVersion": av,
			"PeerID":       pid.String(),
		}
		if status != nil {
			reqData["ForkDigest"] = hex.EncodeToString(status.ForkDigest)
			reqData["HeadRoot"] = hex.EncodeToString(status.HeadRoot)
			reqData["HeadSlot"] = status.HeadSlot
			reqData["FinalizedRoot"] = hex.EncodeToString(status.FinalizedRoot)
			reqData["FinalizedEpoch"] = status.FinalizedEpoch
			reqData["EarliestAvailableSlot"] = status.EarliestAvailableSlot
		}

		if err != nil {
			reqData["Error"] = err.Error()
		}

		traceEvt := &hermeshost.TraceEvent{
			Type:      "REQUEST_STATUS",
			PeerID:    r.host.ID(),
			Timestamp: time.Now(),
			Payload:   reqData,
		}

		traceCtx := context.Background()
		if err := r.cfg.DataStream.PutRecord(traceCtx, traceEvt); err != nil {
			slog.Warn("failed to put record", tele.LogAttrError(err))
		}

		attrs := []attribute.KeyValue{
			attribute.String("rpc", "status_v2"),
			attribute.Bool("success", err == nil),
		}
		r.meterRequestCounter.Add(traceCtx, 1, metric.WithAttributes(attrs...))
	}()

	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCStatusTopicV2))
	if err != nil {
		return nil, fmt.Errorf("new stream to peer %s: %w", pid, err)
	}

	// actually write the data to the stream
	localStI := r.cfg.Chain.GetStatus(statusV1)
	localSt, _ := localStI.(*pb.StatusV2)

	slog.Info(
		"sending local status v2 request",
		"peer", stream.Conn().RemotePeer().String(),
		"trusted-peer", stream.Conn().RemotePeer() == r.delegate,
		"head-slot", localSt.HeadSlot,
		"head-root", hex.EncodeToString(localSt.HeadRoot),
		"finalized-root", hex.EncodeToString(localSt.FinalizedRoot),
		"fork-digest", hex.EncodeToString(localSt.ForkDigest),
		"eas", localSt.EarliestAvailableSlot,
	)
	if err := r.writeRequest(stream, localSt); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write status V2 request: %w", err)
	}

	// read and decode status response
	resp := &pb.StatusV2{}
	if err := r.readResponse(stream, resp); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("read status V2 response: %w", err)
	}

	_ = stream.Close()

	slog.Info(
		"successful status v2 response",
		"peer", stream.Conn().RemotePeer().String(),
		"trusted-peer", stream.Conn().RemotePeer() == r.delegate,
		"head-slot", resp.HeadSlot,
		"head-root", hex.EncodeToString(resp.HeadRoot),
		"finalized-root", hex.EncodeToString(resp.FinalizedRoot),
		"fork-digest", hex.EncodeToString(resp.ForkDigest),
		"eas", resp.EarliestAvailableSlot,
	)

	return resp, nil
}

func (r *ReqResp) Ping(ctx context.Context, pid peer.ID) (err error) {
	defer func() {
		traceEvt := &hermeshost.TraceEvent{
			Type:      "REQUEST_PING",
			PeerID:    r.host.ID(),
			Timestamp: time.Now(),
			Payload: map[string]string{
				"PeerID": pid.String(),
			},
		}
		traceCtx := context.Background()
		if err := r.cfg.DataStream.PutRecord(traceCtx, traceEvt); err != nil {
			slog.Warn("failed to put record", tele.LogAttrError(err))
		}

		attrs := []attribute.KeyValue{
			attribute.String("rpc", "ping"),
			attribute.Bool("success", err == nil),
		}
		r.meterRequestCounter.Add(traceCtx, 1, metric.WithAttributes(attrs...))
	}()

	slog.Debug("Perform ping request", tele.LogAttrPeerID(pid))
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCPingTopicV1))
	if err != nil {
		return fmt.Errorf("new %s stream to peer %s: %w", p2p.RPCPingTopicV1, pid, err)
	}

	seqNum := r.cfg.Chain.CurrentSeqNumber()

	req := primitives.SSZUint64(seqNum)
	if err := r.writeRequest(stream, &req); err != nil {
		stream.Reset()
		return fmt.Errorf("write ping request: %w", err)
	}

	// read and decode status response
	resp := new(primitives.SSZUint64)
	if err := r.readResponse(stream, resp); err != nil {
		stream.Reset()
		return fmt.Errorf("read ping response: %w", err)
	}

	_ = stream.Close()

	return nil
}

// MetaData requests V1 metadata from a peer
func (r *ReqResp) MetaDataV1(ctx context.Context, pid peer.ID) (resp *pb.MetaDataV1, err error) {
	defer func() {
		reqData := map[string]any{
			"PeerID": pid.String(),
		}

		if resp != nil {
			reqData["SeqNumber"] = resp.SeqNumber
			reqData["Attnets"] = resp.Attnets
			reqData["Syncnets"] = resp.Syncnets
		}

		if err != nil {
			reqData["Error"] = err.Error()
		}

		traceEvt := &hermeshost.TraceEvent{
			Type:      "REQUEST_METADATA",
			PeerID:    r.host.ID(),
			Timestamp: time.Now(),
			Payload:   reqData,
		}
		traceCtx := context.Background()
		if err := r.cfg.DataStream.PutRecord(traceCtx, traceEvt); err != nil {
			slog.Warn("failed to put record", tele.LogAttrError(err))
		}

		attrs := []attribute.KeyValue{
			attribute.String("rpc", "meta_data"),
			attribute.Bool("success", err == nil),
		}
		r.meterRequestCounter.Add(traceCtx, 1, metric.WithAttributes(attrs...))
	}()

	slog.Debug("Perform metadata V1 request", tele.LogAttrPeerID(pid))
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCMetaDataTopicV2))
	if err != nil {
		return resp, fmt.Errorf("new %s stream to peer %s: %w", p2p.RPCMetaDataTopicV2, pid, err)
	}

	if err := r.writeRequest(stream, nil); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write metadata v2 request %s", err)
	}

	// read and decode metadata response
	resp = &pb.MetaDataV1{}
	if err := r.readResponse(stream, resp); err != nil {
		stream.Reset()
		return resp, fmt.Errorf("read metadata response: %w", err)
	}

	_ = stream.Close()

	return resp, nil
}

// MetaDataV2 requests V2 metadata from a peer (includes custody group count)
func (r *ReqResp) MetaDataV2(ctx context.Context, pid peer.ID) (resp *pb.MetaDataV2, err error) {
	defer func() {
		reqData := map[string]any{
			"PeerID": pid.String(),
		}

		if resp != nil {
			reqData["SeqNumber"] = resp.SeqNumber
			reqData["Attnets"] = resp.Attnets
			reqData["Syncnets"] = resp.Syncnets
			reqData["CustodyGroupCount"] = resp.CustodyGroupCount
		}

		if err != nil {
			reqData["Error"] = err.Error()
		}

		traceEvt := &hermeshost.TraceEvent{
			Type:      "REQUEST_METADATA_V2",
			PeerID:    r.host.ID(),
			Timestamp: time.Now(),
			Payload:   reqData,
		}
		traceCtx := context.Background()
		if err := r.cfg.DataStream.PutRecord(traceCtx, traceEvt); err != nil {
			slog.Warn("failed to put record", tele.LogAttrError(err))
		}

		attrs := []attribute.KeyValue{
			attribute.String("rpc", "meta_data_v2"),
			attribute.Bool("success", err == nil),
		}
		r.meterRequestCounter.Add(traceCtx, 1, metric.WithAttributes(attrs...))
	}()

	slog.Debug("Perform metadata V2 request", tele.LogAttrPeerID(pid))
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCMetaDataTopicV3))
	if err != nil {
		return resp, fmt.Errorf("new %s stream to peer %s: %w", p2p.RPCMetaDataTopicV3, pid, err)
	}

	if err := r.writeRequest(stream, nil); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("write metadata v2 request %s", err)
	}

	// read and decode metadata response
	resp = &pb.MetaDataV2{}
	if err := r.readResponse(stream, resp); err != nil {
		stream.Reset()
		return resp, fmt.Errorf("read metadata V2 response: %w", err)
	}

	_ = stream.Close()

	return resp, nil
}

func (r *ReqResp) BlocksByRangeV2(ctx context.Context, pid peer.ID, firstSlot, lastSlot uint64) ([]interfaces.ReadOnlySignedBeaconBlock, error) {
	// TODO: reimplement this logic usin the das-guardian logic?
	return make([]interfaces.ReadOnlySignedBeaconBlock, 0, (lastSlot - firstSlot)), nil
}

// readForkDigestFromStream reads any attached context-bytes to the payload.
func (r *ReqResp) readForkDigestFromStream(stream network.Stream) (forkD [4]byte, err error) {
	// Read context (fork-digest) from stream (assumes it has it)
	b := make([]byte, 4)
	if _, err = stream.Read(b); err != nil {
		return ForkVersion{}, err
	}
	copy(forkD[:], b)
	return forkD, nil
}

// getBlockForForkVersion returns an ReadOnlySignedBeaconBlock interface from the block type of each ForkVersion
func (r *ReqResp) getBlockForForkVersion(forkV ForkVersion, encoding encoder.NetworkEncoding, stream network.Stream) (sblk interfaces.ReadOnlySignedBeaconBlock, err error) {
	switch forkV {
	case Phase0ForkVersion:
		blk := &pb.SignedBeaconBlock{}
		err = encoding.DecodeWithMaxLength(stream, blk)
		if err != nil {
			return sblk, err
		}
		return blocks.NewSignedBeaconBlock(blk)

	case AltairForkVersion:
		blk := &pb.SignedBeaconBlockAltair{}
		err = encoding.DecodeWithMaxLength(stream, blk)
		if err != nil {
			return sblk, err
		}
		return blocks.NewSignedBeaconBlock(blk)

	case BellatrixForkVersion:
		blk := &pb.SignedBeaconBlockBellatrix{}
		err = encoding.DecodeWithMaxLength(stream, blk)
		if err != nil {
			return sblk, err
		}
		return blocks.NewSignedBeaconBlock(blk)

	case CapellaForkVersion:
		blk := &pb.SignedBeaconBlockCapella{}
		err = encoding.DecodeWithMaxLength(stream, blk)
		if err != nil {
			return sblk, err
		}
		return blocks.NewSignedBeaconBlock(blk)

	case DenebForkVersion:
		blk := &pb.SignedBeaconBlockDeneb{}
		err = encoding.DecodeWithMaxLength(stream, blk)
		if err != nil {
			return sblk, err
		}
		return blocks.NewSignedBeaconBlock(blk)

	case ElectraForkVersion:
		blk := &pb.SignedBeaconBlockElectra{}
		err = encoding.DecodeWithMaxLength(stream, blk)
		if err != nil {
			return sblk, err
		}
		return blocks.NewSignedBeaconBlock(blk)
	default:
		sblk, _ := blocks.NewSignedBeaconBlock(&pb.SignedBeaconBlock{})
		return sblk, fmt.Errorf("unrecognized fork_version (received:%s) (ours: %d) (global: %s)", forkV, r.cfg.Chain.CurrentFork(), DenebForkVersion)
	}
}

// normalizeAgentVersion extracts the client name from the agent version string
// to reduce metric cardinality.
func normalizeAgentVersion(agentVersion string) string {
	// List of known consensus layer clients
	knownClients := []string{
		"prysm", "lighthouse", "nimbus", "lodestar", "grandine", "teku", "erigon", "caplin",
	}

	// Convert to lowercase for case-insensitive matching.
	lowerAgent := strings.ToLower(agentVersion)

	// Try to match against known clients.
	for _, client := range knownClients {
		if strings.Contains(lowerAgent, client) {
			return client
		}
	}

	// Extract first part before slash if present.
	parts := strings.Split(lowerAgent, "/")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}

	return "unknown"
}
