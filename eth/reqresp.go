package eth

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/types"
	psync "github.com/OffchainLabs/prysm/v6/beacon-chain/sync"
	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	ssz "github.com/ferranbt/fastssz"
	"github.com/libp2p/go-libp2p/core"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	hermeshost "github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type ReqRespConfig struct {
	ForkDigest [4]byte
	Encoder    encoder.NetworkEncoding
	DataStream hermeshost.DataStream

	AttestationSubnetConfig *SubnetConfig
	SyncSubnetConfig        *SubnetConfig

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

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

	metaDataMu sync.RWMutex
	metaData   *pb.MetaDataV1

	statusMu  sync.RWMutex
	status    *pb.Status
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

	md := &pb.MetaDataV1{
		SeqNumber: 0,
		Attnets:   BitArrayFromAttestationSubnets(cfg.AttestationSubnetConfig.Subnets),
		Syncnets:  BitArrayFromSyncSubnets(cfg.SyncSubnetConfig.Subnets),
	}

	slog.Info("Composed local MetaData",
		"attnets", md.Attnets,
		"syncnets", md.Syncnets,
	)

	p := &ReqResp{
		host:      h,
		cfg:       cfg,
		metaData:  md,
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

func (r *ReqResp) SetMetaData(seq uint64) {
	r.metaDataMu.Lock()
	defer r.metaDataMu.Unlock()

	if r.metaData.SeqNumber > seq {
		slog.Warn("Updated metadata with lower sequence number", "old", r.metaData.SeqNumber, "new", seq)
	}

	r.metaData = &pb.MetaDataV1{
		SeqNumber: seq,
		Attnets:   r.metaData.Attnets,
		Syncnets:  r.metaData.Syncnets,
	}
}

func (r *ReqResp) SetStatus(status *pb.Status) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()

	// if the ForkDigest is not the same, we should drop updating the local status
	// TODO: this might be re-checked for hardforks (make the client resilient to them)
	if r.status != nil && !bytes.Equal(r.status.ForkDigest, status.ForkDigest) {
		return
	}

	// check if anything has changed. Prevents the below log message to pollute
	// the log output.
	if r.status != nil && bytes.Equal(r.status.ForkDigest, status.ForkDigest) &&
		bytes.Equal(r.status.FinalizedRoot, status.FinalizedRoot) &&
		r.status.FinalizedEpoch == status.FinalizedEpoch &&
		bytes.Equal(r.status.HeadRoot, status.HeadRoot) &&
		r.status.HeadSlot == status.HeadSlot {
		// nothing has changed -> return
		return
	}

	slog.Info("New status:")
	slog.Info("  fork_digest: " + hex.EncodeToString(status.ForkDigest))
	slog.Info("  finalized_root: " + hex.EncodeToString(status.FinalizedRoot))
	slog.Info("  finalized_epoch: " + strconv.FormatUint(uint64(status.FinalizedEpoch), 10))
	slog.Info("  head_root: " + hex.EncodeToString(status.HeadRoot))
	slog.Info("  head_slot: " + strconv.FormatUint(uint64(status.HeadSlot), 10))

	r.status = status
}

func (r *ReqResp) cpyStatus() *pb.Status {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	if r.status == nil {
		return nil
	}

	return &pb.Status{
		ForkDigest:     bytes.Clone(r.status.ForkDigest),
		FinalizedRoot:  bytes.Clone(r.status.FinalizedRoot),
		FinalizedEpoch: r.status.FinalizedEpoch,
		HeadRoot:       bytes.Clone(r.status.HeadRoot),
		HeadSlot:       r.status.HeadSlot,
	}
}

// RegisterHandlers registers all RPC handlers. It checks first if all
// preconditions are met. This includes valid initial status and metadata
// values.
func (r *ReqResp) RegisterHandlers(ctx context.Context) error {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	if r.status == nil {
		return fmt.Errorf("chain status is nil")
	}

	r.metaDataMu.RLock()
	defer r.metaDataMu.RUnlock()
	if r.metaData == nil {
		return fmt.Errorf("chain metadata is nil")
	}

	handlers := map[string]ContextStreamHandler{
		p2p.RPCPingTopicV1:                r.pingHandler,
		p2p.RPCGoodByeTopicV1:             r.goodbyeHandler,
		p2p.RPCStatusTopicV1:              r.statusHandler,
		p2p.RPCMetaDataTopicV1:            r.metadataV1Handler,
		p2p.RPCMetaDataTopicV2:            r.metadataV2Handler,
		p2p.RPCBlocksByRangeTopicV2:       r.blocksByRangeV2Handler,
		p2p.RPCBlocksByRootTopicV2:        r.blocksByRootV2Handler,
		p2p.RPCBlobSidecarsByRangeTopicV1: r.blobsByRangeV2Handler,
		p2p.RPCBlobSidecarsByRootTopicV1:  r.blobsByRootV2Handler,
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
		rawVal, err := r.host.Peerstore().Get(s.Conn().RemotePeer(), "AgentVersion")

		agentVersion := "n.a."
		if err == nil {
			if av, ok := rawVal.(string); ok {
				agentVersion = normalizeAgentVersion(av)
			}
		}

		slog.Debug("Stream Opened", tele.LogAttrPeerID(s.Conn().RemotePeer()), "protocol", s.Protocol(), "agent", agentVersion)

		// Reset is a no-op if the stream is already closed. Closing the stream
		// is the responsibility of the handler.
		defer logDeferErr(s.Reset, "failed to reset stream")

		// Start request tracing
		ctx, span := r.cfg.Tracer.Start(ctx, "rpc", tattrs)
		span.SetAttributes(attribute.String("peer_id", s.Conn().RemotePeer().String()))
		span.SetAttributes(attribute.String("agent", agentVersion))
		defer span.End()

		// time the request handling
		start := time.Now()
		traceData, err := handler(ctx, s)
		if err != nil {
			slog.Debug("failed handling rpc", "protocol", s.Protocol(), tele.LogAttrError(err), tele.LogAttrPeerID(s.Conn().RemotePeer()), "agent", agentVersion)
		}
		end := time.Now()

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
	req := primitives.SSZUint64(0)
	if err := r.readRequest(ctx, stream, &req); err != nil {
		return nil, fmt.Errorf("read sequence number: %w", err)
	}

	r.metaDataMu.RLock()
	sq := primitives.SSZUint64(r.metaData.SeqNumber)
	r.metaDataMu.RUnlock()

	if err := r.writeResponse(ctx, stream, &sq); err != nil {
		return nil, fmt.Errorf("write sequence number: %w", err)
	}

	traceData := map[string]any{
		"ReceivedSeq": req,
		"SentSeq":     sq,
	}

	return traceData, stream.Close()
}

func (r *ReqResp) goodbyeHandler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	req := primitives.SSZUint64(0)
	if err := r.readRequest(ctx, stream, &req); err != nil {
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

	return traceData, stream.Close()
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

	// check if the request comes from our delegate node. If so, just mirror
	// its own status back and update our latest known status.
	if upstream.Conn().RemotePeer() == r.delegate {

		resp := &pb.Status{}
		if err := r.readRequest(ctx, upstream, resp); err != nil {
			return nil, fmt.Errorf("read status data from delegate: %w", err)
		}

		// update status
		r.SetStatus(resp)

		// mirror its own status back
		if err := r.writeResponse(ctx, upstream, resp); err != nil {
			return nil, fmt.Errorf("write mirrored status response to delegate: %w", err)
		}

		traceData := map[string]any{
			"Request":  statusTraceData(resp),
			"Response": statusTraceData(resp),
		}

		return traceData, upstream.Close()
	}

	// first, read the status from the remote peer
	req := &pb.Status{}
	if err := r.readRequest(ctx, upstream, req); err != nil {
		return nil, fmt.Errorf("read status data from delegate: %w", err)
	}

	// create response status from memory status
	resp := r.cpyStatus()

	// if the rate limiter allows requesting a new status, do that.
	if r.statusLim.Allow() {
		r.statusLim.Reserve()

		// ask our delegate node for the latest status, using our known latest status
		// this is important because blindly forwarding the request from a remote peer
		// will lead to intermittent disconnects from the beacon node. The "trusted peer"
		// setting doesn't seem to apply if we send, e.g., a status payload with
		// a non-matching fork-digest or non-finalized root hash.
		dialCtx := network.WithForceDirectDial(ctx, "prevent backoff")
		var err error
		resp, err = r.Status(dialCtx, r.delegate)
		if err != nil {
			// asking for the latest status failed. Use our own latest known status
			slog.Warn("Downstream status request failed, using the latest known status")

			statusCpy := r.cpyStatus()
			if err := r.writeResponse(ctx, upstream, statusCpy); err != nil {
				return nil, fmt.Errorf("write mirrored status response to delegate: %w", err)
			}

			traceData := map[string]any{
				"Request":  statusTraceData(req),
				"Response": statusTraceData(statusCpy),
			}

			return traceData, upstream.Close()
		}

		// we got a valid response from our delegate node. Update our own status
		r.SetStatus(resp)
	}

	// let the upstream peer (who initiated the request) know the latest status
	if err := r.writeResponse(ctx, upstream, resp); err != nil {
		return nil, fmt.Errorf("respond status to upstream: %w", err)
	}

	traceData := map[string]any{
		"Request":  statusTraceData(req),
		"Response": statusTraceData(resp),
	}

	return traceData, nil
}

func (r *ReqResp) metadataV1Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	r.metaDataMu.RLock()
	metaData := &pb.MetaDataV0{
		SeqNumber: r.metaData.SeqNumber,
		Attnets:   r.metaData.Attnets,
	}
	r.metaDataMu.RUnlock()

	if err := r.writeResponse(ctx, stream, metaData); err != nil {
		return nil, fmt.Errorf("write meta data v1: %w", err)
	}

	traceData := map[string]any{
		"SeqNumber": metaData.SeqNumber,
		"Attnets":   hex.EncodeToString(metaData.Attnets.Bytes()),
	}

	slog.Info(
		"metadata response",
		"attnets", metaData.Attnets,
	)
	return traceData, stream.Close()
}

func (r *ReqResp) metadataV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	r.metaDataMu.RLock()
	metaData := &pb.MetaDataV1{
		SeqNumber: r.metaData.SeqNumber,
		Attnets:   r.metaData.Attnets,
		Syncnets:  r.metaData.Syncnets,
	}
	r.metaDataMu.RUnlock()

	if err := r.writeResponse(ctx, stream, metaData); err != nil {
		return nil, fmt.Errorf("write meta data v2: %w", err)
	}

	traceData := map[string]any{
		"SeqNumber": metaData.SeqNumber,
		"Attnets":   hex.EncodeToString(metaData.Attnets.Bytes()),
		"Syncnets":  hex.EncodeToString(metaData.Syncnets.Bytes()),
	}
	slog.Info(
		"metadata response",
		"attnets", metaData.Attnets,
		"synccommittees", metaData.Syncnets,
	)
	return traceData, stream.Close()
}

func (r *ReqResp) blocksByRangeV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blocks by range request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

func (r *ReqResp) blocksByRootV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blocks by root request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

func (r *ReqResp) blobsByRangeV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blobs by range request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

func (r *ReqResp) blobsByRootV2Handler(ctx context.Context, stream network.Stream) (map[string]any, error) {
	if stream.Conn().RemotePeer() == r.delegate {
		return nil, fmt.Errorf("blobs by root request from delegate peer")
	}

	return nil, r.delegateStream(ctx, stream)
}

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

	slog.Info("Perform status request", tele.LogAttrPeerID(pid))
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCStatusTopicV1))
	if err != nil {
		return nil, fmt.Errorf("new stream to peer %s: %w", pid, err)
	}
	defer logDeferErr(stream.Reset, "failed closing stream") // no-op if closed

	// actually write the data to the stream
	req := r.cpyStatus()
	if req == nil {
		return nil, fmt.Errorf("status unknown")
	}

	if err := r.writeRequest(ctx, stream, req); err != nil {
		return nil, fmt.Errorf("write status request: %w", err)
	}

	// read and decode status response
	resp := &pb.Status{}
	if err := r.readResponse(ctx, stream, resp); err != nil {
		return nil, fmt.Errorf("read status response: %w", err)
	}

	// if we requested the status from our delegate
	if stream.Conn().RemotePeer() == r.delegate {
		r.SetStatus(resp)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

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
	defer logDeferErr(stream.Reset, "failed closing stream") // no-op if closed

	r.metaDataMu.RLock()
	seqNum := r.metaData.SeqNumber
	r.metaDataMu.RUnlock()

	req := primitives.SSZUint64(seqNum)
	if err := r.writeRequest(ctx, stream, &req); err != nil {
		return fmt.Errorf("write ping request: %w", err)
	}

	// read and decode status response
	resp := new(primitives.SSZUint64)
	if err := r.readResponse(ctx, stream, resp); err != nil {
		return fmt.Errorf("read ping response: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return nil
}

func (r *ReqResp) MetaData(ctx context.Context, pid peer.ID) (resp *pb.MetaDataV1, err error) {
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

	slog.Debug("Perform metadata request", tele.LogAttrPeerID(pid))
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCMetaDataTopicV2))
	if err != nil {
		return resp, fmt.Errorf("new %s stream to peer %s: %w", p2p.RPCMetaDataTopicV2, pid, err)
	}
	defer logDeferErr(stream.Reset, "failed closing stream") // no-op if closed

	// read and decode status response
	resp = &pb.MetaDataV1{}
	if err := r.readResponse(ctx, stream, resp); err != nil {
		return resp, fmt.Errorf("read ping response: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return resp, nil
}

func (r *ReqResp) BlocksByRangeV2(ctx context.Context, pid peer.ID, firstSlot, lastSlot uint64) ([]interfaces.ReadOnlySignedBeaconBlock, error) {
	var err error
	blocks := make([]interfaces.ReadOnlySignedBeaconBlock, 0, (lastSlot - firstSlot))

	startT := time.Now()

	defer func() {
		reqData := map[string]any{
			"PeerID": pid.String(),
		}

		if blocks != nil {
			reqData["RequestedBlocks"] = lastSlot - firstSlot
			reqData["ReceivedBlocks"] = len(blocks)
			reqData["Duration"] = time.Since(startT)
		}

		if err != nil {
			reqData["Error"] = err.Error()
		}

		traceEvt := &hermeshost.TraceEvent{
			Type:      "REQUEST_BLOCKS_BY_RANGE",
			PeerID:    r.host.ID(),
			Timestamp: time.Now(),
			Payload:   reqData,
		}
		traceCtx := context.Background()
		if err := r.cfg.DataStream.PutRecord(traceCtx, traceEvt); err != nil {
			slog.Warn("failed to put record", tele.LogAttrError(err))
		}

		attrs := []attribute.KeyValue{
			attribute.String("rpc", "block_by_range"),
			attribute.Bool("success", err == nil),
		}
		r.meterRequestCounter.Add(traceCtx, 1, metric.WithAttributes(attrs...))
	}()

	slog.Debug("Perform blocks_by_range request", tele.LogAttrPeerID(pid))
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCBlocksByRangeTopicV2))
	if err != nil {
		return blocks, fmt.Errorf("new %s stream to peer %s: %w", p2p.RPCMetaDataTopicV2, pid, err)
	}
	defer stream.Close()
	defer logDeferErr(stream.Reset, "failed closing stream") // no-op if closed

	req := &pb.BeaconBlocksByRangeRequest{
		StartSlot: primitives.Slot(firstSlot),
		Count:     (lastSlot - firstSlot),
		Step:      1,
	}
	if err := r.writeRequest(ctx, stream, req); err != nil {
		return blocks, fmt.Errorf("write block_by_range request: %w", err)
	}

	// read and decode status response
	process := func(blk interfaces.ReadOnlySignedBeaconBlock) error {
		blocks = append(blocks, blk)
		slog.Info(
			"got signed_beacon_block",
			slog.Attr{Key: "block_number", Value: slog.AnyValue(blk.Block().Slot())},
			slog.Attr{Key: "from", Value: slog.AnyValue(pid.String())},
		)
		return nil
	}

	for i := uint64(0); ; i++ {
		isFirstChunk := i == 0
		blk, err := r.readChunkedBlock(stream, &encoder.SszNetworkEncoder{}, isFirstChunk)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading block_by_range request: %w", err)
		}
		if err := process(blk); err != nil {
			return nil, fmt.Errorf("processing block_by_range chunk: %w", err)
		}
	}

	return blocks, nil
}

// readRequest reads a request from the given network stream and populates the
// data parameter with the decoded request. It also sets a read deadline on the
// stream and returns an error if it fails to do so. After reading the request,
// it closes the reading side of the stream and returns an error if it fails to
// do so. The method also records any errors encountered using the
// tracer configured at [ReqResp] initialization.
func (r *ReqResp) readRequest(ctx context.Context, stream network.Stream, data ssz.Unmarshaler) (err error) {
	_, span := r.cfg.Tracer.Start(ctx, "read_request")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if err = stream.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return fmt.Errorf("failed setting read deadline on stream: %w", err)
	}

	if err = r.cfg.Encoder.DecodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read request data %T: %w", data, err)
	}

	if err = stream.CloseRead(); err != nil {
		return fmt.Errorf("failed to close reading side of stream: %w", err)
	}

	return nil
}

// readResponse differs from readRequest in first reading a single byte that
// indicates the response code before actually reading the payload data. It also
// handles the response code in case it is not 0 (which would indicate success).
func (r *ReqResp) readResponse(ctx context.Context, stream network.Stream, data ssz.Unmarshaler) (err error) {
	_, span := r.cfg.Tracer.Start(ctx, "read_response")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if err = stream.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return fmt.Errorf("failed setting read deadline on stream: %w", err)
	}

	code := make([]byte, 1)
	if _, err := io.ReadFull(stream, code); err != nil {
		return fmt.Errorf("failed reading response code: %w", err)
	}

	// code == 0 means success
	// code != 0 means error
	if int(code[0]) != 0 {
		errData, err := io.ReadAll(stream)
		if err != nil {
			return fmt.Errorf("failed reading error data (code %d): %w", int(code[0]), err)
		}

		return fmt.Errorf("received error response (code %d): %s", int(code[0]), string(errData))
	}

	if err = r.cfg.Encoder.DecodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read request data %T: %w", data, err)
	}

	if err = stream.CloseRead(); err != nil {
		return fmt.Errorf("failed to close reading side of stream: %w", err)
	}

	return nil
}

// writeRequest writes the given payload data to the given stream. It sets the
// appropriate timeouts and closes the stream for further writes.
func (r *ReqResp) writeRequest(ctx context.Context, stream network.Stream, data ssz.Marshaler) (err error) {
	_, span := r.cfg.Tracer.Start(ctx, "write_request")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if err = stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("failed setting write deadline on stream: %w", err)
	}

	if _, err = r.cfg.Encoder.EncodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	if err = stream.CloseWrite(); err != nil {
		return fmt.Errorf("failed to close writing side of stream: %w", err)
	}

	return nil
}

// writeResponse differs from writeRequest in prefixing the payload data with
// a response code byte.
func (r *ReqResp) writeResponse(ctx context.Context, stream network.Stream, data ssz.Marshaler) (err error) {
	_, span := r.cfg.Tracer.Start(ctx, "write_response")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if err = stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("failed setting write deadline on stream: %w", err)
	}

	if _, err := stream.Write([]byte{0}); err != nil { // success response
		return fmt.Errorf("write success response code: %w", err)
	}

	if _, err = r.cfg.Encoder.EncodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	if err = stream.CloseWrite(); err != nil {
		return fmt.Errorf("failed to close writing side of stream: %w", err)
	}

	return nil
}

// ReadChunkedBlock handles each response chunk that is sent by the
// peer and converts it into a beacon block.
// Adaptation from Prysm's -> https://github.com/prysmaticlabs/prysm/blob/2e29164582c3665cdf5a472cd4ec9838655c9754/beacon-chain/sync/rpc_chunked_response.go#L85
func (r *ReqResp) readChunkedBlock(stream core.Stream, encoding encoder.NetworkEncoding, isFirstChunk bool) (interfaces.ReadOnlySignedBeaconBlock, error) {
	// Handle deadlines differently for first chunk
	if isFirstChunk {
		return r.readFirstChunkedBlock(stream, encoding)
	}
	return r.readResponseChunk(stream, encoding)
}

// readFirstChunkedBlock reads the first chunked block and applies the appropriate deadlines to it.
func (r *ReqResp) readFirstChunkedBlock(stream core.Stream, encoding encoder.NetworkEncoding) (interfaces.ReadOnlySignedBeaconBlock, error) {
	// read status
	code, errMsg, err := psync.ReadStatusCode(stream, encoding)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, errors.New(errMsg)
	}
	// set deadline for reading from stream
	if err = stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return nil, fmt.Errorf("failed setting write deadline on stream: %w", err)
	}
	// get fork version and block type
	forkD, err := r.readForkDigestFromStream(stream)
	if err != nil {
		return nil, err
	}
	forkV, err := GetForkVersionFromForkDigest(forkD)
	if err != nil {
		return nil, err
	}
	return r.getBlockForForkVersion(forkV, encoding, stream)
}

// readResponseChunk reads the response from the stream and decodes it into the
// provided message type.
func (r *ReqResp) readResponseChunk(stream core.Stream, encoding encoder.NetworkEncoding) (interfaces.ReadOnlySignedBeaconBlock, error) {
	if err := stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return nil, fmt.Errorf("failed setting write deadline on stream: %w", err)
	}
	code, errMsg, err := psync.ReadStatusCode(stream, encoding)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, errors.New(errMsg)
	}
	// No-op for now with the rpc context.
	forkD, err := r.readForkDigestFromStream(stream)
	if err != nil {
		return nil, err
	}
	forkV, err := GetForkVersionFromForkDigest(forkD)
	if err != nil {
		return nil, err
	}

	return r.getBlockForForkVersion(forkV, encoding, stream)
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
		return sblk, fmt.Errorf("unrecognized fork_version (received:%s) (ours: %s) (global: %s)", forkV, r.cfg.ForkDigest, DenebForkVersion)
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
