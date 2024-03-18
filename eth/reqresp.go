package eth

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	ssz "github.com/ferranbt/fastssz"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/types"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	eth "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	pb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	hermeshost "github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type ReqRespConfig struct {
	ForkDigest [4]byte
	Encoder    encoder.NetworkEncoding
	DataStream hermeshost.DataStream

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

	statusMu sync.RWMutex
	status   *eth.Status

	// metrics
	meterRequestCounter metric.Int64Counter
	latencyHistogram    metric.Float64Histogram
}

type ContextStreamHandler func(context.Context, network.Stream) (map[string]any, error)

func NewReqResp(h host.Host, cfg *ReqRespConfig) (*ReqResp, error) {
	if cfg == nil {
		return nil, fmt.Errorf("req resp server config must not be nil")
	}

	md := &pb.MetaDataV1{
		SeqNumber: 0,
		Attnets:   bitfield.NewBitvector64(),
		Syncnets:  bitfield.Bitvector4{byte(0x00)},
	}

	// fake to support all attnets
	for i := uint64(0); i < md.Attnets.Len(); i++ {
		md.Attnets.SetBitAt(i, true)
	}

	p := &ReqResp{
		host:     h,
		cfg:      cfg,
		metaData: md,
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

func (r *ReqResp) SetStatus(status *eth.Status) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()

	if r.status != nil && !bytes.Equal(r.status.ForkDigest, status.ForkDigest) {
		slog.Warn("reqresp status updated with different fork digests", "old", hex.EncodeToString(r.status.ForkDigest), "new", hex.EncodeToString(status.ForkDigest))
	}

	slog.Info("New status:")
	slog.Info("  fork_digest: " + hex.EncodeToString(status.ForkDigest))
	slog.Info("  finalized_root: " + hex.EncodeToString(status.FinalizedRoot))
	slog.Info("  finalized_epoch: " + strconv.FormatUint(uint64(status.FinalizedEpoch), 10))
	slog.Info("  head_root: " + hex.EncodeToString(status.HeadRoot))
	slog.Info("  head_slot: " + strconv.FormatUint(uint64(status.HeadSlot), 10))

	r.status = status
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
		p2p.RPCPingTopicV1:          r.pingHandler,
		p2p.RPCGoodByeTopicV1:       r.goodbyeHandler,
		p2p.RPCStatusTopicV1:        r.statusHandler,
		p2p.RPCMetaDataTopicV1:      r.metadataV1Handler,
		p2p.RPCMetaDataTopicV2:      r.metadataV2Handler,
		p2p.RPCBlocksByRangeTopicV2: r.blocksByRangeV2Handler,
		p2p.RPCBlocksByRootTopicV2:  r.blocksByRootV2Handler,
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
				agentVersion = av
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

		// Usual protocol string: /eth2/beacon_chain/req/metadata/2/ssz_snappy
		parts := strings.Split(string(s.Protocol()), "/")
		if len(parts) > 4 {
			traceType = "HANDLE_" + strings.ToUpper(parts[4])
		}

		commonData := map[string]any{
			"RemotePeer": s.Conn().RemotePeer(),
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

func (r *ReqResp) cpyStatus() *eth.Status {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	if r.status == nil {
		return nil
	}

	return &eth.Status{
		ForkDigest:     bytes.Clone(r.status.ForkDigest),
		FinalizedRoot:  bytes.Clone(r.status.FinalizedRoot),
		FinalizedEpoch: r.status.FinalizedEpoch,
		HeadRoot:       bytes.Clone(r.status.HeadRoot),
		HeadSlot:       r.status.HeadSlot,
	}
}

func (r *ReqResp) verifyStatus(status *eth.Status) error {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	if !bytes.Equal(status.ForkDigest, r.status.ForkDigest) {
		return fmt.Errorf("fork digest mismatch")
	}
	return nil
}

func (r *ReqResp) statusHandler(ctx context.Context, upstream network.Stream) (map[string]any, error) {
	if upstream.Conn().RemotePeer() == r.delegate {
		// because we pass the status request through to the upstream,
		// we need to handle its requests differently

		resp := &eth.Status{}
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
			"Request": map[string]any{
				"ForkDigest":     hex.EncodeToString(resp.ForkDigest),
				"HeadRoot":       hex.EncodeToString(resp.HeadRoot),
				"HeadSlot":       resp.HeadSlot,
				"FinalizedRoot":  hex.EncodeToString(resp.FinalizedRoot),
				"FinalizedEpoch": resp.FinalizedEpoch,
			},
			"Response": map[string]any{
				"ForkDigest":     hex.EncodeToString(resp.ForkDigest),
				"HeadRoot":       hex.EncodeToString(resp.HeadRoot),
				"HeadSlot":       resp.HeadSlot,
				"FinalizedRoot":  hex.EncodeToString(resp.FinalizedRoot),
				"FinalizedEpoch": resp.FinalizedEpoch,
			},
		}

		return traceData, upstream.Close()
	}

	req := &eth.Status{}
	if err := r.readRequest(ctx, upstream, req); err != nil {
		return nil, fmt.Errorf("read status data from delegate: %w", err)
	}

	dialCtx := network.WithForceDirectDial(ctx, "prevent backoff")
	resp, err := r.Status(dialCtx, r.delegate)
	if err != nil {

		slog.Warn("Downstream status request failed, using the latest known status")

		statusCpy := r.cpyStatus()
		if err := r.writeResponse(ctx, upstream, statusCpy); err != nil {
			return nil, fmt.Errorf("write mirrored status response to delegate: %w", err)
		}

		traceData := map[string]any{
			"Request": map[string]any{
				"ForkDigest":     hex.EncodeToString(req.ForkDigest),
				"HeadRoot":       hex.EncodeToString(req.HeadRoot),
				"HeadSlot":       req.HeadSlot,
				"FinalizedRoot":  hex.EncodeToString(req.FinalizedRoot),
				"FinalizedEpoch": req.FinalizedEpoch,
			},
			"Response": map[string]any{
				"ForkDigest":     hex.EncodeToString(statusCpy.ForkDigest),
				"HeadRoot":       hex.EncodeToString(statusCpy.HeadRoot),
				"HeadSlot":       statusCpy.HeadSlot,
				"FinalizedRoot":  hex.EncodeToString(statusCpy.FinalizedRoot),
				"FinalizedEpoch": statusCpy.FinalizedEpoch,
			},
		}

		return traceData, upstream.Close()
	}

	// update status
	r.SetStatus(resp)

	if err := r.writeResponse(ctx, upstream, resp); err != nil {
		return nil, fmt.Errorf("respond status to upstream: %w", err)
	}

	traceData := map[string]any{
		"Request": map[string]any{
			"ForkDigest":     hex.EncodeToString(req.ForkDigest),
			"HeadRoot":       hex.EncodeToString(req.HeadRoot),
			"HeadSlot":       req.HeadSlot,
			"FinalizedRoot":  hex.EncodeToString(req.FinalizedRoot),
			"FinalizedEpoch": req.FinalizedEpoch,
		},
		"Response": map[string]any{
			"ForkDigest":     hex.EncodeToString(resp.ForkDigest),
			"HeadRoot":       hex.EncodeToString(resp.HeadRoot),
			"HeadSlot":       resp.HeadSlot,
			"FinalizedRoot":  hex.EncodeToString(resp.FinalizedRoot),
			"FinalizedEpoch": resp.FinalizedEpoch,
		},
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

func (r *ReqResp) Status(ctx context.Context, pid peer.ID) (status *eth.Status, err error) {
	defer func() {
		reqData := map[string]any{}
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
	resp := &eth.Status{}
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
		reqData := map[string]any{}
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
	resp = &eth.MetaDataV1{}
	if err := r.readResponse(ctx, stream, resp); err != nil {
		return resp, fmt.Errorf("read ping response: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return resp, nil
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
