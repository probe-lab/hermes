package eth

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	ssz "github.com/ferranbt/fastssz"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/types"
	"github.com/prysmaticlabs/prysm/v4/consensus-types/primitives"
	eth "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"
	pb "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/tele"
)

type ReqRespConfig struct {
	ForkDigest [4]byte

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Telemetry accessors
	Tracer trace.Tracer
	Meter  metric.Meter
}

type ReqResp struct {
	host     host.Host
	cfg      *ReqRespConfig
	delegate peer.ID // peer ID that we delegate requests to
	enc      encoder.NetworkEncoding

	metaDataMu sync.RWMutex
	metaData   *pb.MetaDataV1

	statusMu sync.RWMutex
	status   *eth.Status

	// metrics
	meterRequestCounter metric.Int64Counter
	latencyHistogram    metric.Float64Histogram
}

type ContextStreamHandler func(context.Context, network.Stream) error

func NewReqResp(h host.Host, cfg *ReqRespConfig) (*ReqResp, error) {
	if cfg == nil {
		return nil, fmt.Errorf("req resp server config must not be nil")
	}

	md := &pb.MetaDataV1{
		SeqNumber: 0,
		Attnets:   bitfield.NewBitvector64(),
		Syncnets:  bitfield.Bitvector4{byte(0x00)},
	}

	p := &ReqResp{
		host:     h,
		cfg:      cfg,
		enc:      encoder.SszNetworkEncoder{},
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

func (r *ReqResp) RegisterHandlers(ctx context.Context) {
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
		r.host.SetStreamHandler(protocolID, r.wrapStreamHandler(ctx, string(protocolID), handler))
	}
}

func (r *ReqResp) protocolID(topic string) protocol.ID {
	return protocol.ID(topic + r.enc.ProtocolSuffix())
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
		if err := handler(ctx, s); err != nil {
			slog.Debug("failed handling rpc", "protocol", s.Protocol(), tele.LogAttrError(err), tele.LogAttrPeerID(s.Conn().RemotePeer()))
		}
		end := time.Now()

		// update meters
		r.meterRequestCounter.Add(ctx, 1, mattrs)
		r.latencyHistogram.Record(ctx, float64(end.Sub(start).Milliseconds()), mattrs)
	}
}

func (r *ReqResp) pingHandler(ctx context.Context, stream network.Stream) error {
	req := primitives.SSZUint64(0)
	if err := r.ReadAndClose(ctx, stream, &req); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	r.metaDataMu.RLock()
	sq := primitives.SSZUint64(r.metaData.SeqNumber)
	r.metaDataMu.RUnlock()

	if err := r.WriteAndClose(ctx, stream, &sq); err != nil {
		return fmt.Errorf("write sequence number: %w", err)
	}

	return stream.Close()
}

func (r *ReqResp) goodbyeHandler(ctx context.Context, stream network.Stream) error {
	req := primitives.SSZUint64(0)
	if err := r.ReadAndClose(ctx, stream, &req); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	msg, found := types.GoodbyeCodeMessages[req]
	if found {
		slog.Debug("Received goodbye message", tele.LogAttrPeerID(stream.Conn().RemotePeer()), "msg", msg)
	}

	return stream.Close()
}

func (r *ReqResp) statusHandler(ctx context.Context, upstream network.Stream) error {
	if upstream.Conn().RemotePeer() == r.delegate {
		// because we pass the status request through to the upstream,
		// we need to handle its requests differently

		resp := &eth.Status{}
		if err := r.ReadAndClose(ctx, upstream, resp); err != nil {
			return fmt.Errorf("read status data from delegate: %w", err)
		}

		r.statusMu.Lock()
		if r.status == nil {
			slog.Info("Received first delegate status", "head_slot", resp.HeadSlot, "finalized_epoch", resp.FinalizedEpoch)
		}
		r.status = resp
		r.statusMu.Unlock()

		// mirror its own status back
		if err := r.WriteAndClose(ctx, upstream, resp); err != nil {
			return fmt.Errorf("write mirrored status response to delegate: %w", err)
		}

		return upstream.Close()
	}

	downstream, err := r.host.NewStream(ctx, r.delegate, upstream.Protocol())
	if err != nil {
		return fmt.Errorf("new stream to downstream host: %w", err)
	}
	defer logDeferErr(downstream.Reset, "failed resetting downstream stream")

	// send status request to downstream peer. This will stop as soon as the
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
	respReader := io.TeeReader(downstream, upstream)

	if err = r.ReadRespCode(ctx, respReader); err != nil {
		return fmt.Errorf("response code: %w", err)
	}

	// read and decode status response
	resp := &eth.Status{}
	if err := r.enc.DecodeWithMaxLength(respReader, resp); err != nil {
		return fmt.Errorf("failed reading response data: %w", err)
	}

	r.statusMu.Lock()
	if r.status == nil {
		slog.Info("Received first delegate status", "head_slot", resp.HeadSlot, "finalized_epoch", resp.FinalizedEpoch)
	}
	r.status = resp
	r.statusMu.Unlock()

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

func (r *ReqResp) metadataV1Handler(ctx context.Context, stream network.Stream) error {
	r.metaDataMu.RLock()
	metaData := &pb.MetaDataV0{
		SeqNumber: r.metaData.SeqNumber,
		Attnets:   r.metaData.Attnets,
	}
	r.metaDataMu.RUnlock()

	if err := r.WriteAndClose(ctx, stream, metaData); err != nil {
		return fmt.Errorf("write meta data v1: %w", err)
	}

	return stream.Close()
}

func (r *ReqResp) metadataV2Handler(ctx context.Context, stream network.Stream) error {
	r.metaDataMu.RLock()
	defer r.metaDataMu.RUnlock()

	if err := r.WriteAndClose(ctx, stream, r.metaData); err != nil {
		return fmt.Errorf("write meta data v2: %w", err)
	}

	return stream.Close()
}

func (r *ReqResp) blocksByRangeV2Handler(ctx context.Context, stream network.Stream) error {
	if stream.Conn().RemotePeer() == r.delegate {
		return fmt.Errorf("blocks by range request from delegate peer")
	}

	return r.delegateStream(ctx, stream)
}

func (r *ReqResp) blocksByRootV2Handler(ctx context.Context, stream network.Stream) error {
	if stream.Conn().RemotePeer() == r.delegate {
		return fmt.Errorf("blocks by root request from delegate peer")
	}

	return r.delegateStream(ctx, stream)
}

func (r *ReqResp) delegateStream(ctx context.Context, upstream network.Stream) error {
	downstream, err := r.host.NewStream(ctx, r.delegate, upstream.Protocol())
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

func (r *ReqResp) ReadRespCode(ctx context.Context, reader io.Reader) error {
	code := make([]byte, 1)
	if _, err := io.ReadFull(reader, code); err != nil {
		return fmt.Errorf("failed reading response code: %w", err)
	}

	// code == 0 means success
	// code != 0 means error
	if int(code[0]) != 0 {
		errData, _ := io.ReadAll(reader)
		return fmt.Errorf("failed reading error data: %s", string(errData))
	}

	return nil
}

func (r *ReqResp) ReadAndClose(ctx context.Context, stream network.Stream, data ssz.Unmarshaler) (err error) {
	_, span := r.cfg.Tracer.Start(ctx, "read")
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

	if err = r.enc.DecodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	if err = stream.CloseRead(); err != nil {
		return fmt.Errorf("failed to close reading side of stream: %w", err)
	}

	return nil
}

func (r *ReqResp) WriteAndClose(ctx context.Context, stream network.Stream, data ssz.Marshaler) (err error) {
	_, span := r.cfg.Tracer.Start(ctx, "write")
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

	if _, err = r.enc.EncodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	if err = stream.CloseWrite(); err != nil {
		return fmt.Errorf("failed to close writing side of stream: %w", err)
	}

	return nil
}

func (r *ReqResp) Status(ctx context.Context, pid peer.ID) (*eth.Status, error) {
	slog.Debug("Do status handshake")
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCStatusTopicV1))
	if err != nil {
		return nil, fmt.Errorf("new stream to peer %s: %w", pid, err)
	}
	defer logDeferErr(stream.Reset, "failed closing stream") // no-op if closed

	// actually write the data to the stream
	r.statusMu.RLock()
	var req *eth.Status
	if r.status == nil {
		req = &eth.Status{
			ForkDigest:     r.cfg.ForkDigest[:],
			FinalizedRoot:  []byte{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4},
			FinalizedEpoch: 1,
			HeadRoot:       []byte{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4},
			HeadSlot:       1,
		}
	} else {
		req = &eth.Status{
			ForkDigest:     r.status.ForkDigest,
			FinalizedRoot:  r.status.FinalizedRoot,
			FinalizedEpoch: r.status.FinalizedEpoch,
			HeadRoot:       r.status.HeadRoot,
			HeadSlot:       r.status.HeadSlot,
		}
	}
	r.statusMu.RUnlock()

	if err = stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return nil, fmt.Errorf("failed setting write deadline on stream: %w", err)
	}

	if _, err = r.enc.EncodeWithMaxLength(stream, req); err != nil {
		return nil, fmt.Errorf("read sequence number: %w", err)
	}

	if err = stream.CloseWrite(); err != nil {
		return nil, fmt.Errorf("failed to close writing side of stream: %w", err)
	}

	// define the maximum time we allow for receiving a response
	if err = stream.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil { // TODO: parameterize timeout
		return nil, fmt.Errorf("failed to set write deadline: %w", err)
	}

	if err := r.ReadRespCode(ctx, stream); err != nil {
		return nil, fmt.Errorf("read status response code")
	}

	// read and decode status response
	resp := &eth.Status{}
	if err := r.ReadAndClose(ctx, stream, resp); err != nil {
		return nil, fmt.Errorf("read status response: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close()

	return resp, nil
}
