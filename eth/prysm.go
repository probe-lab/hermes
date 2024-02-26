package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/rpc/prysm/node"
	"github.com/prysmaticlabs/prysm/v4/network/httputil"
	eth "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PrysmClient is an HTTP client for Prysm's JSON RPC API.
type PrysmClient struct {
	host         string
	port         int
	nodeClient   eth.NodeClient
	timeout      time.Duration
	tracer       trace.Tracer
	beaconClient eth.BeaconChainClient
}

func NewPrysmClient(host string, portHTTP int, portGRPC int, timeout time.Duration) (*PrysmClient, error) {
	tracer := otel.GetTracerProvider().Tracer("prysm_client")

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", host, portGRPC), grpc.WithTransportCredentials(
		insecure.NewCredentials(),
	))
	if err != nil {
		return nil, fmt.Errorf("new grpc connection: %w", err)
	}

	return &PrysmClient{
		host:         host,
		port:         portHTTP,
		nodeClient:   eth.NewNodeClient(conn),
		beaconClient: eth.NewBeaconChainClient(conn),
		timeout:      timeout,
		tracer:       tracer,
	}, nil
}

func (p *PrysmClient) AddTrustedPeer(ctx context.Context, addrInfo peer.AddrInfo) (err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.addTrustedPeer", trace.WithAttributes(attribute.String("pid", addrInfo.ID.String())))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	if len(addrInfo.Addrs) != 1 {
		return fmt.Errorf("trusted peer has %d addresses, expected exactly 1", len(addrInfo.Addrs))
	}

	maddrs, err := peer.AddrInfoToP2pAddrs(&addrInfo)
	if err != nil {
		return fmt.Errorf("failed to construct p2p addr from addrinfo: %w", err)
	}

	payload := node.AddrRequest{
		Addr: maddrs[0].String(), // save because we checked the length above
	}

	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", p.host, p.port),
		Path:   "/prysm/node/trusted_peers",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal add trusted peer payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("new add trusted peer request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", strconv.Itoa(len(data)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("add trusted peer http post failed: %w", err)
	}
	defer logDeferErr(resp.Body.Close, "Failed closing body")

	if resp.StatusCode != http.StatusOK {
		errResp := &httputil.DefaultJsonError{}
		respData, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed reading response body: %w", err)
		}

		if err := json.Unmarshal(respData, errResp); err != nil {
			return fmt.Errorf("failed unmarshalling response data: %w", err)
		}

		return errResp
	}

	return nil
}

func (p *PrysmClient) RemoveTrustedPeer(ctx context.Context, pid peer.ID) (err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.removeTrustedPeer", trace.WithAttributes(attribute.String("pid", pid.String())))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", p.host, p.port),
		Path:   "/prysm/node/trusted_peers/" + pid.String(),
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("new remove trusted peer request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("remove trusted peer http delete failed: %w", err)
	}
	defer logDeferErr(resp.Body.Close, "Failed closing body")

	if resp.StatusCode != http.StatusOK {
		errResp := &httputil.DefaultJsonError{}
		respData, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed reading response body: %w", err)
		}

		if err := json.Unmarshal(respData, errResp); err != nil {
			return fmt.Errorf("failed unmarshalling response data: %w", err)
		}

		return errResp
	}

	return nil
}

func (p *PrysmClient) ChainHead(ctx context.Context) (chainHead *eth.ChainHead, err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.chain_head")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	return p.beaconClient.GetChainHead(ctx, &emptypb.Empty{})
}

func (p *PrysmClient) Identity(ctx context.Context) (addrInfo *peer.AddrInfo, err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.identity")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	hostData, err := p.nodeClient.GetHost(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	pid, err := peer.Decode(hostData.PeerId)
	if err != nil {
		return nil, fmt.Errorf("decode peer ID %s: %w", hostData.PeerId, err)
	}

	addrInfo = &peer.AddrInfo{
		ID:    pid,
		Addrs: make([]ma.Multiaddr, 0, len(hostData.Addresses)),
	}

	for _, addr := range hostData.Addresses {

		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("parse host data multiaddress %s: %w", addr, err)
		}

		addrInfo.Addrs = append(addrInfo.Addrs, maddr)
	}

	return addrInfo, nil
}
