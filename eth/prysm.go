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
	rpcnode "github.com/prysmaticlabs/prysm/v4/beacon-chain/rpc/eth/node"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/rpc/prysm/node"
	"github.com/prysmaticlabs/prysm/v4/network/httputil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// PrysmClient is an HTTP client for Prysm's JSON RPC API.
type PrysmClient struct {
	host    string
	port    int
	timeout time.Duration
	tracer  trace.Tracer
}

func NewPrysmClient(host string, port int) *PrysmClient {
	tracer := otel.GetTracerProvider().Tracer("prysm_client")
	timeout := 5 * time.Second

	return &PrysmClient{
		host:    host,
		port:    port,
		timeout: timeout,
		tracer:  tracer,
	}
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

func (p *PrysmClient) Identity(ctx context.Context) (idResp *rpcnode.GetIdentityResponse, err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.identity")
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
		Path:   "/eth/v1/node/identity",
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("new get prysm identity request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get prysm identity http get failed: %w", err)
	}
	defer logDeferErr(resp.Body.Close, "Failed closing body")

	if resp.StatusCode != http.StatusOK {
		errResp := &httputil.DefaultJsonError{}
		respData, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed reading response body: %w", err)
		}

		if err := json.Unmarshal(respData, errResp); err != nil {
			return nil, fmt.Errorf("failed unmarshalling response data: %w", err)
		}

		return nil, errResp
	}

	idResp = &rpcnode.GetIdentityResponse{}
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}

	if err := json.Unmarshal(respData, idResp); err != nil {
		return nil, fmt.Errorf("failed unmarshalling response data: %w", err)
	}

	return idResp, nil
}

// AddrInfo calls the beaconAPI /eth/v1/node/identity endpoint
// and extracts the beacon node addr info object.
func (p *PrysmClient) AddrInfo(ctx context.Context) (*peer.AddrInfo, error) {
	beaIdentity, err := p.Identity(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to contact prysm node: %w", err)
	}

	beaAddrInfo := &peer.AddrInfo{Addrs: []ma.Multiaddr{}}
	for _, p2pMaddr := range beaIdentity.Data.P2PAddresses {
		maddr, err := ma.NewMultiaddr(p2pMaddr)
		if err != nil {
			return nil, fmt.Errorf("parse prysm node identity multiaddress: %w", err)
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("parse prysm node identity p2p maddr: %w", err)
		}

		if beaAddrInfo.ID.Size() != 0 && beaAddrInfo.ID != addrInfo.ID {
			return nil, fmt.Errorf("received inconsistend prysm node identity peer IDs %s != %s", beaAddrInfo.ID, addrInfo.ID)
		}

		beaAddrInfo.ID = addrInfo.ID
		beaAddrInfo.Addrs = append(beaAddrInfo.Addrs, addrInfo.Addrs...)
	}

	return beaAddrInfo, nil
}
