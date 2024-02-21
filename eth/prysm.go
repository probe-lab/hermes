package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/rpc/prysm/node"
	"github.com/prysmaticlabs/prysm/v4/network/httputil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/tele"
)

type IPrysmClient interface {
	AddTrustedPeer(ctx context.Context, addrInfo peer.AddrInfo) (err error)
	RemoveTrustedPeer(ctx context.Context, pid peer.ID) (err error)
}

// PrysmClient is an HTTP client for Prysm's JSON RPC API.
type PrysmClient struct {
	host    string
	port    int
	timeout time.Duration
	tracer  trace.Tracer
}

var _ IPrysmClient = (*PrysmClient)(nil)

func NewPrysmClient(host string, port int) *PrysmClient {
	return &PrysmClient{
		host:    host,
		port:    port,
		timeout: 5 * time.Second,
		tracer:  otel.GetTracerProvider().Tracer("prysm_client"),
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

	payload := node.AddrRequest{
		Addr: fmt.Sprintf("%s/p2p/%s", addrInfo.Addrs[0].String(), addrInfo.ID.String()),
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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("Failed closing body", tele.LogAttrError(err))
		}
	}()

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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("Failed closing body", tele.LogAttrError(err))
		}
	}()

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

// NoopPrysmClient doesn't do anything
type NoopPrysmClient struct{}

var _ IPrysmClient = (*NoopPrysmClient)(nil)

func (n NoopPrysmClient) AddTrustedPeer(ctx context.Context, addrInfo peer.AddrInfo) (err error) {
	return nil
}

func (n NoopPrysmClient) RemoveTrustedPeer(ctx context.Context, pid peer.ID) (err error) {
	return nil
}
