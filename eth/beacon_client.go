package eth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	rpcnode "github.com/prysmaticlabs/prysm/v4/beacon-chain/rpc/eth/node"
	"github.com/prysmaticlabs/prysm/v4/network/httputil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type BeaconClient interface {
	Identity(ctx context.Context) (*rpcnode.GetIdentityResponse, error)
}

type BeaconAPIClient struct {
	host    string
	port    int
	timeout time.Duration
	tracer  trace.Tracer
}

func NewBeaconAPIClient(host string, port int) *BeaconAPIClient {
	tracer := otel.GetTracerProvider().Tracer("beacon_client")
	timeout := 5 * time.Second

	return &BeaconAPIClient{
		host:    host,
		port:    port,
		timeout: timeout,
		tracer:  tracer,
	}
}

func (b *BeaconAPIClient) Identity(ctx context.Context) (idResp *rpcnode.GetIdentityResponse, err error) {
	ctx, span := b.tracer.Start(ctx, "beacon_client.identity")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", b.host, b.port),
		Path:   "/eth/v1/node/identity",
	}

	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("new get beacon peer identity request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get beacon peer identity http get failed: %w", err)
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

// NoopBeaconClient doesn't do anything. See documentation of [BeaconClient]
// for the rationale.
type NoopBeaconClient struct{}

var _ BeaconClient = (*NoopBeaconClient)(nil)

func (n NoopBeaconClient) Identity(ctx context.Context) (*rpcnode.GetIdentityResponse, error) {
	return nil, nil
}
