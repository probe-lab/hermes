package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/OffchainLabs/prysm/v6/api/client"
	apiCli "github.com/OffchainLabs/prysm/v6/api/client/beacon"
	"github.com/OffchainLabs/prysm/v6/api/server/structs"
	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v6/network/httputil"
	eth "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v6/time/slots"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PrysmClient is an HTTP client for Prysm's JSON RPC API.
type PrysmClient struct {
	host            string
	port            int
	auth            *AuthConfig
	nodeClient      eth.NodeClient
	timeout         time.Duration
	tracer          trace.Tracer
	beaconClient    eth.BeaconChainClient
	beaconApiClient *apiCli.Client
	genesis         *GenesisConfig
	httpClient      *http.Client
	useTLS          bool
	scheme          string
}

func NewPrysmClient(host string, portHTTP int, portGRPC int, timeout time.Duration, genesis *GenesisConfig) (*PrysmClient, error) {
	return NewPrysmClientWithTLS(host, portHTTP, portGRPC, false, timeout, genesis)
}

func NewPrysmClientWithTLS(host string, portHTTP int, portGRPC int, useTLS bool, timeout time.Duration, genesis *GenesisConfig) (*PrysmClient, error) {
	tracer := otel.GetTracerProvider().Tracer("prysm_client")

	// Parse any auth info from host we might have.
	auth, err := parseHostAuth(host)
	if err != nil {
		return nil, fmt.Errorf("parse host auth: %w", err)
	}

	// Setup gRPC options.
	var (
		dialOpts []grpc.DialOption
		scheme   string
	)

	if useTLS {
		// Use TLS for secure connections.
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, auth.Host)))
		scheme = "https"
	} else {
		// Use insecure credentials for non-TLS connections.
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		scheme = "http"
	}

	// Add auth if credentials provided.
	if auth.Username != "" && auth.Password != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&basicAuthCreds{
			username: auth.Username,
			password: auth.Password,
		}))
	}

	// Create gRPC client.
	conn, err := grpc.NewClient(
		fmt.Sprintf("%s:%d", auth.Host, portGRPC),
		dialOpts...,
	)
	if err != nil {
		return nil, fmt.Errorf("new grpc connection: %w", err)
	}

	// Create API client options.
	var (
		opts       = []client.ClientOpt{}
		httpClient = http.DefaultClient
	)

	// Add auth transport if credentials provided.
	if auth.Username != "" && auth.Password != "" {
		transport := &basicAuthTransport{
			username: auth.Username,
			password: auth.Password,
			base:     http.DefaultTransport,
		}

		opts = append(opts, client.WithRoundTripper(transport))

		httpClient = &http.Client{
			Transport: transport,
		}
	}

	// Create API client.
	apiCli, err := apiCli.NewClient(
		fmt.Sprintf("%s://%s:%d", scheme, auth.Host, portHTTP),
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("new http api client: %w", err)
	}

	return &PrysmClient{
		host:            auth.Host,
		auth:            auth,
		port:            portHTTP,
		scheme:          scheme,
		nodeClient:      eth.NewNodeClient(conn),
		beaconClient:    eth.NewBeaconChainClient(conn),
		beaconApiClient: apiCli,
		timeout:         timeout,
		tracer:          tracer,
		genesis:         genesis,
		httpClient:      httpClient,
		useTLS:          useTLS,
	}, nil
}

func (p *PrysmClient) AddTrustedPeer(ctx context.Context, pid peer.ID, maddr ma.Multiaddr) (err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.addTrustedPeer", trace.WithAttributes(attribute.String("pid", pid.String())))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	payload := structs.AddrRequest{
		Addr: fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
	}

	u := url.URL{
		Scheme: p.scheme,
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

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("add trusted peer http post failed: %w", err)
	}
	defer logDeferErr(resp.Body.Close, "Failed closing body")

	if resp.StatusCode != http.StatusOK {
		return parseErrorResponse(resp)
	}

	return nil
}

func (p *PrysmClient) ListTrustedPeers(ctx context.Context) (peers map[peer.ID]*structs.Peer, err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.listTrustedPeers")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	u := url.URL{
		Scheme: p.scheme,
		Host:   fmt.Sprintf("%s:%d", p.host, p.port),
		Path:   "/prysm/node/trusted_peers",
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("new list trusted peer request: %w", err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list trusted peer http get failed: %w", err)
	}
	defer logDeferErr(resp.Body.Close, "Failed closing body")

	if resp.StatusCode != http.StatusOK {
		return nil, parseErrorResponse(resp)
	}

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}

	peerResp := &structs.PeersResponse{}
	if err := json.Unmarshal(respData, peerResp); err != nil {
		return nil, fmt.Errorf("failed unmarshalling response data: %w", err)
	}

	peerData := make(map[peer.ID]*structs.Peer, len(peerResp.Peers))
	for _, p := range peerResp.Peers {
		pid, err := peer.Decode(p.PeerId)
		if err != nil {
			return nil, fmt.Errorf("decode peer ID %s: %w", p.PeerId, err)
		}
		peerData[pid] = p
	}

	return peerData, nil
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
		Scheme: p.scheme,
		Host:   fmt.Sprintf("%s:%d", p.host, p.port),
		Path:   "/prysm/node/trusted_peers/" + pid.String(),
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("new remove trusted peer request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("remove trusted peer http delete failed: %w", err)
	}
	defer logDeferErr(resp.Body.Close, "Failed closing body")

	if resp.StatusCode != http.StatusOK {
		return parseErrorResponse(resp)
	}

	return nil
}

//lint:ignore SA1019 gRPC API deprecated but still supported until v8 (2026)
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

	return p.beaconClient.GetChainHead(ctx, &emptypb.Empty{}) //lint:ignore SA1019 I don't see an alternative
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

	//lint:ignore SA1019 gRPC API deprecated but still supported until v8 (2026)
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

// FetchAndSetBlobSchedule fetches the BLOB_SCHEDULE from Prysm's spec endpoint
// and sets it in the params configuration to ensure correct fork digest calculation for BPO.
func (p *PrysmClient) FetchAndSetBlobSchedule(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	specResp, err := p.beaconApiClient.GetConfigSpec(ctx)
	if err != nil {
		return fmt.Errorf("failed fetching config spec: %w", err)
	}

	// Type assert Data to map[string]interface{}
	specData, ok := specResp.Data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected spec response data type: %T", specResp.Data)
	}

	// Check if BLOB_SCHEDULE exists in the spec, if no BLOB_SCHEDULE
	// that means we're not on a BPO-enabled network.
	blobScheduleRaw, exists := specData["BLOB_SCHEDULE"]
	if !exists {
		return nil
	}

	// Parse the BLOB_SCHEDULE.
	var blobScheduleData []struct {
		Epoch            string `json:"EPOCH"`
		MaxBlobsPerBlock string `json:"MAX_BLOBS_PER_BLOCK"`
	}

	switch v := blobScheduleRaw.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &blobScheduleData); err != nil {
			return fmt.Errorf("failed parsing BLOB_SCHEDULE string: %w", err)
		}
	case []interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed marshaling BLOB_SCHEDULE array: %w", err)
		}

		if err := json.Unmarshal(jsonBytes, &blobScheduleData); err != nil {
			return fmt.Errorf("failed parsing BLOB_SCHEDULE array: %w", err)
		}
	default:
		return fmt.Errorf("BLOB_SCHEDULE has unexpected type: %T", blobScheduleRaw)
	}

	// Convert to BlobScheduleEntry format.
	blobSchedule := make([]params.BlobScheduleEntry, len(blobScheduleData))
	for i, entry := range blobScheduleData {
		epoch, err := strconv.ParseUint(entry.Epoch, 10, 64)
		if err != nil {
			return fmt.Errorf("failed parsing EPOCH %s: %w", entry.Epoch, err)
		}

		maxBlobs, err := strconv.ParseUint(entry.MaxBlobsPerBlock, 10, 64)
		if err != nil {
			return fmt.Errorf("failed parsing MAX_BLOBS_PER_BLOCK %s: %w", entry.MaxBlobsPerBlock, err)
		}

		blobSchedule[i] = params.BlobScheduleEntry{
			Epoch:            primitives.Epoch(epoch),
			MaxBlobsPerBlock: maxBlobs,
		}
	}

	// Update the beacon config with the BlobSchedule + set GenesisValidatorsRoot.
	// This gives us the correct details for InitializeForkSchedule().
	config := params.BeaconConfig().Copy()
	config.BlobSchedule = blobSchedule
	copy(config.GenesisValidatorsRoot[:], p.genesis.GenesisValidatorRoot)
	config.InitializeForkSchedule()

	// Override the config with updated BlobSchedule.
	params.OverrideBeaconConfig(config)

	return nil
}

func (p *PrysmClient) getActiveValidatorCount(ctx context.Context) (activeVals uint64, err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.active_validators")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	//lint:ignore SA1019 gRPC API deprecated but still supported until v8 (2026)
	actVals, err := p.beaconClient.ListValidators(ctx, &eth.ListValidatorsRequest{Active: true})
	if err != nil {
		return 0, err
	}
	activeVals = uint64(actVals.GetTotalSize())
	return activeVals, nil
}

func (p *PrysmClient) isOnNetwork(ctx context.Context, hermesForkDigest [4]byte) (onNetwork bool, err error) {
	ctx, span := p.tracer.Start(ctx, "prysm_client.check_on_same_fork_digest")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	// this checks whether the local fork_digest at hermes matches the one that the remote node keeps
	// Get the chain head to determine the current epoch
	chainHead, err := p.ChainHead(ctx)
	if err != nil {
		return false, fmt.Errorf("get chain head: %w", err)
	}

	// Calculate the current epoch from the head slot
	currentEpoch := slots.ToEpoch(chainHead.HeadSlot)

	// We *must* use the current epoch from chain head, not the fork activation
	// epoch in-order for our fork digests to be valid.
	forkDigest := params.ForkDigest(currentEpoch)

	// check if our version is within the versions of the node
	if forkDigest == hermesForkDigest {
		return true, nil
	}
	return false, nil
}

func parseErrorResponse(resp *http.Response) error {
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return errors.New("authorization required")
	default:
		respData, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed reading response body: %w", err)
		}

		errResp := &httputil.DefaultJsonError{}

		if err := json.Unmarshal(respData, errResp); err != nil {
			return fmt.Errorf("failed unmarshalling response data: %w", err)
		}

		return errResp
	}
}
