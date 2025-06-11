//go:build integration
// +build integration

package eth_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/probe-lab/hermes/eth"
	hermeshost "github.com/probe-lab/hermes/host"
)

// TestPeerFilter_Integration tests peer filtering with real libp2p hosts
func TestPeerFilter_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.Default()

	// Test scenarios
	tests := []struct {
		name           string
		filterMode     eth.FilterMode
		filterPatterns []string
		hermesAgent    string
		peerAgent      string
		shouldConnect  bool
	}{
		{
			name:           "denylist blocks hermes peers",
			filterMode:     eth.FilterModeDenylist,
			filterPatterns: []string{"^hermes.*"},
			hermesAgent:    "hermes/v1.0.0",
			peerAgent:      "hermes/v2.0.0",
			shouldConnect:  false,
		},
		{
			name:           "denylist allows non-matching peers",
			filterMode:     eth.FilterModeDenylist,
			filterPatterns: []string{"^hermes.*"},
			hermesAgent:    "hermes/v1.0.0",
			peerAgent:      "prysm/v3.0.0",
			shouldConnect:  true,
		},
		{
			name:           "allowlist allows matching peers",
			filterMode:     eth.FilterModeAllowlist,
			filterPatterns: []string{"^prysm.*", "^lighthouse.*"},
			hermesAgent:    "hermes/v1.0.0",
			peerAgent:      "prysm/v3.0.0",
			shouldConnect:  true,
		},
		{
			name:           "allowlist blocks non-matching peers",
			filterMode:     eth.FilterModeAllowlist,
			filterPatterns: []string{"^prysm.*", "^lighthouse.*"},
			hermesAgent:    "hermes/v1.0.0",
			peerAgent:      "teku/v1.0.0",
			shouldConnect:  false,
		},
		{
			name:           "disabled mode allows all peers",
			filterMode:     eth.FilterModeDisabled,
			filterPatterns: []string{"^hermes.*"},
			hermesAgent:    "hermes/v1.0.0",
			peerAgent:      "hermes/v2.0.0",
			shouldConnect:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create filter configuration
			filterConfig := eth.FilterConfig{
				Mode:     tt.filterMode,
				Patterns: tt.filterPatterns,
			}

			// Create Hermes host with filter
			hermesHost, err := createHostWithFilter(ctx, tt.hermesAgent, filterConfig, logger)
			require.NoError(t, err)
			defer hermesHost.Close()

			// Create peer host
			peerHost, err := createHost(ctx, tt.peerAgent)
			require.NoError(t, err)
			defer peerHost.Close()

			// Add peer's address to Hermes peerstore
			hermesHost.Peerstore().AddAddrs(peerHost.ID(), peerHost.Addrs(), peerstore.PermanentAddrTTL)

			// Set agent version in peerstore (simulating handshake)
			err = hermesHost.Peerstore().Put(peerHost.ID(), "AgentVersion", tt.peerAgent)
			require.NoError(t, err)

			// Try to connect
			err = hermesHost.Connect(ctx, peer.AddrInfo{
				ID:    peerHost.ID(),
				Addrs: peerHost.Addrs(),
			})

			if tt.shouldConnect {
				assert.NoError(t, err, "Connection should have succeeded")
				
				// Verify connection exists
				conns := hermesHost.Network().ConnsToPeer(peerHost.ID())
				assert.NotEmpty(t, conns, "Should have active connection")
			} else {
				// Connection might fail or succeed initially but should be closed
				// Wait a bit for the filter to kick in
				time.Sleep(100 * time.Millisecond)
				
				// Check that no connection exists
				conns := hermesHost.Network().ConnsToPeer(peerHost.ID())
				assert.Empty(t, conns, "Connection should have been blocked/closed")
			}
		})
	}
}

// createHostWithFilter creates a libp2p host with peer filtering enabled
func createHostWithFilter(ctx context.Context, agent string, filterConfig eth.FilterConfig, logger *slog.Logger) (*hermeshost.Host, error) {
	// Create deferred gater
	gater := eth.NewDeferredGater()

	// Create host config
	hostConfig := &hermeshost.Config{
		DataStream:            &hermeshost.TraceLogger{},
		PeerscoreSnapshotFreq: time.Minute,
	}

	// Create libp2p options
	opts := []libp2p.Option{
		libp2p.UserAgent(agent),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.ConnectionGater(gater),
	}

	// Create Hermes host
	h, err := hermeshost.New(hostConfig, opts...)
	if err != nil {
		return nil, err
	}

	// Create and set peer filter
	if filterConfig.Mode != eth.FilterModeDisabled {
		peerFilter, err := eth.NewPeerFilter(h, filterConfig, logger)
		if err != nil {
			h.Close()
			return nil, err
		}
		gater.SetActual(peerFilter)
	}

	return h, nil
}

// createHost creates a basic libp2p host
func createHost(ctx context.Context, agent string) (host.Host, error) {
	return libp2p.New(
		libp2p.UserAgent(agent),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
}

// TestPeerFilter_SelfPeering tests that Hermes instances cannot peer with each other
func TestPeerFilter_SelfPeering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := slog.Default()

	// Create filter config that blocks hermes peers
	filterConfig := eth.FilterConfig{
		Mode:     eth.FilterModeDenylist,
		Patterns: []string{"^hermes.*"},
	}

	// Create two Hermes hosts
	host1, err := createHostWithFilter(ctx, "hermes/v1.0.0", filterConfig, logger)
	require.NoError(t, err)
	defer host1.Close()

	host2, err := createHostWithFilter(ctx, "hermes/v1.0.1", filterConfig, logger)
	require.NoError(t, err)
	defer host2.Close()

	// Add addresses to peerstores
	host1.Peerstore().AddAddrs(host2.ID(), host2.Addrs(), peerstore.PermanentAddrTTL)
	host2.Peerstore().AddAddrs(host1.ID(), host1.Addrs(), peerstore.PermanentAddrTTL)

	// Set agent versions
	err = host1.Peerstore().Put(host2.ID(), "AgentVersion", "hermes/v1.0.1")
	require.NoError(t, err)
	err = host2.Peerstore().Put(host1.ID(), "AgentVersion", "hermes/v1.0.0")
	require.NoError(t, err)

	// Try to connect from host1 to host2
	err = host1.Connect(ctx, peer.AddrInfo{
		ID:    host2.ID(),
		Addrs: host2.Addrs(),
	})

	// Wait for filter to process
	time.Sleep(200 * time.Millisecond)

	// Verify no connection exists
	conns1to2 := host1.Network().ConnsToPeer(host2.ID())
	assert.Empty(t, conns1to2, "Host1 should not be connected to Host2")

	conns2to1 := host2.Network().ConnsToPeer(host1.ID())
	assert.Empty(t, conns2to1, "Host2 should not be connected to Host1")
}