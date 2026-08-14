package host

import (
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	libp2ptest "github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePeerAddrInfos(t *testing.T) {
	first := libp2ptest.RandPeerIDFatal(t)
	second := libp2ptest.RandPeerIDFatal(t)
	firstAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/9000/p2p/%s", first)
	secondAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/9001/p2p/%s", second)

	tests := []struct {
		name      string
		addrs     []string
		wantIDs   []string
		wantError string
	}{
		{
			name:    "empty",
			addrs:   []string{},
			wantIDs: []string{},
		},
		{
			name:    "nil",
			wantIDs: []string{},
		},
		{
			name:    "single peer",
			addrs:   []string{firstAddr},
			wantIDs: []string{first.String()},
		},
		{
			name:    "multiple peers preserve order",
			addrs:   []string{secondAddr, firstAddr},
			wantIDs: []string{second.String(), first.String()},
		},
		{
			name:      "malformed multiaddress",
			addrs:     []string{"not-a-multiaddress"},
			wantError: "not-a-multiaddress",
		},
		{
			name:      "malformed address fails fast without partial results",
			addrs:     []string{firstAddr, "not-a-multiaddress", secondAddr},
			wantError: "not-a-multiaddress",
		},
		{
			name:      "missing peer ID",
			addrs:     []string{firstAddr, "/ip4/127.0.0.1/tcp/9000"},
			wantError: "/ip4/127.0.0.1/tcp/9000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePeerAddrInfos(tt.addrs)
			if tt.wantError != "" {
				require.Error(t, err)
				assert.Nil(t, got)
				assert.ErrorContains(t, err, tt.wantError)
				return
			}

			require.NoError(t, err)
			require.Len(t, got, len(tt.wantIDs))
			for i, wantID := range tt.wantIDs {
				assert.Equal(t, wantID, got[i].ID.String())
			}
		})
	}
}

func TestNewPubsubBlacklist(t *testing.T) {
	first := peer.ID("configured-peer-1")
	second := peer.ID("configured-peer-2")
	unlisted := peer.ID("unlisted-peer")

	blacklist := NewPubsubBlacklist([]peer.AddrInfo{{ID: first}, {ID: second}})

	assert.True(t, blacklist.Contains(first))
	assert.True(t, blacklist.Contains(second))
	assert.False(t, blacklist.Contains(unlisted))
}
