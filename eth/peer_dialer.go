package eth

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
)

// PeerDialer is a suture service that reads peers from the peerChan (which
// is filled by the [Discovery] service until that peerChan channel is closed.
// When PeerDialer sees a new peer, it does a few sanity checks and tries
// to establish a connection.
type PeerDialer struct {
	host     *host.Host
	peerChan <-chan peer.AddrInfo
	maxPeers int
}

var _ suture.Service = (*PeerDialer)(nil)

func (p *PeerDialer) Serve(ctx context.Context) error {
	slog.Debug("Started Peer Dialer Service")
	defer slog.Debug("Stopped Peer Dialer Service")

	for {
		// if we're at capacity, don't look for more peers
		if len(p.host.Network().Peers()) >= p.maxPeers {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				// pass
			}
			continue
		}

		var (
			more     bool
			addrInfo peer.AddrInfo
		)
		select {
		case <-ctx.Done():
			return nil
		case addrInfo, more = <-p.peerChan:
			if !more {
				return nil
			}
		}

		// don't connect with ourselves
		if addrInfo.ID == p.host.ID() {
			continue
		}

		// finally, start the connection establishment.
		// The success case is handled in net_notifiee.go.
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = p.host.Connect(timeoutCtx, addrInfo) // ignore error, this happens all the time
		cancel()
	}
}
