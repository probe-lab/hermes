package eth

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/probe-lab/hermes/host"
	"github.com/thejerf/suture/v4"
)

// PeerDialer is a suture service that reads peers from the peerChan (which
// is filled by the [Discovery] service until that peerChan channel is closed.
// When PeerDialer sees a new peer, it does a few sanity checks and tries
// to establish a connection.
type PeerDialer struct {
	host     *host.Host
	pool     *Pool
	peerChan <-chan peer.AddrInfo
	maxPeers int
}

var _ suture.Service = (*PeerDialer)(nil)

func (p *PeerDialer) Serve(ctx context.Context) error {
	slog.Debug("Started Peer Consumer Service")
	defer slog.Debug("Stopped Peer Consumer Service")

	for {
		// if we're at capacity, don't look for more peers
		if len(p.host.Network().Peers()) >= p.maxPeers {
			time.Sleep(time.Second)
			continue
		}

		// channel will be closed when context is cancelled, so no
		// select on the context necessary here
		addrInfo, more := <-p.peerChan
		if !more {
			return nil
		}

		// don't connect with ourselves
		if addrInfo.ID == p.host.ID() {
			continue
		}

		// skip peer if we banned it previously
		if p.pool.BanStatus(addrInfo.ID) {
			continue
		}

		// finally, start the connection establishment.
		// The success case is handled in net_notifiee.go.
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = p.host.Connect(timeoutCtx, addrInfo) // ignore error, this happens all the time
		cancel()
	}
}
