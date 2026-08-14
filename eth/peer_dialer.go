package eth

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
)

// PeerDialer is a suture service that reads peers from the peerChan (which
// is filled by the [Discovery] service until that peerChan channel is closed.
// When PeerDialer sees a new peer, it does a few sanity checks and tries
// to establish a connection.
type PeerDialer struct {
	host     *host.Host
	peerChan <-chan *DiscoveredPeer
	maxPeers int
}

var _ suture.Service = (*PeerDialer)(nil)

func (p *PeerDialer) Serve(ctx context.Context) error {
	slog.Debug("Started Peer Dialer Service")
	defer slog.Debug("Stopped Peer Dialer Service")

	for {
		// if we're at capacity, don't look for more peers
		if len(p.host.Network().Peers()) >= p.maxPeers {
			// TODO: add check to see if we need to rotate any of the peers
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				// pass
			}
			continue
		}

		var (
			more    bool
			newPeer *DiscoveredPeer
		)
		select {
		case <-ctx.Done():
			return nil
		case newPeer, more = <-p.peerChan:
			if !more {
				return nil
			}
		}

		// don't connect with ourselves
		if newPeer.AddrInfo.ID == p.host.ID() {
			continue
		}

		// check if we are already connected to the peer
		pConnect := p.host.Network().Connectedness(newPeer.AddrInfo.ID)
		switch pConnect {
		case network.NotConnected:
			// TODO: Good to have:
			// filter to only peers that support attestation networks that we want
			// - if we are not subscribed to subnets, connect any node

			// finally, start the connection establishment.
			// The success case is handled in net_notifiee.go.
			timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := p.host.Connect(timeoutCtx, newPeer.AddrInfo) // failures happen all the time
			cancel()

			// Our own gater vetoing means the peer was never contacted, so counting
			// it would let the backoff escalate from our retries alone.
			if err != nil && !errors.Is(err, swarm.ErrGaterDisallowedConnection) {
				if backoff := p.host.DialBackoff(); backoff != nil {
					backoff.RecordRefusal(newPeer.AddrInfo.ID)
				}
			}

		case network.Connected:
			continue // the peer is already connected
		default:
			continue
		}
	}
}
