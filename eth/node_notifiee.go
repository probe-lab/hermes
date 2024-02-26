package eth

import (
	"context"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/probe-lab/hermes/tele"
)

// The Hermes Ethereum [Node] implements the [network.Notifiee] interface.
// This means it will be notified about new connections.
var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	slog.Debug("Connected with peer", tele.LogAttrPeerID(c.RemotePeer()), "total", len(n.host.Network().Peers()), "dir", c.Stat().Direction)

	// handle the new connection by validating the peer. Needs to happen in a
	// go routine because Connected is called synchronously.
	go n.handleNewConnection(c.RemotePeer())
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	if n.prysmAddrInfo != nil && c.RemotePeer() == n.prysmAddrInfo.ID { // TODO: beaconAddrInfo access is racy
		slog.Warn("Beacon node disconnected")
	}

	n.pool.RemovePeer(c.RemotePeer())
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {
	slog.Debug("Listen on", "maddr", maddr.String(), "total", len(n.host.Network().Peers()))
}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

// handleNewConnection validates the newly established connection to the given
// peer.
func (n *Node) handleNewConnection(pid peer.ID) {
	// before we add the peer to our pool, we'll perform a handshake

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	valid := true
	s, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		slog.Debug("Did status handshake", tele.LogAttrError(err))
		valid = false
	} else {
		slog.Info("Performed successful handshake", tele.LogAttrPeerID(pid))
	}
	_ = s

	if !valid {
		// the handshake failed, we disconnect and remove it from our pool
		n.host.Peerstore().RemovePeer(pid)
		n.host.Network().ClosePeer(pid)
		n.pool.RemovePeer(pid)
	} else {
		// handshake succeeded, add this peer to our pool
		n.pool.AddPeer(pid)
	}
}
