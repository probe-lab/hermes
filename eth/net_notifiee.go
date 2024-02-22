package eth

import (
	"context"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/probe-lab/hermes/tele"
)

var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	slog.Debug("Connected with peer", tele.LogAttrPeerID(c.RemotePeer()), "total", len(n.host.Network().Peers()))

	// handle the new connection by validating the peer. Needs to happen in a
	// go routine because Connected is called synchronously.
	go n.handleNewConnection(c.RemotePeer())
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	if n.beaconAddrInfo != nil && c.RemotePeer() == n.beaconAddrInfo.ID { // TODO: beaconAddrInfo access is racy
		slog.Warn("Beacon node disconnected")
	}

	n.pool.RemovePeer(c.RemotePeer())
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

// handleNewConnection validates the newly established connection to the given
// peer.
func (n *Node) handleNewConnection(pid peer.ID) {
	// before we add the peer to our pool, we'll perform a handshake

	valid := true
	s, err := n.p2pClient.Status(context.Background(), pid)
	if err != nil {
		valid = false
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
