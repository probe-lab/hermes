package fil

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/probe-lab/hermes/tele"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	peerstoreKeyConnectedAt = "connected_at"
)

// The Hermes Ethereum [Node] implements the [network.Notifiee] interface.
// This means it will be notified about new connections.
var _ network.Notifiee = (*Node)(nil)

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) Connected(net network.Network, c network.Conn) {
	slog.Debug("Connected with peer", tele.LogAttrPeerID(c.RemotePeer()), "total", len(n.host.Network().Peers()), "dir", c.Stat().Direction)

	if err := n.host.Peerstore().Put(c.RemotePeer(), peerstoreKeyConnectedAt, time.Now()); err != nil {
		slog.Warn("Failed to store connection timestamp in peerstore", tele.LogAttrError(err))
	}
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	if !c.Stat().Opened.IsZero() {
		av := n.host.AgentVersion(c.RemotePeer())
		parts := strings.Split(av, "/")
		if len(parts) > 0 {
			switch strings.ToLower(parts[0]) {
			case "prysm", "lighthouse", "nimbus", "lodestar", "grandine", "teku", "erigon":
				av = strings.ToLower(parts[0])
			default:
				av = "other"
			}
		} else {
			av = "unknown"
		}
		n.connAge.Record(context.TODO(), time.Since(c.Stat().Opened).Seconds(), metric.WithAttributes(attribute.String("agent", av)))
	}

	ps := n.host.Peerstore()
	if val, err := ps.Get(c.RemotePeer(), peerstoreKeyConnectedAt); err == nil {
		n.connDurHist.Record(context.Background(), time.Since(val.(time.Time)).Hours())
	}
}
