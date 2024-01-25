package eth

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	gethlog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/peer"
)

func newDiscv5(ethNode *enode.LocalNode, cfg *HostConfig) (*discover.UDPv5, error) {
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: cfg.Devp2pPort,
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen udp: %w", err)
	}

	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil, fmt.Errorf("local address is not a UDP one")
	}

	ethNode.Set(enr.UDP(localAddr.Port))

	devnull := gethlog.New()
	devnull.SetHandler(gethlog.FuncHandler(func(r *gethlog.Record) error {
		return nil
	}))

	discv5Config := discover.Config{
		PrivateKey:   cfg.PrivKey,
		NetRestrict:  nil,
		Bootnodes:    cfg.Bootstrappers,
		Unhandled:    nil, // Not used in dv5
		Log:          devnull,
		ValidSchemes: enode.ValidSchemes,
	}

	d5Listener, err := discover.ListenV5(conn, ethNode, discv5Config)
	if err != nil {
		return nil, fmt.Errorf("listen v5: %w", err)
	}

	slog.Info("Initialized local discv5 listener",
		slog.String("addr", localAddr.String()),
	)

	return d5Listener, err
}

func (h *Host) DiscoverNodes(ctx context.Context) {
	iterator := h.discv5.RandomNodes()
	iterator = enode.Filter(iterator, h.filterPeer)
	defer iterator.Close()

	for {
		if ctx.Err() != nil {
			return
		}

		if h.isPeerAtLimit(false) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}

		exists := iterator.Next()
		if !exists {
			break
		}

		info, err := ParseEnode(iterator.Node())
		if err != nil {
			slog.Debug("Failed parsing discovered enode", slog.String("err", err.Error()))
			continue
		}

		if info.PeerID == h.psHost.ID() {
			slog.Debug("Skipping connection to own peer")
			continue
		}

		if h.peers.IsBad(info.PeerID) {
			slog.Debug("Skipping connection to peer as it's considered bad")
			continue
		}

		// TODO: add dial backoff - check prysm listenForNewNodes
		go func(addrInfo peer.AddrInfo) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := h.psHost.Connect(ctx, addrInfo); err != nil {
				h.peers.Scorers().BadResponsesScorer().Increment(addrInfo.ID)
			} else {
				slog.Debug("Established new connection",
					slog.String("peerID", addrInfo.ID.String()),
				)
			}
		}(info.AddrInfo())
	}
}

// This checks our set max peers in our config, and
// determines whether our currently connected and
// active peers are above our set max peer limit.
func (h *Host) isPeerAtLimit(inbound bool) bool {
	numOfConns := len(h.psHost.Network().Peers())
	maxPeers := h.cfg.MaxPeers
	// If we are measuring the limit for inbound peers
	// we apply the high watermark buffer.
	if inbound {
		maxPeers += highWatermarkBuffer
		maxInbound := h.peers.InboundLimit() + highWatermarkBuffer
		currInbound := len(h.peers.InboundConnected())
		// Exit early if we are at the inbound limit.
		if currInbound >= maxInbound {
			return true
		}
	}

	activePeers := len(h.peers.Active())
	return activePeers >= maxPeers || numOfConns >= maxPeers
}
