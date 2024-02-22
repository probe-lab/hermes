package eth

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type Node struct {
	cfg    *NodeConfig
	host   *host.Host
	peerer TrustedPeerer
}

func NewNode(cfg *NodeConfig) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("node config validation failed: %w", err)
	}

	// initialize libp2p host
	opts, err := cfg.libp2pOptions()
	if err != nil {
		return nil, fmt.Errorf("build libp2p options: %w", err)
	}

	h, err := host.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("new libp2p host: %w", err)
	}
	slog.Info("Initialized new libp2p Host", tele.LogAttrPeerID(h.ID()))

	// initialize peerer that we'll use to register this Hermes node as a
	// trusted peer with the beacon client that we delegate requests to.
	var peerer TrustedPeerer
	if cfg.BeaconType == BeaconTypePrysm {
		addr, port, err := cfg.BeaconHostPort()
		if err != nil {
			return nil, fmt.Errorf("extract prysm api host and port: %w", err)
		}

		slog.Info("Init Prysm JSON RPC client", "addr", addr, "port", port)
		client := NewPrysmClient(addr, port)
		client.tracer = cfg.Tracer
		peerer = client
	} else {
		slog.Info("Using no-op peerer")
		peerer = NoopTrustedPeerer{}
	}

	// finally, initialize hermes node
	n := &Node{
		cfg:    cfg,
		host:   h,
		peerer: peerer,
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	self := peer.AddrInfo{
		ID:    n.host.ID(),
		Addrs: n.host.Addrs(),
	}

	slog.Info("Adding ourself as a trusted peer to Prysm", tele.LogAttrPeerID(self.ID), "addr", self.Addrs[0].String())
	if err := n.peerer.AddTrustedPeer(ctx, self); err != nil {
		return fmt.Errorf("failed adding ourself as trusted peer: %w", err)
	}
	defer func() {
		slog.Info("Removing ourself as a trusted peer from Prysm", tele.LogAttrPeerID(self.ID))
		if err := n.peerer.RemoveTrustedPeer(ctx, n.host.ID()); err != nil {
			slog.Warn("failed to remove ourself as a trusted peer", tele.LogAttrError(err))
		}
	}()

	slog.Info("Done!")

	//sub, err := n.host.EventBus().Subscribe(new(event.EvtLocalAddressesUpdated))
	//if err != nil {
	//	return fmt.Errorf("failed to subscribe to EvtLocalAddressesUpdated events: %w", err)
	//}
	//
	//for {
	//	select {
	//	case <-ctx.Done():
	//		return ctx.Err()
	//	case evt := <-sub.Out():
	//		fmt.Println(evt)
	//	}
	//}
	return nil
}

func (n *Node) Shutdown(ctx context.Context) error {
	return nil
}

func buildDiscoveryNode(cfg *NodeConfig) (*enode.LocalNode, *net.UDPConn, error) {
	ip := net.ParseIP(cfg.Devp2pAddr)

	var bindIP net.IP
	var networkVersion string
	switch {
	case ip == nil:
		return nil, nil, fmt.Errorf("invalid IP address provided: %s", cfg.Devp2pAddr)
	case ip.To4() != nil:
		bindIP = net.IPv4zero
		networkVersion = "udp4"
	case ip.To16() != nil:
		bindIP = net.IPv6zero
		networkVersion = "udp6"
	default:
		return nil, nil, fmt.Errorf("invalid IP address provided: %s", cfg.Devp2pAddr)
	}

	udpAddr := &net.UDPAddr{
		IP:   bindIP,
		Port: cfg.Devp2pPort,
	}

	conn, err := net.ListenUDP(networkVersion, udpAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on %s:%d: %w", bindIP, cfg.Devp2pPort, err)
	}

	db, err := enode.OpenDB("") // in memory db
	if err != nil {
		return nil, nil, fmt.Errorf("could not open node's peer database: %w", err)
	}

	privKey, err := cfg.ECDSAPrivateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("get ecdsa private key: %w", err)
	}
	localNode := enode.NewLocalNode(db, privKey)

	localNode.Set(enr.IP(cfg.Devp2pAddr))
	localNode.Set(enr.UDP(cfg.Devp2pPort))
	localNode.Set(enr.TCP(cfg.Libp2pPort))
	localNode.Set(cfg.enrAttnetsEntry())
	localNode.Set(cfg.enrSyncnetsEntry())
	localNode.SetFallbackIP(ip)
	localNode.SetFallbackUDP(cfg.Libp2pPort)

	enrEth2Entry, err := cfg.enrEth2Entry()
	if err != nil {
		return nil, nil, fmt.Errorf("build enr fork entry: %w", err)
	}

	localNode.Set(enrEth2Entry)

	return localNode, conn, nil
}
