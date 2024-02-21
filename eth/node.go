package eth

import (
	"context"
	"fmt"
	"net"

	"github.com/libp2p/go-libp2p/core/event"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"

	"github.com/probe-lab/hermes/host"
)

type Node struct {
	cfg *NodeConfig
	h   *host.Host
}

func NewNode(cfg *NodeConfig) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("node config validation failed: %w", err)
	}

	opts, err := cfg.libp2pOptions()
	if err != nil {
		return nil, fmt.Errorf("build libp2p options: %w", err)
	}

	h, err := host.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("new libp2p host: %w", err)
	}

	n := &Node{
		cfg: cfg,
		h:   h,
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	sub, err := n.h.EventBus().Subscribe(new(event.EvtLocalAddressesUpdated))
	if err != nil {
		return fmt.Errorf("failed to subscribe to EvtLocalAddressesUpdated events: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt := <-sub.Out():
			fmt.Println(evt)
		}
	}
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
