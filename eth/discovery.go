package eth

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log/slog"
	"net"

	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/probe-lab/hermes/tele"
	"github.com/thejerf/suture/v4"
	"go.opentelemetry.io/otel/metric"
)

// Discovery is a suture service that periodically queries the discv5 DHT
// for random peers and publishes the discovered peers on the `out` channel.
// Users of this Discovery service are required to read from the channel.
// Otherwise, the discovery will block forever.
type Discovery struct {
	cfg  *DiscoveryConfig
	pk   *ecdsa.PrivateKey
	node *enode.LocalNode
	out  chan peer.AddrInfo

	// Metrics
	MeterDiscoveredPeers metric.Int64Counter
}

var _ suture.Service = (*Discovery)(nil)

func NewDiscovery(privKey *ecdsa.PrivateKey, cfg *DiscoveryConfig) (*Discovery, error) {
	slog.Info("Initialize Discovery service")

	db, err := enode.OpenDB("") // in memory db
	if err != nil {
		return nil, fmt.Errorf("could not open node's peer database: %w", err)
	}

	ip := net.ParseIP(cfg.Addr)

	localNode := enode.NewLocalNode(db, privKey)

	localNode.Set(enr.IP(ip.String()))
	localNode.Set(enr.UDP(cfg.UDPPort))
	localNode.Set(enr.TCP(cfg.TCPPort))
	localNode.Set(cfg.enrAttnetsEntry())
	localNode.Set(cfg.enrSyncnetsEntry())
	localNode.SetFallbackIP(ip)
	localNode.SetFallbackUDP(cfg.TCPPort)

	enrEth2Entry, err := cfg.enrEth2Entry()
	if err != nil {
		return nil, fmt.Errorf("build enr fork entry: %w", err)
	}

	localNode.Set(enrEth2Entry)

	slog.Info("Initialized new enode",
		"id", localNode.ID().String(),
		"ip", localNode.Node().IP().String(),
		"tcp", localNode.Node().TCP(),
		"udp", localNode.Node().UDP(),
	)

	d := &Discovery{
		cfg:  cfg,
		pk:   privKey,
		node: localNode,
		out:  make(chan peer.AddrInfo),
	}

	d.MeterDiscoveredPeers, err = cfg.Meter.Int64Counter("discovered_peers", metric.WithDescription("Total number of discovered peers"))
	if err != nil {
		return nil, fmt.Errorf("discovered_peers counter: %w", err)
	}

	return d, nil
}

func (d *Discovery) Serve(ctx context.Context) (err error) {
	slog.Info("Starting discv5 Discovery Service")
	defer slog.Info("Stopped disv5 Discovery Service")
	defer func() { err = terminateSupervisorTreeOnErr(err) }()

	ip := net.ParseIP(d.cfg.Addr)

	var bindIP net.IP
	var networkVersion string
	switch {
	case ip == nil:
		return fmt.Errorf("invalid IP address provided: %s", d.cfg.Addr)
	case ip.To4() != nil:
		bindIP = net.IPv4zero
		networkVersion = "udp4"
	case ip.To16() != nil:
		bindIP = net.IPv6zero
		networkVersion = "udp6"
	default:
		return fmt.Errorf("invalid IP address provided: %s", d.cfg.Addr)
	}

	udpAddr := &net.UDPAddr{
		IP:   bindIP,
		Port: d.cfg.UDPPort,
	}

	conn, err := net.ListenUDP(networkVersion, udpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s:%d: %w", bindIP, d.cfg.UDPPort, err)
	}
	defer logDeferErr(conn.Close, "Failed to close discovery UDP connection")

	enodes, err := d.cfg.BootstrapNodes()
	if err != nil {
		return err
	}

	cfg := discover.Config{
		PrivateKey: d.pk,
		Bootnodes:  enodes,
	}

	listener, err := discover.ListenV5(conn, d.node, cfg)
	if err != nil {
		return fmt.Errorf("failed to start discv5 listener: %w", err)
	}
	defer listener.Close()

	iterator := listener.RandomNodes()

	slog.Info("Listen for discv5 peers...")
	defer iterator.Close()
	defer close(d.out)
	for {

		// check if the context is already cancelled
		select {
		case <-ctx.Done():
			return nil
		default:
			// pass
		}

		// let's see if we have another peer to process
		exists := iterator.Next()
		if !exists {
			continue
		}

		// yes, we do
		node := iterator.Node()

		// Skip peer if it is only privately reachable
		if node.IP().IsPrivate() {
			continue
		}

		// construct the data structure that libp2p can make sense of
		addrInfo, err := EnodeToAddrInfo(node)
		if err != nil {
			slog.Warn("Could not convert discovered peer info to addr info", tele.LogAttrError(err))
			continue
		}

		// Update metrics
		d.MeterDiscoveredPeers.Add(ctx, 1)

		slog.Debug("Discovered peer", tele.LogAttrPeerID(addrInfo.ID))
		select {
		case d.out <- *addrInfo:
		case <-ctx.Done():
			return nil
		}
	}
}
