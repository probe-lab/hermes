package eth

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"log/slog"
	"net"

	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
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

	out chan *DiscoveredPeer

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
	localNode.Set(cfg.enrCustodyEntry())
	localNode.SetFallbackIP(net.ParseIP("127.0.0.1"))
	localNode.SetFallbackUDP(cfg.TCPPort)

	enrEth2Entry, err := cfg.enrEth2Entry()
	if err != nil {
		return nil, fmt.Errorf("build enr fork entry: %w", err)
	}

	localNode.Set(enrEth2Entry)

	d := &Discovery{
		cfg:  cfg,
		pk:   privKey,
		node: localNode,
		out:  make(chan *DiscoveredPeer),
	}
	cfg.Chain.RegisterChainUpgrade(d.forkUpdateSubs)

	slog.Info("Initialized new enode",
		"id", localNode.ID().String(),
		"ip", localNode.Node().IP().String(),
		"tcp", localNode.Node().TCP(),
		"udp", localNode.Node().UDP(),
		"attnets", cfg.Chain.cfg.AttestationSubnetConfig.Subnets,
		"syncnets", cfg.Chain.cfg.SyncSubnetConfig.Subnets,
		"cgc", cfg.Chain.cfg.ColumnSubnetConfig.Count,
		"columns", cfg.Chain.GetColumnCustodySubnets(localNode.ID()),
	)

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

	_, _, _, forkDigest, err := d.cfg.Chain.epochStats()
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
	defer logDeferErr(conn.Close, "failed to close discovery UDP connection")

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

	go func() {
		<-ctx.Done()
		iterator.Close()
	}()

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
			return
		}

		// yes, we do
		node := iterator.Node()

		// Skip peer if it is only privately reachable
		if node.IP().IsPrivate() {
			continue
		}
		sszEncodedForkEntry := make([]byte, 16)
		entry := enr.WithEntry("eth2", &sszEncodedForkEntry)
		if err = node.Record().Load(entry); err != nil {
			// failed reading eth2 enr entry, likely because it doesn't exist
			continue
		}

		forkEntry := &pb.ENRForkID{}
		if err = forkEntry.UnmarshalSSZ(sszEncodedForkEntry); err != nil {
			slog.Debug("failed unmarshalling eth2 enr entry", tele.LogAttrError(err))
			continue
		}

		if !bytes.Equal(forkEntry.CurrentForkDigest, forkDigest[:]) {
			// irrelevant network
			continue
		}

		// construct the data structure that libp2p can make sense of
		pi, err := NewDiscoveredPeer(node)
		if err != nil {
			slog.Warn("Could not convert discovered node to peer info", tele.LogAttrError(err))
			continue
		}

		// Update metrics
		d.MeterDiscoveredPeers.Add(ctx, 1)

		select {
		case d.out <- pi:
		case <-ctx.Done():
			return nil
		}
	}
}

func (d *Discovery) forkUpdateSubs() error {
	// update the enr entry with the new fork
	enrEth2Entry, err := d.cfg.enrEth2Entry()
	if err != nil {
		return fmt.Errorf("build enr fork entry: %w", err)
	}

	d.node.Set(enrEth2Entry)

	slog.Info("Initialized new enode",
		"seq", d.node.Seq(),
		"id", d.node.ID().String(),
		"ip", d.node.Node().IP().String(),
		"tcp", d.node.Node().TCP(),
		"udp", d.node.Node().UDP(),
		"attnets", d.cfg.Chain.cfg.AttestationSubnetConfig.Subnets,
		"syncnets", d.cfg.Chain.cfg.SyncSubnetConfig.Subnets,
		"cgc", len(d.cfg.Chain.cfg.ColumnSubnetConfig.Subnets),
	)
	return nil
}

type DiscoveredPeer struct {
	AddrInfo peer.AddrInfo
	ENR      *enode.Node
}

func NewDiscoveredPeer(node *enode.Node) (*DiscoveredPeer, error) {
	pubKey := node.Pubkey()
	if pubKey == nil {
		return nil, fmt.Errorf("no public key")
	}

	pubBytes := elliptic.Marshal(secp256k1.S256(), pubKey.X, pubKey.Y) //lint:ignore SA1019 couldn't figure out the alternative
	secpKey, err := crypto.UnmarshalSecp256k1PublicKey(pubBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal secp256k1 public key: %w", err)
	}

	peerID, err := peer.IDFromPublicKey(secpKey)
	if err != nil {
		return nil, fmt.Errorf("peer ID from public key: %w", err)
	}

	var ipScheme string
	if p4 := node.IP().To4(); len(p4) == net.IPv4len {
		ipScheme = "ip4"
	} else {
		ipScheme = "ip6"
	}

	maddrs := []ma.Multiaddr{}
	if node.UDP() != 0 {
		maddrStr := fmt.Sprintf("/%s/%s/udp/%d", ipScheme, node.IP(), node.UDP())
		maddr, err := ma.NewMultiaddr(maddrStr)
		if err != nil {
			return nil, fmt.Errorf("parse multiaddress %s: %w", maddrStr, err)
		}
		maddrs = append(maddrs, maddr)
	}

	if node.TCP() != 0 {
		maddrStr := fmt.Sprintf("/%s/%s/tcp/%d", ipScheme, node.IP(), node.TCP())
		maddr, err := ma.NewMultiaddr(maddrStr)
		if err != nil {
			return nil, fmt.Errorf("parse multiaddress %s: %w", maddrStr, err)
		}
		maddrs = append(maddrs, maddr)
	}

	pi := &DiscoveredPeer{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: maddrs,
		},
		ENR: node,
	}

	return pi, nil
}
