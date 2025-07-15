package op

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	elog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/thejerf/suture/v4"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/tele"
)

type DiscoveryConfig struct {
	ChainID uint64
	Addr    string
	UDPPort int
	TCPPort int
	Seeds   []*enode.Node
	Tracer  trace.Tracer
	Meter   metric.Meter
}

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

	opstackENREntry := &OpStackENRData{
		chainID: cfg.ChainID,
		version: 0,
	}

	localNode.Set(enr.IP(ip.String()))
	localNode.Set(enr.UDP(cfg.UDPPort))
	localNode.Set(enr.TCP(cfg.TCPPort))
	localNode.Set(opstackENREntry)
	localNode.SetFallbackIP(ip)
	localNode.SetFallbackUDP(cfg.TCPPort)

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
	defer logDeferErr(conn.Close, "failed to close discovery UDP connection")
	logger := elog.New()
	elog.SetDefault(logger)
	cfg := discover.Config{
		PrivateKey:   d.pk,
		Bootnodes:    d.cfg.Seeds,
		Unhandled:    nil, // Not used in dv5
		Log:          logger,
		ValidSchemes: enode.ValidSchemes,
	}

	listener, err := discover.ListenV5(conn, d.node, cfg)
	if err != nil {
		return fmt.Errorf("failed to start discv5 listener: %w", err)
	}
	defer listener.Close()

	//rec := enr.Record{}
	/////ip4/57.129.36.163/tcp/9222/p2p/16Uiu2HAm5NThaQmNwCziMNj5CX2mduWDRa85hfmawaGTEYbddSsx
	//
	//rec.Set(enr.IP(net.ParseIP("172.16.58.3").String()))
	//rec.Set(enr.UDP(9222))
	//
	//n, err := enode.New(enode.ValidSchemes, &rec)
	//if err != nil {
	//	panic(err)
	//}
	//en, err := listener.RequestENR(n)
	//if err != nil {
	//	panic(err)
	//}

	iterator := enode.Filter(listener.RandomNodes(), func(node *enode.Node) bool {
		var dat OpStackENRData
		if err := node.Load(&dat); err != nil {
			return false
		}
		// check chain ID matches
		if d.cfg.ChainID != dat.chainID {
			return false
		}

		if dat.version != 0 {
			return false
		}

		return true
	})

	go func() {
		<-ctx.Done()
		iterator.Close()
	}()

	slog.Info("Looking for discv5 peers...")
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

		slog.Info("New peer discovered")

		// yes, we do
		node := iterator.Node()

		// Skip peer if it is only privately reachable
		if node.IP().IsPrivate() {
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
		case d.out <- pi.AddrInfo:
		case <-ctx.Done():
			return nil
		}
	}
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

type OpStackENRData struct {
	chainID uint64
	version uint64
}

var _ enr.Entry = (*OpStackENRData)(nil)

func (o *OpStackENRData) ENRKey() string {
	return "opstack"
}

func (o *OpStackENRData) EncodeRLP(w io.Writer) error {
	out := make([]byte, 2*binary.MaxVarintLen64)
	offset := binary.PutUvarint(out, o.chainID)
	offset += binary.PutUvarint(out[offset:], o.version)
	out = out[:offset]
	// encode as byte-string
	return rlp.Encode(w, out)
}

func (o *OpStackENRData) DecodeRLP(s *rlp.Stream) error {
	b, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("failed to decode outer ENR entry: %w", err)
	}
	// We don't check the byte length: the below readers are limited, and the ENR itself has size limits.
	// Future "opstack" entries may contain additional data, and will be tagged with a newer version etc.
	r := bytes.NewReader(b)
	chainID, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("failed to read chain ID var int: %w", err)
	}
	version, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("failed to read version var int: %w", err)
	}
	o.chainID = chainID
	o.version = version
	return nil
}
