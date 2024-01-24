package eth

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	gethlog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"

	"github.com/plprobelab/hermes/pkg/pubsub"
)

type HostConfig struct {
	Bootstrappers []*enode.Node
	ForkDigest    ForkDigest
	PrivKey       *ecdsa.PrivateKey
	Attnets       string

	// discv5 config
	Devp2pHost string
	Devp2pPort int

	// libp2p config
	Libp2pHost string
	Libp2pPort int
}

func (c *HostConfig) Validate() error {
	if len(c.Bootstrappers) == 0 {
		return fmt.Errorf("no bootstrappers provided")
	}

	// ForkDigest uses type check, so no need to validate (technically yes, but that's fine)

	if c.PrivKey == nil {
		return fmt.Errorf("private key is nil but required")
	}

	if c.Attnets == "" {
		return fmt.Errorf("attnets is empty but required")
	}

	return nil
}

func (c *HostConfig) Secp256k1() (*crypto.Secp256k1PrivateKey, error) {
	privBytes := gcrypto.FromECDSA(c.PrivKey)
	if len(privBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("expected secp256k1 data size to be %d", secp256k1.PrivKeyBytesLen)
	}
	return (*crypto.Secp256k1PrivateKey)(secp256k1.PrivKeyFromBytes(privBytes)), nil
}

type Host struct {
	cfg     *HostConfig
	ethNode *enode.LocalNode
	genesis time.Time
	discv5  *discover.UDPv5
	psHost  *pubsub.Host
}

func NewHost(cfg *HostConfig) (*Host, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate host config: %w", err)
	}

	ethNode, err := initEthNode(cfg)
	if err != nil {
		return nil, fmt.Errorf("init eth node: %w", err)
	}

	discv5, err := initDiscv5(ethNode, cfg)
	if err != nil {
		return nil, fmt.Errorf("init eth node: %w", err)
	}

	// convert private key to a libp2p private key (ps prefix = pubsub)
	psPrivKey, err := cfg.Secp256k1()
	if err != nil {
		return nil, fmt.Errorf("adapt Secp256k1 from ECDSA: %w", err)
	}

	psConfig := &pubsub.Config{
		UserAgent:      "hermes", // TODO: add version
		Host:           cfg.Libp2pHost,
		Port:           cfg.Libp2pPort,
		PrivateKey:     psPrivKey,
		BootstrapPeers: nil,
	}

	psHost, err := pubsub.NewHost(psConfig)
	if err != nil {
		return nil, fmt.Errorf("new libp2p pubsub host: %w", err)
	}

	genesis, err := GenesisForForkDigest(cfg.ForkDigest)
	if err != nil {
		return nil, err
	}

	h := &Host{
		cfg:     cfg,
		ethNode: ethNode,
		discv5:  discv5,
		psHost:  psHost,
		genesis: genesis,
	}

	return h, nil
}

func initEthNode(cfg *HostConfig) (*enode.LocalNode, error) {
	peerstore, err := enode.OpenDB("") // uses in memory db
	if err != nil {
		return nil, fmt.Errorf("open enr database: %v", err)
	}

	ethNode := enode.NewLocalNode(peerstore, cfg.PrivKey)

	eth2Entry, err := NewENREntryEth2(string(cfg.ForkDigest))
	if err != nil {
		return nil, fmt.Errorf("new eth2 enr entry: %w", err)
	}

	attnetsEntry, err := NewENREntryAttnets(cfg.Attnets)
	if err != nil {
		return nil, fmt.Errorf("new attnets enr entry: %w", err)
	}

	ethNode.Set(eth2Entry)
	ethNode.Set(attnetsEntry)

	slog.Info("Initialized local eth node",
		slog.String("id", ethNode.ID().String()),
		slog.String("forkDigest", eth2Entry.ForkDigest.String()),
		slog.String("attnets", attnetsEntry.Attnets),
	)

	return ethNode, err
}

func initDiscv5(ethNode *enode.LocalNode, cfg *HostConfig) (*discover.UDPv5, error) {
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: cfg.Devp2pPort,
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen udp: %w", err)
	}

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

	slog.Info("Initialized local discv5 listener", slog.String("addr", conn.LocalAddr().String()))

	return d5Listener, err
}

// Taken from: https://github.com/prysmaticlabs/prysm/blob/75a28310c25123393c5b2a9dae9c57380dd30570/beacon-chain/p2p/discovery.go#L273
func (h *Host) filterPeer(node *enode.Node) bool {
	// Ignore nil node entries passed in.
	if node == nil {
		return false
	}

	// ignore nodes with no ip address stored.
	if node.IP() == nil {
		return false
	}

	// do not dial nodes with their tcp ports not set
	if err := node.Record().Load(enr.WithEntry("tcp", new(enr.TCP))); err != nil {
		return false
	}

	peerInfo, err := ParseEnode(node)
	if err != nil {
		return false
	}

	if h.psHost.Network().Connectedness(peerInfo.PeerID) == network.Connected {
		return false
	}
	if !s.peers.IsReadyToDial(peerData.ID) {
		return false
	}
	nodeENR := node.Record()

	// Decide whether or not to connect to peer that does not
	// match the proper fork ENR data with our local node.
	if s.genesisValidatorsRoot != nil {
		if err := s.compareForkENR(nodeENR); err != nil {
			log.WithError(err).Trace("Fork ENR mismatches between peer and local node")
			return false
		}
	}
	// Add peer to peer handler.
	s.peers.Add(nodeENR, peerData.ID, multiAddr, network.DirUnknown)

	return true
}

func (h *Host) Close() error {
	slog.Debug("Closing pubsub host...")

	h.discv5.Close()             // no err
	h.ethNode.Database().Close() // no err

	return h.psHost.Close()
}

func (h *Host) Run(ctx context.Context) error {
	slog.Debug("Starting hermes host...")
	h.discv5.RandomNodes()
	iterator := enode.Filter(h.discv5.RandomNodes(), h.filterPeer)
	defer iterator.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}
