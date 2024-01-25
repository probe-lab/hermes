package eth

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/peers/scorers"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	pubsub2 "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/peers"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	"github.com/prysmaticlabs/prysm/v4/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v4/network/forks"
	"github.com/prysmaticlabs/prysm/v4/runtime/version"

	"github.com/plprobelab/hermes/pkg/pubsub"
)

type HostConfig struct {
	Bootstrappers []*enode.Node
	PrivKey       *ecdsa.PrivateKey

	// chain configuration
	Fork     string
	Topics   []string
	Attnets  string
	ChainCfg *params.BeaconChainConfig

	// discv5 config
	Devp2pHost string
	Devp2pPort int
	handshaker *handshake.HandShaker

	// libp2p config
	Libp2pHost string
	Libp2pPort int
	MaxPeers   int
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
	bcCfg   *params.BeaconChainConfig
	ethNode *enode.LocalNode
	discv5  *discover.UDPv5
	psHost  *pubsub.Host
	peers   *peers.Status
	gvr     []byte // genesis validators root
	digest  [4]byte
}

func NewHost(cfg *HostConfig) (*Host, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate host config: %w", err)
	}

	// Globally set the chain configuration in prysm, so that methods like CreateForkDigest work correctly
	params.OverrideBeaconConfig(cfg.ChainCfg)

	// get genesis validators root hash which we'll need to compute fork digests and more
	gvr, err := GenesisValidatorsRootByName(cfg.ChainCfg.ConfigName)
	if err != nil {
		return nil, err
	}

	// compute fork digest
	var digest [4]byte
	if cfg.Fork == "current" {
		genTime := time.Unix(int64(cfg.ChainCfg.MinGenesisTime), 0)
		digest, err = forks.CreateForkDigest(genTime, gvr)
		if err != nil {
			return nil, fmt.Errorf("fork digest for genesis time %s and root %s: %w", genTime, hex.EncodeToString(gvr), err)
		}
	} else {
		forkVersion, err := version.FromString(cfg.Fork)
		if err != nil {
			return nil, fmt.Errorf("unknown fork %s", cfg.Fork)
		}

		var epoch primitives.Epoch
		switch forkVersion {
		case version.Phase0:
			epoch = 0
		case version.Altair:
			epoch = cfg.ChainCfg.AltairForkEpoch
		case version.Bellatrix:
			epoch = cfg.ChainCfg.BellatrixForkEpoch
		case version.Capella:
			epoch = cfg.ChainCfg.CapellaForkEpoch
		case version.Deneb:
			epoch = cfg.ChainCfg.DenebForkEpoch
		default:
			return nil, fmt.Errorf("unsupported fork version %d, %s", forkVersion, cfg.Fork)
		}

		digest, err = forks.ForkDigestFromEpoch(epoch, gvr)
		if err != nil {
			return nil, fmt.Errorf("fork digest from epoch %d and root %s: %w", epoch, hex.EncodeToString(gvr), err)
		}
	}

	ethNode, err := newEthNode(digest, cfg)
	if err != nil {
		return nil, fmt.Errorf("init eth node: %w", err)
	}

	discv5, err := newDiscv5(ethNode, cfg)
	if err != nil {
		return nil, fmt.Errorf("init eth node: %w", err)
	}

	// convert private key to a libp2p private key (ps prefix means pubsub)
	psPrivKey, err := cfg.Secp256k1()
	if err != nil {
		return nil, fmt.Errorf("adapt Secp256k1 from ECDSA: %w", err)
	}

	psConfig := &pubsub.HostConfig{
		UserAgent:  "hermes", // TODO: add version
		Host:       cfg.Libp2pHost,
		Port:       cfg.Libp2pPort,
		PrivateKey: psPrivKey,
	}

	psHost, err := pubsub.NewHost(psConfig)
	if err != nil {
		return nil, fmt.Errorf("new libp2p pubsub host: %w", err)
	}

	// parse the assigned TCP port and add it to our ENR
	for _, maddr := range psHost.Addrs() {
		val, err := maddr.ValueForProtocol(ma.P_TCP)
		if err != nil {
			continue
		}

		tcpPort, err := strconv.Atoi(val)
		if err != nil {
			continue
		}

		ethNode.Set(enr.TCP(tcpPort))
		break
	}

	// TODO: add external IP address to ENR?

	h := &Host{
		cfg:     cfg,
		ethNode: ethNode,
		discv5:  discv5,
		psHost:  psHost,
		gvr:     gvr,
		peers:   nil, // initialized in [Run] because it requires a context
		digest:  digest,
	}

	slog.Debug("Initialized Hermes Host", "enr", ethNode.Node().String())

	return h, nil
}

func newEthNode(digest [4]byte, cfg *HostConfig) (*enode.LocalNode, error) {
	peerstore, err := enode.OpenDB("") // uses in memory db
	if err != nil {
		return nil, fmt.Errorf("open enr database: %v", err)
	}

	ethNode := enode.NewLocalNode(peerstore, cfg.PrivKey)

	eth2Entry, err := NewENREntryEth2(hex.EncodeToString(digest[:]))
	if err != nil {
		return nil, fmt.Errorf("new eth2 enr entry: %w", err)
	}

	attnetsEntry, err := NewENREntryAttnets(cfg.Attnets)
	if err != nil {
		return nil, fmt.Errorf("new attnets enr entry: %w", err)
	}

	ethNode.Set(eth2Entry)
	ethNode.Set(attnetsEntry)

	parsed, err := ParseEnode(ethNode.Node())
	if err != nil {
		return nil, fmt.Errorf("failed parsing own enode: %w", err)
	}

	slog.Info("Initialized local eth node",
		slog.String("id", parsed.PeerID.String()),
		slog.String("forkDigest", fmt.Sprintf("%s (%s)", eth2Entry.ForkDigest.String(), cfg.Fork)),
		slog.String("attnets", attnetsEntry.Attnets),
	)

	return ethNode, err
}

func (h *Host) registerStreamHandlers() error {
	return nil
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

	//peerInfo, err := ParseEnode(node)
	//if err != nil {
	//	return false
	//}

	//if h.psHost.Network().Connectedness(peerInfo.PeerID) == network.Connected {
	//	return false
	//}
	//if !s.peers.IsReadyToDial(peerData.ID) {
	//	return false
	//}
	//nodeENR := node.Record()
	//
	//// Decide whether or not to connect to peer that does not
	//// match the proper fork ENR data with our local node.
	//if s.genesisValidatorsRoot != nil {
	//	if err := s.compareForkENR(nodeENR); err != nil {
	//		log.WithError(err).Trace("Fork ENR mismatches between peer and local node")
	//		return false
	//	}
	//}
	//// Add peer to peer handler.
	//s.peers.Add(nodeENR, peerData.ID, multiAddr, network.DirUnknown)

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

	// needs to be initialized here because the constructor requires a context
	h.peers = peers.NewStatus(ctx, &peers.StatusConfig{
		PeerLimit: h.cfg.MaxPeers,
		ScorerParams: &scorers.Config{
			BadResponsesScorerConfig: &scorers.BadResponsesScorerConfig{
				Threshold:     maxBadResponses,
				DecayInterval: time.Hour,
			},
		},
	})

	topicHandlers := make(map[string]pubsub.MessageHandler, len(h.cfg.Topics))
	for _, topic := range h.cfg.Topics {
		topicHandlers[h.topicID(topic)] = func(message *pubsub2.Message) error {
			return nil
		}
	}

	runOpts := &pubsub.RunConfig{
		PubSubOptions: h.pubSubOptions(),
		TopicHandlers: topicHandlers,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := h.psHost.Run(ctx, runOpts); err != nil {
			slog.Error("Failed running host", slog.String("err", err.Error()))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		h.DiscoverNodes(ctx)
	}()

	wg.Wait()
	return nil
}

func (h *Host) topicID(topic string) string {
	return fmt.Sprintf("/eth2/%x/%s/%s", h.digest, topic, encoder.ProtocolSuffixSSZSnappy)
}

func (h *Host) attnetTopicID(subnet int) string {
	return h.topicID(fmt.Sprintf("%s_%d", p2p.GossipAttestationMessage, subnet))
}
