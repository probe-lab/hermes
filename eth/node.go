package eth

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"net"
	"strings"
	"time"

	gethlog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/plprobelab/hermes/eth/rpc/methods"
	"github.com/plprobelab/hermes/eth/rpc/reqresp"
	"github.com/protolambda/zrnt/eth2/beacon/common"
)

const (
	RPCTimeout time.Duration = 20 * time.Second
)

type Listener struct {
	cfg ListenerConfig
	// ethereum node
	ethNode *enode.LocalNode
	// node's metadata in the network
	LocalStatus   common.Status
	LocalMetadata common.MetaData
	// Network Details
	networkGenesis time.Time

	privKey    *ecdsa.PrivateKey
	listenPort int
	bootnodes  []*enode.Node

	d5Listener *discover.UDPv5
}

type ListenerConfig struct {
	ForkDigest  string
	AttNetworks string
	MaxPeers    int
}

func (c *ListenerConfig) Validate() error {
	if c.ForkDigest == "" {
		return fmt.Errorf("ForkDigest cannot be empty")
	}
	if c.AttNetworks == "" {
		return fmt.Errorf("AttNetworks cannot be empty")
	}
	if c.MaxPeers < 1 {
		return fmt.Errorf("MaxPeers must be greater than zero")
	}
	return nil
}

func DefaultListenerConfig() *ListenerConfig {
	return &ListenerConfig{
		ForkDigest:  ForkDigests[CapellaKey],
		AttNetworks: "ffffffffffffffff",
		MaxPeers:    20,
	}
}

func NewListener(privKey *ecdsa.PrivateKey, listenPort int, status common.Status, metadata common.MetaData, bootnodes []*enode.Node, cfg *ListenerConfig) (*Listener, error) {
	if cfg == nil {
		cfg = DefaultListenerConfig()
	} else if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("listener config: %w", err)
	}

	if len(bootnodes) == 0 {
		return nil, fmt.Errorf("no bootnodes provided")
	}

	// db to store the ENRs
	ethDB, err := enode.OpenDB("")
	if err != nil {
		return nil, fmt.Errorf("open enr database: %v", err)
	}

	// select network based on the network that we are participating in
	var genesis time.Time
	switch cfg.ForkDigest {
	// Mainnet
	case ForkDigests[Phase0Key], ForkDigests[AltairKey], ForkDigests[BellatrixKey]:
		genesis = MainnetGenesis
	// Prater
	case ForkDigests[PraterPhase0Key], ForkDigests[PraterBellatrixKey]:
		genesis = GoerliGenesis
	// Gnosis
	case ForkDigests[GnosisPhase0Key], ForkDigests[GnosisBellatrixKey]:
		genesis = GnosisGenesis
	// Mainnet
	default:
		genesis = MainnetGenesis
	}

	en := enode.NewLocalNode(ethDB, privKey)
	en.Set(NewEth2DataEntry(strings.Trim(cfg.ForkDigest, "0x")))
	en.Set(NewAttnetsENREntry(cfg.AttNetworks))

	return &Listener{
		cfg:            *cfg,
		ethNode:        en,
		networkGenesis: genesis,
		listenPort:     listenPort,
		privKey:        privKey,
		bootnodes:      bootnodes,
	}, nil
}

func (l *Listener) GetNetworkGenesis() time.Time {
	return l.networkGenesis
}

func (l *Listener) UpdateStatus(newStatus common.Status) {
	// check if the new one is newer than ours
	if newStatus.HeadSlot > l.LocalStatus.HeadSlot {
		l.LocalStatus = newStatus
	}
}

func (l *Listener) EthNode() *enode.LocalNode {
	return l.ethNode
}

func ComposeQuickBeaconStatus(forkDigest string) common.Status {
	frkDgst := new(common.ForkDigest)
	frkDgst.UnmarshalText([]byte(forkDigest))

	return common.Status{
		ForkDigest:     *frkDgst,
		FinalizedRoot:  common.Root{},
		FinalizedEpoch: 0,
		HeadRoot:       common.Root{},
		HeadSlot:       0,
	}
}

func ComposeQuickBeaconMetaData() common.MetaData {
	attnets := new(common.AttnetBits)
	b, err := hex.DecodeString("ffffffffffffffff")
	if err != nil {
		log.Panic("unable to decode Attnets", err.Error())
	}
	attnets.UnmarshalText(b)

	return common.MetaData{
		SeqNumber: common.SeqNr(1),
		Attnets:   *attnets,
	}
}

func (l *Listener) Listen(ctx context.Context, h host.Host) error {
	l.StartDiscovery(ctx, h)
	l.ServeBeaconPing(ctx, h)
	l.ServeBeaconStatus(ctx, h)
	l.ServeBeaconMetadata(ctx, h)

	return nil
}

func (l *Listener) Close() error {
	l.d5Listener.Close()

	return nil
}

func (l *Listener) StartDiscovery(ctx context.Context, h host.Host) error {
	slog.Debug("starting discovery")
	// udp address to listen
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: l.listenPort,
	}

	// start listening and create a connection object
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}

	// Set custom logger for the discovery5 service (Debug)
	gethLogger := gethlog.New()
	gethLogger.SetHandler(gethlog.FuncHandler(func(r *gethlog.Record) error {
		return nil
	}))

	// configuration of the discovery5
	cfg := discover.Config{
		PrivateKey:   l.privKey,
		NetRestrict:  nil,
		Bootnodes:    l.bootnodes,
		Unhandled:    nil, // Not used in dv5
		Log:          gethLogger,
		ValidSchemes: enode.ValidSchemes,
	}

	// start the discovery5 service and listen using the given connection
	d5Listener, err := discover.ListenV5(conn, l.ethNode, cfg)
	if err != nil {
		return fmt.Errorf("listen v5: %w", err)
	}

	l.d5Listener = d5Listener

	go l.listenForNewNodes(ctx, h)

	return nil
}

// listenForNewNodes watches for new nodes in the network and adds them to the peerstore.
func (l *Listener) listenForNewNodes(ctx context.Context, h host.Host) {
	const pollInterval = 10 * time.Second

	iterator := l.d5Listener.RandomNodes()
	iterator = enode.Filter(iterator, l.makePeerFilter(h))
	defer iterator.Close()
	for {
		select {
		case <-ctx.Done():
			slog.Debug("shutdown detected, closing discv5 node iterator")
			return
		default:
		}

		numOfConns := len(h.Network().Peers())
		if numOfConns >= l.cfg.MaxPeers {
			slog.Debug("pausing new node discovery since maximum number of connections reached", "conns", numOfConns)
			time.Sleep(pollInterval)
			continue
		}

		slog.Debug(fmt.Sprintf("attempting to connect with up to %d peers", l.cfg.MaxPeers-numOfConns))
		for i := numOfConns; i < l.cfg.MaxPeers; i++ {
			exists := iterator.Next()
			if !exists {
				break
			}
			node := iterator.Node()
			logger := slog.With("id", node.ID().TerminalString())
			ai, _, err := ConvertToAddrInfo(node)
			if err != nil {
				logger.Error("failed to convert to address info", "error", err)
				continue
			}
			go func(info *peer.AddrInfo) {
				// Make sure that peer is not dialed too often
				advanceNextDialTime(h.Peerstore(), ai.ID)
				if err := l.connectWithPeer(ctx, h, *info, node); err != nil {
					logger.Debug("failed to connect with peer", "info", info.String(), "error", err)
				}
			}(ai)
		}
		time.Sleep(pollInterval)
	}
}

// filterPeer validates each node that we retrieve from our dht. We
// try to ascertain that the peer can be a valid protocol peer.
// Validity Conditions:
//  1. The local node is still actively looking for peers to
//     connect to.
//  2. Peer has a valid IP and TCP port set in their enr.
//  3. Peer hasn't been marked as 'bad'
//  4. Peer is not currently active or connected.
//  5. Peer is ready to receive incoming connections.
//  6. Peer's fork digest in their ENR matches that of
//     our localnodes.
func (l *Listener) makePeerFilter(h host.Host) func(*enode.Node) bool {
	return func(node *enode.Node) bool {
		// Ignore nil node entries passed in.
		if node == nil {
			return false
		}
		logger := slog.With("id", node.ID().TerminalString())
		// ignore nodes with no ip address stored.
		if node.IP() == nil {
			logger.Debug("rejecting node: no ip address")
			return false
		}
		// do not dial nodes with their tcp ports not set
		if err := node.Record().Load(enr.WithEntry("tcp", new(enr.TCP))); err != nil {
			if !enr.IsNotFound(err) {
				slog.Debug("rejecting node: could not retrieve tcp port", "error", err)
			}
			logger.Debug("rejecting node: no tcp port")
			return false
		}
		ai, _, err := ConvertToAddrInfo(node)
		if err != nil {
			slog.Debug("rejecting node: could not convert to address info", "error", err)
			return false
		}
		// if en.peers.IsBad(peerData.ID) {
		// logger.Debug("rejecting node: marked as bad")
		// 	return false
		// }
		// if en.peers.IsActive(peerData.ID) {
		// logger.Debug("rejecting node: marked as active")
		// 	return false
		// }
		if h.Network().Connectedness(ai.ID) == network.Connected {
			logger.Debug("rejecting node: not connected")
			return false
		}
		if !isReadyToDial(h.Peerstore(), ai.ID) {
			logger.Debug("rejecting node: not ready to dial")
			return false
		}
		// nodeENR := node.Record()
		// Decide whether or not to connect to peer that does not
		// match the proper fork ENR data with our local node.
		// if en.genesisValidatorsRoot != nil {
		// 	if err := en.compareForkENR(nodeENR); err != nil {
		// logger.Debug("rejecting node: fork enr mismatch", "error", err)
		// 		slog.Debug("Fork ENR mismatches between peer and local node", "error", err)
		// 		return false
		// 	}
		// }
		// Add peer to peer handler.
		// en.peers.Add(nodeENR, peerData.ID, multiAddr, network.DirUnknown)
		return true
	}
}

func (l *Listener) connectWithPeer(ctx context.Context, h host.Host, info peer.AddrInfo, node *enode.Node) error {
	if info.ID == h.ID() {
		return nil
	}
	logger := slog.With("id", node.ID().TerminalString())
	logger.Info("connecting to peer", "info", info.String())
	// if s.Peers().IsBad(info.ID) {
	// 	return errors.New("refused to connect to bad peer")
	// }
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := h.Connect(ctx, info); err != nil {
		// s.Peers().Scorers().BadResponsesScorer().Increment(info.ID)
		return err
	}
	return nil
}

func (l *Listener) ServeBeaconMetadata(ctx context.Context, h host.Host) {
	go func() {
		comp := new(reqresp.SnappyCompression)
		listenReq := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
			var reqMetadata common.MetaData
			err := handler.ReadRequest(&reqMetadata)
			if err != nil {
				_ = handler.WriteErrorChunk(reqresp.InvalidReqCode, "could not parse status request")
				slog.Debug("failed to read beacon metadata request", "error", err, "peer_id", peerId.String())
			} else {
				if err := handler.WriteResponseChunk(reqresp.SuccessCode, &l.LocalMetadata); err != nil {
					slog.Debug("failed to respond to beacon metadata request", "error", err)
				} else {
					slog.Debug("handled beacon metadata request")
				}
			}
		}
		m := methods.MetaDataRPCv2
		streamHandler := m.MakeStreamHandler(ctx, comp, listenReq, RPCTimeout)
		h.SetStreamHandler(m.Protocol, streamHandler)
		slog.Info("Started serving Beacon Metadata")
		<-ctx.Done()
		slog.Info("Stopped serving Beacon Metadata")
	}()
}

func (l *Listener) ServeBeaconPing(ctx context.Context, h host.Host) {
	go func() {
		comp := new(reqresp.SnappyCompression)
		listenReq := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
			var ping common.Ping
			err := handler.ReadRequest(&ping)
			if err != nil {
				_ = handler.WriteErrorChunk(reqresp.InvalidReqCode, "could not parse ping request")
				slog.Debug("failed to read ping request", "error", err, "peer_id", peerId.String())
			} else {
				if err := handler.WriteResponseChunk(reqresp.SuccessCode, &ping); err != nil {
					slog.Debug("failed to respond to ping request", "error", err)
				} else {
					slog.Debug("handled ping request", "request", ping)
				}
			}
		}
		m := methods.PingRPCv1
		streamHandler := m.MakeStreamHandler(ctx, comp, listenReq, RPCTimeout)
		h.SetStreamHandler(m.Protocol, streamHandler)
		slog.Info("Started serving ping")
		<-ctx.Done()
		slog.Info("Stopped serving ping")
	}()
}

func (l *Listener) ServeBeaconGoodbye(ctx context.Context, h host.Host) {
	go func() {
		comp := new(reqresp.SnappyCompression)
		listenReq := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
			var goodbye common.Goodbye
			err := handler.ReadRequest(&goodbye)
			if err != nil {
				_ = handler.WriteErrorChunk(reqresp.InvalidReqCode, "could not parse goodbye request")
				slog.Debug("failed to read goodbye request", "error", err, "peer_id", peerId.String())
			} else {
				if err := handler.WriteResponseChunk(reqresp.SuccessCode, &goodbye); err != nil {
					slog.Debug("failed to respond to goodbye request", "error", err)
				} else {
					slog.Debug("handled goodbye request", "request", goodbye)
				}
			}
		}
		m := methods.GoodbyeRPCv1
		streamHandler := m.MakeStreamHandler(ctx, comp, listenReq, RPCTimeout)
		h.SetStreamHandler(m.Protocol, streamHandler)
		slog.Info("Started serving goodbye")
		<-ctx.Done()
		slog.Info("Stopped serving goodbye")
	}()
}

func (l *Listener) ServeBeaconStatus(ctx context.Context, h host.Host) {
	go func() {
		comp := new(reqresp.SnappyCompression)
		listenReq := func(ctx context.Context, peerId peer.ID, handler reqresp.ChunkedRequestHandler) {
			var reqStatus common.Status
			err := handler.ReadRequest(&reqStatus)
			if err != nil {
				_ = handler.WriteErrorChunk(reqresp.InvalidReqCode, "could not parse status request")
				slog.Debug("failed to read status request", "error", err, "peer_id", peerId.String())
			} else {
				if err := handler.WriteResponseChunk(reqresp.SuccessCode, &l.LocalStatus); err != nil {
					slog.Debug("failed to respond to status request", "error", err)
				} else {
					// update if possible out status
					l.UpdateStatus(reqStatus)
					slog.Debug("handled status request")
				}
			}
		}
		m := methods.StatusRPCv1
		streamHandler := m.MakeStreamHandler(ctx, comp, listenReq, RPCTimeout)
		h.SetStreamHandler(m.Protocol, streamHandler)
		slog.Info("Started serving Beacon Status")
		<-ctx.Done()
		slog.Info("Stopped serving Beacon Status")
	}()
}

const (
	PeerNextDialTime = "peer_next_dial_time" // peerstore metadata key, holds a time.Time indicating the next time we are allowed to dial the peer
)

// isReadyToDial checks whether the given peer can be dialed at this time.
func isReadyToDial(pstore peerstore.Peerstore, pid peer.ID) bool {
	v, err := pstore.Get(pid, PeerNextDialTime)
	if err != nil {
		//lint:ignore S1008 keep this explicit
		if errors.Is(err, peerstore.ErrNotFound) {
			// no dial time set yet
			return true
		}
		return false
	}

	nextDialTime, ok := v.(time.Time)
	if !ok {
		// invalid dial time, dialling will fix it
		return true
	}

	if nextDialTime.IsZero() {
		// no dial time
		return true
	}

	return nextDialTime.Before(time.Now())
}

func advanceNextDialTime(pstore peerstore.Peerstore, pid peer.ID) {
	// don't adjust next dial time if previous one hasn't expired
	if !isReadyToDial(pstore, pid) {
		return
	}

	// random delay between 1s and 6s
	delay := time.Duration(1000+rand.Intn(5000)) * time.Millisecond
	nextDialTime := time.Now().Add(delay)
	if err := pstore.Put(pid, PeerNextDialTime, nextDialTime); err != nil {
		slog.Error("failed to set next dial time", "peer_id", pid, "error", err)
	}
}
