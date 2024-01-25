package eth

import (
	"crypto/elliptic"
	"fmt"
	"net"

	ma "github.com/multiformats/go-multiaddr"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerInfo struct {
	PeerID     peer.ID
	Maddrs     []ma.Multiaddr
	ForkDigest string
	Attnets    string
}

func (info *PeerInfo) AddrInfo() peer.AddrInfo {
	return peer.AddrInfo{
		ID:    info.PeerID,
		Addrs: info.Maddrs,
	}
}

func ParseEnode(n *enode.Node) (*PeerInfo, error) {
	pubKey := n.Pubkey()
	if pubKey == nil {
		return nil, fmt.Errorf("no public key")
	}

	pubBytes := elliptic.Marshal(secp256k1.S256(), pubKey.X, pubKey.Y)
	secpKey, err := crypto.UnmarshalSecp256k1PublicKey(pubBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal secp256k1 public key: %w", err)
	}

	pid, err := peer.IDFromPublicKey(secpKey)
	if err != nil {
		return nil, fmt.Errorf("peer ID from public key: %w", err)
	}

	var ipScheme string
	if p4 := n.IP().To4(); len(p4) == net.IPv4len {
		ipScheme = "ip4"
	} else {
		ipScheme = "ip6"
	}

	maddrs := []ma.Multiaddr{}
	if n.UDP() != 0 {
		maddrStr := fmt.Sprintf("/%s/%s/udp/%d", ipScheme, n.IP(), n.UDP())
		maddr, err := ma.NewMultiaddr(maddrStr)
		if err != nil {
			return nil, fmt.Errorf("parse multiaddress %s: %w", maddrStr, err)
		}
		maddrs = append(maddrs, maddr)
	}

	if n.TCP() != 0 {
		maddrStr := fmt.Sprintf("/%s/%s/tcp/%d", ipScheme, n.IP(), n.TCP())
		maddr, err := ma.NewMultiaddr(maddrStr)
		if err != nil {
			return nil, fmt.Errorf("parse multiaddress %s: %w", maddrStr, err)
		}
		maddrs = append(maddrs, maddr)
	}

	forkDigest := ""
	var enrEntryEth2 ENREntryEth2
	if err := n.Load(&enrEntryEth2); err == nil {
		forkDigest = enrEntryEth2.ForkDigest.String()
	}

	attnets := ""
	var enrEntryAttnets ENREntryAttnets
	if err := n.Load(&enrEntryAttnets); err == nil {
		attnets = enrEntryAttnets.Attnets
	}

	info := &PeerInfo{
		PeerID:     pid,
		Maddrs:     maddrs,
		ForkDigest: forkDigest,
		Attnets:    attnets,
	}

	return info, nil
}
