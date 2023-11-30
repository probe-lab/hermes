package eth

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"net"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// translates the arrival time into time since slot started
func GetTimeInSlot(genesis time.Time, arrivalTime time.Time, slot int64) time.Duration {
	// get slot time since genesis
	slotTime := genesis.Add((time.Duration(slot) * SecondsPerSlot))

	// compare the arrival time to the base-slot time
	inSlotTime := arrivalTime.Sub(slotTime)
	return inSlotTime
}

func ConvertToAddrInfo(node *enode.Node) (*peer.AddrInfo, ma.Multiaddr, error) {
	multiAddr, err := ConvertToSingleMultiAddr(node)
	if err != nil {
		return nil, nil, err
	}
	info, err := peer.AddrInfoFromP2pAddr(multiAddr)
	if err != nil {
		return nil, nil, err
	}
	return info, multiAddr, nil
}

func ConvertToSingleMultiAddr(node *enode.Node) (ma.Multiaddr, error) {
	pubkey := node.Pubkey()
	assertedKey, err := ECDSAPubkeyToSecp2561k(pubkey)
	if err != nil {
		return nil, fmt.Errorf("could not get pubkey: %v", err)
	}
	id, err := peer.IDFromPublicKey(assertedKey)
	if err != nil {
		return nil, fmt.Errorf("could not get peer id: %v", err)
	}
	return MultiAddressWithID(node.IP().String(), "tcp", uint(node.TCP()), id)
}

func MultiAddressWithID(ipAddr, protocol string, port uint, id peer.ID) (ma.Multiaddr, error) {
	parsedIP := net.ParseIP(ipAddr)
	if parsedIP.To4() == nil && parsedIP.To16() == nil {
		return nil, fmt.Errorf("invalid ip address provided: %s", ipAddr)
	}
	if id.String() == "" {
		return nil, fmt.Errorf("empty peer id given")
	}
	if parsedIP.To4() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/%s/%d/p2p/%s", ipAddr, protocol, port, id.String()))
	}
	return ma.NewMultiaddr(fmt.Sprintf("/ip6/%s/%s/%d/p2p/%s", ipAddr, protocol, port, id.String()))
}

// Generate PrivateKey valid for Ethereum Consensus Layer
func GenerateECDSAPrivKey() (*ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %v", err)
	}
	return key, nil
}

// Parse a Secp256k1PrivateKey from string (Libp2p), checking if it has the proper curve
func ParseECDSAPrivateKey(strKey string) (*ecdsa.PrivateKey, error) {
	return gcrypto.HexToECDSA(strKey)
}

func AdaptSecp256k1FromECDSA(ecdsaKey *ecdsa.PrivateKey) (*crypto.Secp256k1PrivateKey, error) {
	privBytes := gcrypto.FromECDSA(ecdsaKey)
	if len(privBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("expected secp256k1 data size to be %d", secp256k1.PrivKeyBytesLen)
	}
	return (*crypto.Secp256k1PrivateKey)(secp256k1.PrivKeyFromBytes(privBytes)), nil
}

func ECDSAPubkeyToSecp2561k(pubkey *ecdsa.PublicKey) (*crypto.Secp256k1PublicKey, error) {
	pubBytes := gcrypto.FromECDSAPub(pubkey)
	secp256k1, err := crypto.UnmarshalSecp256k1PublicKey(pubBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal libp2p key from geth pubkey bytes: %v", err)
	}
	return secp256k1.(*crypto.Secp256k1PublicKey), nil
}
