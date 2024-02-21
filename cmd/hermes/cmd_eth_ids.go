package main

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var cmdEthIds = &cli.Command{
	Name:      "ids",
	Usage:     "generates peer identities in csv format",
	UsageText: "hermes eth ids COUNT",
	Description: `This sub-command generates the given number of peer identities that
are compatible with the ethereum network. These peer identities can
be used to configure a beacon node with trusted peers.
`,
	Action: cmdEthIDsAction,
}

func cmdEthIDsAction(c *cli.Context) error {
	if c.NArg() > 1 {
		return fmt.Errorf("multiple arguments given")
	}

	count := 1

	arg := c.Args().First()
	if arg != "" {
		var err error
		count, err = strconv.Atoi(arg)
		if err != nil {
			return fmt.Errorf("failed to convert %s to int", c.Args().First())
		}
	}

	fmt.Println("peer_id,private_key")
	for i := 0; i < count; i++ {
		key, err := ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
		if err != nil {
			return fmt.Errorf("failed to generate key: %w", err)
		}

		privBytes := gcrypto.FromECDSA(key)
		if len(privBytes) != secp256k1.PrivKeyBytesLen {
			return fmt.Errorf("expected secp256k1 data size to be %d", secp256k1.PrivKeyBytesLen)
		}
		libp2pPrivKey := (*crypto.Secp256k1PrivateKey)(secp256k1.PrivKeyFromBytes(privBytes))

		pid, err := peer.IDFromPrivateKey(libp2pPrivKey)
		if err != nil {
			return fmt.Errorf("failed to generate peer ID from private key: %w", err)
		}

		fmt.Printf("%s,%s\n", pid, hex.EncodeToString(privBytes))
	}

	return nil
}
