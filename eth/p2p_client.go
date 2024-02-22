package eth

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/encoder"
	eth "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"
)

type P2PClient struct {
	host       host.Host
	enc        encoder.NetworkEncoding
	forkDigest [4]byte
}

func NewP2PClient(h host.Host, forkDigest [4]byte) *P2PClient {
	b := &P2PClient{
		host:       h,
		enc:        encoder.SszNetworkEncoder{},
		forkDigest: forkDigest,
	}

	return b
}

func (p *P2PClient) protocolID(topic string) protocol.ID {
	return protocol.ID(topic + p.enc.ProtocolSuffix())
}

func (p *P2PClient) Status(ctx context.Context, pid peer.ID) (*eth.Status, error) {
	s, err := p.host.NewStream(ctx, pid, p.protocolID(p2p.RPCStatusTopicV1))
	if err != nil {
		return nil, fmt.Errorf("new stream to peer %s: %w", pid, err)
	}
	defer logDeferErr(s.Close, "failed closing stream")

	// define the maximum time we allow for writing data to the stream
	if err = s.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil { // TODO: parameterize timeout
		return nil, fmt.Errorf("failed to set write deadline: %w", err)
	}

	// actually write the data to the stream
	// TODO: get correct status
	req := &eth.Status{
		ForkDigest:     p.forkDigest[:],
		FinalizedRoot:  []byte{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4},
		FinalizedEpoch: 0,
		HeadRoot:       []byte{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4},
		HeadSlot:       0,
	}
	if _, err = p.enc.EncodeWithMaxLength(s, req); err != nil {
		return nil, fmt.Errorf("failed to write encode status payload: %w", err)
	}

	// indicate to our peer that we're not sending anything else
	if err = s.CloseWrite(); err != nil {
		return nil, fmt.Errorf("failed to close writer side of stream: %w", err)
	}

	// define the maximum time we allow for receiving a response
	if err = s.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil { // TODO: parameterize timeout
		return nil, fmt.Errorf("failed to set write deadline: %w", err)
	}

	code := make([]byte, 1)
	if _, err = io.ReadFull(s, code); err != nil {
		return nil, fmt.Errorf("failed reading response code: %w", err)
	}

	// code == 0 means success
	// code != 0 means error
	if int(code[0]) != 0 {
		errData, _ := io.ReadAll(s)
		return nil, fmt.Errorf("failed reading error data: %s", string(errData))
	}

	// read and decode status response
	resp := &eth.Status{}
	if err := p.enc.DecodeWithMaxLength(s, resp); err != nil {
		return nil, fmt.Errorf("failed reading response data: %w", err)
	}

	return resp, nil
}
