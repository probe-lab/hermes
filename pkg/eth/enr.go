package eth

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
	beacon "github.com/protolambda/zrnt/eth2/beacon/common"
	"github.com/protolambda/ztyp/codec"
)

type ENREntryEth2 struct {
	beacon.Eth2Data
}

type ENREntryAttnets struct {
	AttnetsNum int
	Attnets    string
	raw        []byte
}

var (
	_ enr.Entry = (*ENREntryEth2)(nil)
	_ enr.Entry = (*ENREntryAttnets)(nil)
)

func (e *ENREntryEth2) ENRKey() string    { return "eth2" }
func (e *ENREntryAttnets) ENRKey() string { return "attnets" }

func NewENREntryEth2(in string) (*ENREntryEth2, error) {
	forkDigest := beacon.ForkDigest{}
	if err := forkDigest.UnmarshalText([]byte(in)); err != nil {
		return nil, fmt.Errorf("unmarshal fork digest")
	}

	// TODO: could calculate next fork version/epoch:
	// https://github.com/prysmaticlabs/prysm/blob/c4c28e4825031480450c481aaa6560bf615a5fd3/beacon-chain/p2p/fork.go#L96

	return &ENREntryEth2{
		Eth2Data: beacon.Eth2Data{
			ForkDigest:    forkDigest,
			NextForkEpoch: math.MaxUint64,
		},
	}, nil
}

func (e *ENREntryEth2) EncodeRLP(w io.Writer) error {
	var b bytes.Buffer
	if err := e.Eth2Data.Serialize(codec.NewEncodingWriter(&b)); err != nil {
		return err
	}

	enc, err := rlp.EncodeToBytes(b.Bytes())
	if err != nil {
		return err
	}
	_, err = w.Write(enc)

	return err
}

func (e *ENREntryEth2) DecodeRLP(s *rlp.Stream) error {
	b, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("failed to get bytes for attnets ENR entry: %w", err)
	}

	if err := e.Eth2Data.Deserialize(codec.NewDecodingReader(bytes.NewReader(b), uint64(len(b)))); err != nil {
		return fmt.Errorf("deserialize eth2 beacon data ENR entry: %w", err)
	}

	return nil
}

func NewENREntryAttnets(in string) (*ENREntryAttnets, error) {
	if len(in) >= 2 && in[0] == '0' && (in[1] == 'x' || in[1] == 'X') {
		in = in[2:]
	}

	result, err := hex.DecodeString(in)
	if err != nil {
		return nil, fmt.Errorf("")
	}

	if len(result) != 8 {
		return nil, fmt.Errorf("unexpected length of input '%s'", string(in))
	}

	return &ENREntryAttnets{
		AttnetsNum: bits.OnesCount64(binary.BigEndian.Uint64(result)),
		Attnets:    in,
		raw:        result,
	}, nil
}

func (e *ENREntryAttnets) EncodeRLP(w io.Writer) error {
	enc, err := rlp.EncodeToBytes(e.raw[:])
	if err != nil {
		return err
	}
	_, err = w.Write(enc)

	return err
}

func (e *ENREntryAttnets) DecodeRLP(s *rlp.Stream) error {
	b, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("failed to get bytes for attnets ENR entry: %w", err)
	}

	e.AttnetsNum = bits.OnesCount64(binary.BigEndian.Uint64(b))
	e.Attnets = "0x" + hex.EncodeToString(b)
	e.raw = b

	return nil
}
