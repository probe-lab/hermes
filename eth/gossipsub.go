package eth

import (
	"bytes"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/protolambda/zrnt/eth2/beacon/capella"
	"github.com/protolambda/zrnt/eth2/beacon/common"
	"github.com/protolambda/zrnt/eth2/beacon/phase0"
	"github.com/protolambda/zrnt/eth2/configs"
	"github.com/protolambda/ztyp/codec"

	"github.com/probe-lab/hermes/node"
)

var (
	ErrorNoSubnet          = errors.New("no subnet found in topic")
	ErrorNotParsableSubnet = errors.New("not parseable subnet int")
)

type TrackedAttestation struct {
	MsgID  string
	Sender peer.ID
	Subnet int

	ArrivalTime time.Time     // time of arrival
	TimeInSlot  time.Duration // exact time inside the slot (range between 0secs and 12s*32slots)

	ValPubkey string
	Slot      int64
}

func (a *TrackedAttestation) IsZero() bool {
	return a.Slot == 0
}

type TrackedBeaconBlock struct {
	MsgID  string
	Sender peer.ID

	ArrivalTime time.Time     // time of arrival
	TimeInSlot  time.Duration // exact time inside the slot (range between 0secs and 12s*32slots)

	ValIndex int64
	Slot     int64
}

func (a *TrackedBeaconBlock) IsZero() bool {
	return a.Slot == 0
}

func GetSubnetFromTopic(topic string) (int, error) {
	re := regexp.MustCompile(`attestation_([0-9]+)`)
	match := re.FindAllString(topic, -1)
	if len(match) < 1 {
		return -1, ErrorNoSubnet
	}

	re2 := regexp.MustCompile("([0-9]+)")
	match = re2.FindAllString(match[0], -1)
	if len(match) < 1 {
		return -1, ErrorNotParsableSubnet
	}
	subnet, err := strconv.Atoi(match[0])
	if err != nil {
		return -1, errors.Wrap(err, "unable to conver subnet to int")
	}
	return subnet, nil
}

// EthMessageBaseHandler extracts that basing message data from the
// entire Pubsub.Message message
func EthMessageBaseHandler(topic string, msg *pubsub.Message) ([]byte, error) {
	var msgData []byte
	msgData, err := snappy.Decode(nil, msg.Data)
	if err != nil {
		return msgData, errors.Wrap(err, "cannot decompress snappy message")
	}
	return msgData, nil
}

type EthMessageHandler struct {
	genesisTime time.Time
	pubkeys     []*common.BLSPubkey // pubkeys of those validators we want to track
}

func NewEthMessageHandler(genesis time.Time, pubkeysStr []string) (*EthMessageHandler, error) {
	subHandler := &EthMessageHandler{
		genesisTime: genesis,
		pubkeys:     make([]*common.BLSPubkey, 0, len(pubkeysStr)),
	}
	// parse pubkeys
	for _, pubkeyStr := range pubkeysStr {
		blsKey := &common.BLSPubkey{}
		err := blsKey.UnmarshalText([]byte(pubkeyStr))
		if err != nil {
			return subHandler, err
		}
		if blsKey.String() != pubkeyStr {
			return subHandler, fmt.Errorf("blsKey (%s) and given-pubkey (%s) missmatch", blsKey.String(), pubkeyStr)
		}
		subHandler.pubkeys = append(subHandler.pubkeys, blsKey)
	}
	return subHandler, nil
}

// as reference https://github.com/protolambda/zrnt/blob/4ecaadfe0cb3c0a90d85e6a6dddcd3ebed0411b9/eth2/beacon/phase0/indexed.go#L99
func (s *EthMessageHandler) SubnetMessageHandler(msg *pubsub.Message) (node.PersistableMsg, error) {
	slog.Debug("SubnetMessageHandler")
	t := time.Now()
	topic := *msg.Topic

	// extract the data from the raw message
	msgBytes, err := EthMessageBaseHandler(topic, msg)
	if err != nil {
		return nil, err
	}
	msgBuf := bytes.NewBuffer(msgBytes)

	// once we have the data, get Attestation from it
	var attestation phase0.Attestation
	err = attestation.Deserialize(configs.Mainnet, codec.NewDecodingReader(msgBuf, uint64(len(msgBuf.Bytes()))))
	if err != nil {
		return nil, err
	}

	// ----- TODO: attestation ownership still missing
	// // get Signing Root of the Attestation
	// signingRoot := common.ComputeSigningRoot(attestation.Data.HashTreeRoot(tree.GetHashFn()), dom)
	// attSignature, err := attestation.Signature.Signature()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to deserialize and sub-group check indexed attestation signature: %v", err)
	// }
	// for _, pubkey := range s.pubkeys {
	// 	pubk, err := pubkey.Pubkey()
	// 	if err != nil {
	// 		log.WithError(err).Warn("unable to get blsu.Pubkey from BLS.Pubkey")
	// 	}
	// 	if blsu.Verify(pubk, signingRoot[:], attSignature) {
	// 		log.Info("--------->>>>>>Attestation for a known validator found")

	// verify if the hash of the message, the signature and the pubkeys of the list of validators match

	subnet, err := GetSubnetFromTopic(*msg.Topic)
	if err != nil {
		return nil, err
	}

	trackedAttestation := &TrackedAttestation{
		MsgID:       msg.ID,
		ArrivalTime: t,
		Subnet:      subnet,
		Slot:        int64(attestation.Data.Slot),
		TimeInSlot:  GetTimeInSlot(s.genesisTime, t, int64(attestation.Data.Slot)),
		Sender:      msg.ReceivedFrom,
		ValPubkey:   "",
	}

	return trackedAttestation, nil
}

func (mh *EthMessageHandler) BeaconBlockMessageHandler(msg *pubsub.Message) (node.PersistableMsg, error) {
	t := time.Now()
	topic := *msg.Topic

	// extract the data from the raw message
	msgBytes, err := EthMessageBaseHandler(topic, msg)
	if err != nil {
		return nil, err
	}
	msgBuf := bytes.NewBuffer(msgBytes)
	bblock := new(capella.SignedBeaconBlock)

	err = bblock.Deserialize(configs.Mainnet, codec.NewDecodingReader(msgBuf, uint64(len(msgBuf.Bytes()))))
	if err != nil {
		return nil, err
	}

	trackedBlock := &TrackedBeaconBlock{
		MsgID:       msg.ID,
		Sender:      msg.ReceivedFrom,
		ArrivalTime: t,
		TimeInSlot:  GetTimeInSlot(mh.genesisTime, t, int64(bblock.Message.Slot)),
		ValIndex:    int64(bblock.Message.ProposerIndex),
		Slot:        int64(bblock.Message.Slot),
	}

	return trackedBlock, nil
}
