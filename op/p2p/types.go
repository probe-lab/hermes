package p2p

import (
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/beacon/engine"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"reflect"
)

type ErrorCode int

func (c ErrorCode) IsEngineError() bool {
	return -38100 < c && c <= -38000
}

func (c ErrorCode) IsGenericRPCError() bool {
	return -32700 < c && c <= -32600
}

// Engine error codes used to be -3200x, but were rebased to -3800x:
// https://github.com/ethereum/execution-apis/pull/214
const (
	MethodNotFound           ErrorCode = -32601 // RPC method not found or not available.
	InvalidParams            ErrorCode = -32602
	UnknownPayload           ErrorCode = -38001 // Payload does not exist / is not available.
	InvalidForkchoiceState   ErrorCode = -38002 // Forkchoice state is invalid / inconsistent.
	InvalidPayloadAttributes ErrorCode = -38003 // Payload attributes are invalid / inconsistent.
	TooLargeEngineRequest    ErrorCode = -38004 // Unused, here for completeness, only used by engine_getPayloadBodiesByHashV1
	UnsupportedFork          ErrorCode = -38005 // Unused, see issue #11130.
)

var ErrBedrockScalarPaddingNotEmpty = errors.New("version 0 scalar value has non-empty padding")

// InputError can be used to create rpc.Error instances with a specific error code.
type InputError struct {
	Inner error
	Code  ErrorCode
}

func (ie InputError) Error() string {
	return fmt.Sprintf("input error %d: %s", ie.Code, ie.Inner.Error())
}

// Makes InputError implement the rpc.Error interface
func (ie InputError) ErrorCode() int {
	return int(ie.Code)
}

func (ie InputError) Unwrap() error {
	return ie.Inner
}

// Is checks if the error is the given target type.
// Any type of InputError counts, regardless of code.
func (ie InputError) Is(target error) bool {
	_, ok := target.(InputError)
	return ok // we implement Unwrap, so we do not have to check the inner type now
}

// Bytes65 is a 65-byte long byte string, and encoded with 0x-prefix in hex.
// This can be used to represent encoded secp256k ethereum signatures.
type Bytes65 [65]byte

func (b *Bytes65) UnmarshalJSON(text []byte) error {
	return hexutil.UnmarshalFixedJSON(reflect.TypeOf(b), text, b[:])
}

func (b *Bytes65) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("Bytes65", text, b[:])
}

func (b Bytes65) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

func (b Bytes65) String() string {
	return hexutil.Encode(b[:])
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes65) TerminalString() string {
	return fmt.Sprintf("0x%x..%x", b[:3], b[65-3:])
}

type Bytes32 [32]byte

func (b *Bytes32) UnmarshalJSON(text []byte) error {
	return hexutil.UnmarshalFixedJSON(reflect.TypeOf(b), text, b[:])
}

func (b *Bytes32) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("Bytes32", text, b[:])
}

func (b Bytes32) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

func (b Bytes32) String() string {
	return hexutil.Encode(b[:])
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes32) TerminalString() string {
	return fmt.Sprintf("0x%x..%x", b[:3], b[29:])
}

type Bytes8 [8]byte

func (b *Bytes8) UnmarshalJSON(text []byte) error {
	return hexutil.UnmarshalFixedJSON(reflect.TypeOf(b), text, b[:])
}

func (b *Bytes8) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("Bytes8", text, b[:])
}

func (b Bytes8) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

func (b Bytes8) String() string {
	return hexutil.Encode(b[:])
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes8) TerminalString() string {
	return fmt.Sprintf("0x%x", b[:])
}

type Bytes96 [96]byte

func (b *Bytes96) UnmarshalJSON(text []byte) error {
	return hexutil.UnmarshalFixedJSON(reflect.TypeOf(b), text, b[:])
}

func (b *Bytes96) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("Bytes96", text, b[:])
}

func (b Bytes96) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

func (b Bytes96) String() string {
	return hexutil.Encode(b[:])
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes96) TerminalString() string {
	return fmt.Sprintf("0x%x..%x", b[:3], b[93:])
}

type Bytes256 [256]byte

func (b *Bytes256) UnmarshalJSON(text []byte) error {
	return hexutil.UnmarshalFixedJSON(reflect.TypeOf(b), text, b[:])
}

func (b *Bytes256) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("Bytes32", text, b[:])
}

func (b Bytes256) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

func (b Bytes256) String() string {
	return hexutil.Encode(b[:])
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes256) TerminalString() string {
	return fmt.Sprintf("0x%x..%x", b[:3], b[253:])
}

type Uint64Quantity = hexutil.Uint64

type BytesMax32 []byte

func (b *BytesMax32) UnmarshalJSON(text []byte) error {
	if len(text) > 64+2+2 { // account for delimiter "", and 0x prefix
		return fmt.Errorf("input too long, expected at most 32 hex-encoded, 0x-prefixed, bytes: %x", text)
	}
	return (*hexutil.Bytes)(b).UnmarshalJSON(text)
}

func (b *BytesMax32) UnmarshalText(text []byte) error {
	if len(text) > 64+2 { // account for 0x prefix
		return fmt.Errorf("input too long, expected at most 32 hex-encoded, 0x-prefixed, bytes: %x", text)
	}
	return (*hexutil.Bytes)(b).UnmarshalText(text)
}

func (b BytesMax32) MarshalText() ([]byte, error) {
	return (hexutil.Bytes)(b).MarshalText()
}

func (b BytesMax32) String() string {
	return hexutil.Encode(b)
}

type Uint256Quantity = hexutil.U256

type Data = hexutil.Bytes

type PayloadID = engine.PayloadID

type ExecutionPayloadEnvelope struct {
	ParentBeaconBlockRoot *common.Hash
	ExecutionPayload      *ExecutionPayload
}

func (env *ExecutionPayloadEnvelope) ID() BlockID {
	return env.ExecutionPayload.ID()
}

func (env *ExecutionPayloadEnvelope) String() string {
	return fmt.Sprintf("envelope(%s)", env.ID())
}

type ExecutionPayload struct {
	ParentHash    common.Hash
	FeeRecipient  common.Address
	StateRoot     Bytes32
	ReceiptsRoot  Bytes32
	LogsBloom     Bytes256
	PrevRandao    Bytes32
	BlockNumber   Uint64Quantity
	GasLimit      Uint64Quantity
	GasUsed       Uint64Quantity
	Timestamp     Uint64Quantity
	ExtraData     BytesMax32
	BaseFeePerGas Uint256Quantity
	BlockHash     common.Hash
	// Array of transaction objects, each object is a byte list (DATA) representing
	// TransactionType || TransactionPayload or LegacyTransaction as defined in EIP-2718
	Transactions []Data
	// Nil if not present (Bedrock)
	Withdrawals *types.Withdrawals
	// Nil if not present (Bedrock, Canyon, Delta)
	BlobGasUsed *Uint64Quantity
	// Nil if not present (Bedrock, Canyon, Delta)
	ExcessBlobGas *Uint64Quantity
	// Nil if not present (Bedrock, Canyon, Delta, Ecotone, Fjord, Granite, Holocene)
	WithdrawalsRoot *common.Hash
}

func (payload *ExecutionPayload) ID() BlockID {
	return BlockID{Hash: payload.BlockHash, Number: uint64(payload.BlockNumber)}
}

func (payload *ExecutionPayload) String() string {
	return fmt.Sprintf("payload(%s)", payload.ID())
}

func (payload *ExecutionPayload) ParentID() BlockID {
	n := uint64(payload.BlockNumber)
	if n > 0 {
		n -= 1
	}
	return BlockID{Hash: payload.ParentHash, Number: n}
}

func (payload *ExecutionPayload) BlockRef() BlockRef {
	return BlockRef{
		Hash:       payload.BlockHash,
		Number:     uint64(payload.BlockNumber),
		ParentHash: payload.ParentHash,
		Time:       uint64(payload.Timestamp),
	}
}
