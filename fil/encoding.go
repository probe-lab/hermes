package fil

// Taken from: https://github.com/filecoin-project/go-f3/blob/main/internal/encoding/encoding.go
// Permalink: https://github.com/filecoin-project/go-f3/blob/fd3ef15f457bf01a01f10ac6748acdfbd7657ed6/internal/encoding/encoding.go#L1

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// maxDecompressedSize is the default maximum amount of memory allocated by the
// zstd decoder. The limit of 1MiB is chosen based on the default maximum message
// size in GossipSub.
const maxDecompressedSize = 1 << 20

var bufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, maxDecompressedSize)
		return &buf
	},
}

type CBORMarshalUnmarshaler interface {
	cbg.CBORMarshaler
	cbg.CBORUnmarshaler
}

type EncodeDecoder[T CBORMarshalUnmarshaler] interface {
	Encode(v T) ([]byte, error)
	Decode([]byte, T) error
}

type CBOR[T CBORMarshalUnmarshaler] struct{}

func NewCBOR[T CBORMarshalUnmarshaler]() *CBOR[T] {
	return &CBOR[T]{}
}

func (c *CBOR[T]) Encode(m T) (_ []byte, _err error) {
	var out bytes.Buffer
	if err := m.MarshalCBOR(&out); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func (c *CBOR[T]) Decode(v []byte, t T) (_err error) {
	r := bytes.NewReader(v)
	return t.UnmarshalCBOR(r)
}

type ZSTD[T CBORMarshalUnmarshaler] struct {
	cborEncoding *CBOR[T]
	compressor   *zstd.Encoder
	decompressor *zstd.Decoder
}

func NewZSTD[T CBORMarshalUnmarshaler]() (*ZSTD[T], error) {
	writer, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	reader, err := zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(maxDecompressedSize),
		zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, err
	}
	return &ZSTD[T]{
		cborEncoding: &CBOR[T]{},
		compressor:   writer,
		decompressor: reader,
	}, nil
}

func (c *ZSTD[T]) Encode(t T) (_ []byte, _err error) {
	decompressed, err := c.cborEncoding.Encode(t)
	if len(decompressed) > maxDecompressedSize {
		// Error out early if the encoded value is too large to be decompressed.
		return nil, fmt.Errorf("encoded value cannot exceed maximum size: %d > %d", len(decompressed), maxDecompressedSize)
	}
	if err != nil {
		return nil, err
	}
	maxCompressedSize := c.compressor.MaxEncodedSize(len(decompressed))
	compressed := c.compressor.EncodeAll(decompressed, make([]byte, 0, maxCompressedSize))
	return compressed, nil
}

func (c *ZSTD[T]) Decode(compressed []byte, t T) (_err error) {
	buf := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(buf)
	decompressed, err := c.decompressor.DecodeAll(compressed, (*buf)[:0])
	if err != nil {
		return err
	}
	return c.cborEncoding.Decode(decompressed, t)
}
