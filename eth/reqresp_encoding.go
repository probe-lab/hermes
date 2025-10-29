package eth

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p/core/network"
	dynssz "github.com/pk910/dynamic-ssz"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type ErrorMessage []byte

// Maximum message sizes
const (
	MaxRequestSize  = 2048             // 2KB for requests
	MaxResponseSize = 16 * 1024 * 1024 // 16MB for responses
	MaxChunkSize    = 1024 * 1024      // 1MB per chunk
)

// Error codes for RPC responses
const (
	ResponseCodeSuccess           = 0x00
	ResponseCodeInvalidRequest    = 0x01
	ResponseCodeServerError       = 0x02
	ResponseCodeResourceExhausted = 0x03
)

var sszCodec = dynssz.NewDynSsz(map[string]any{})

// Helper function to get response code names for debugging
func getResponseCodeName(code byte) string {
	switch code {
	case ResponseCodeSuccess:
		return "SUCCESS"
	case ResponseCodeInvalidRequest:
		return "INVALID_REQUEST"
	case ResponseCodeServerError:
		return "SERVER_ERROR"
	case ResponseCodeResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	default:
		return "UNKNOWN"
	}
}

// Sync pools for snappy readers and writers
var (
	bufReaderPool = &sync.Pool{}
	bufWriterPool = &sync.Pool{}
)

// Writes a bytes value through a snappy buffered writer.
func writeSnappyBuffer(w io.Writer, b []byte) (int, error) {
	bufWriter := newBufferedWriter(w)
	defer bufWriterPool.Put(bufWriter)
	num, err := bufWriter.Write(b)
	if err != nil {
		// Close buf writer in the event of an error.
		if err := bufWriter.Close(); err != nil {
			return 0, err
		}
		return 0, err
	}
	return num, bufWriter.Close()
}

// Instantiates a new instance of the snappy buffered reader
// using our sync pool.
func newBufferedReader(r io.Reader) *snappy.Reader {
	rawReader := bufReaderPool.Get()
	if rawReader == nil {
		return snappy.NewReader(r)
	}
	bufR, ok := rawReader.(*snappy.Reader)
	if !ok {
		return snappy.NewReader(r)
	}
	bufR.Reset(r)
	return bufR
}

// Instantiates a new instance of the snappy buffered writer
// using our sync pool.
func newBufferedWriter(w io.Writer) *snappy.Writer {
	rawBufWriter := bufWriterPool.Get()
	if rawBufWriter == nil {
		return snappy.NewBufferedWriter(w)
	}
	bufW, ok := rawBufWriter.(*snappy.Writer)
	if !ok {
		return snappy.NewBufferedWriter(w)
	}
	bufW.Reset(w)
	return bufW
}

// writeVarint writes a varint to the stream
func writeVarint(w io.Writer, val uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, val)
	_, err := w.Write(buf[:n])
	return err
}

// readVarint reads a varint from the stream
func readVarint(r io.Reader) (uint64, error) {
	var buf [binary.MaxVarintLen64]byte
	for i := 0; i < len(buf); i++ {
		if _, err := r.Read(buf[i : i+1]); err != nil {
			return 0, err
		}
		if buf[i] < 0x80 {
			val, n := binary.Uvarint(buf[:i+1])
			if n <= 0 {
				return 0, fmt.Errorf("invalid varint")
			}
			return val, nil
		}
	}
	return 0, fmt.Errorf("varint too long")
}

// writeRequest writes a request to the stream with SSZ+Snappy encoding
func (r *ReqResp) writeRequest(stream network.Stream, req any) error {
	defer stream.CloseWrite()

	// Set write deadline
	if err := stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	if req == nil {
		// we close the write side of the stream immediately, communicating we have no data to send
		return nil
	}

	// Marshal to SSZ if the request is not nil
	data, err := sszCodec.MarshalSSZ(req)
	if err != nil {
		return fmt.Errorf("failed to marshal SSZ: %w", err)
	}

	// Validate size
	msgSize := len(data)
	if msgSize > MaxRequestSize {
		return fmt.Errorf("request too large: %d bytes", msgSize)
	}

	// Write to buffer first for inspection
	var buf bytes.Buffer

	// Write uncompressed length prefix (varint)
	if err := writeVarint(&buf, uint64(msgSize)); err != nil {
		return fmt.Errorf("failed to write uncompressed length to buffer: %w", err)
	}

	// Compress with snappy buffered writer
	if _, err := writeSnappyBuffer(&buf, data); err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	log.WithFields(log.Fields{
		"protocol":  stream.Protocol(),
		"data":      hexutil.Encode(data),
		"data_len":  len(data),
		"wire_data": hexutil.Encode(buf.Bytes()),
		"wire_len":  buf.Len(),
	}).Debug("writing request")

	// Write buffer to the stream
	if _, err := io.Copy(stream, &buf); err != nil {
		return fmt.Errorf("failed to write final payload to stream: %w", err)
	}

	return nil
}

// readRequest reads a request from the stream with SSZ+Snappy decoding
func (r *ReqResp) readRequest(stream network.Stream, req any) error {
	// Set read deadline
	if err := stream.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Read uncompressed length prefix
	uncompressedLength, err := readVarint(stream)
	if err != nil {
		return fmt.Errorf("failed to read uncompressed length: %w", err)
	}

	// Validate uncompressed size
	if uncompressedLength > MaxRequestSize {
		return fmt.Errorf("request too large: %d bytes", uncompressedLength)
	}

	// Calculate maximum compressed length
	maxCompressedLen := snappy.MaxEncodedLen(int(uncompressedLength))
	if maxCompressedLen < 0 {
		return fmt.Errorf("max encoded length is negative: %d", maxCompressedLen)
	}

	// Use limited reader to read compressed data
	limitedReader := io.LimitReader(stream, int64(maxCompressedLen))

	// Create buffered snappy reader
	snappyReader := newBufferedReader(limitedReader)
	defer bufReaderPool.Put(snappyReader)

	// Read decompressed data
	data := make([]byte, uncompressedLength)
	if _, err := io.ReadFull(snappyReader, data); err != nil {
		return fmt.Errorf("failed to read decompressed data: %w", err)
	}

	// Unmarshal from SSZ
	if err := sszCodec.UnmarshalSSZ(req, data); err != nil {
		return fmt.Errorf("failed to unmarshal SSZ: %w", err)
	}

	return nil
}

// writeResponse writes a response to the stream with SSZ+Snappy encoding
func (r *ReqResp) writeResponse(stream network.Stream, resp any) error {
	// Set write deadline
	if err := stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	// Write success response code
	if _, err := stream.Write([]byte{ResponseCodeSuccess}); err != nil {
		return fmt.Errorf("failed to write response code: %w", err)
	}

	// Marshal to SSZ
	data, err := sszCodec.MarshalSSZ(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal SSZ: %w", err)
	}

	// Validate size
	msgSize := len(data)
	if msgSize > MaxChunkSize {
		return fmt.Errorf("response too large: %d bytes", msgSize)
	}

	// Write uncompressed length prefix (varint)
	if err := writeVarint(stream, uint64(msgSize)); err != nil {
		return fmt.Errorf("failed to write uncompressed length: %w", err)
	}

	// Compress with snappy buffered writer
	if _, err := writeSnappyBuffer(stream, data); err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	return nil
}

// readResponse reads a response from the stream with SSZ+Snappy decoding
func (r *ReqResp) readResponse(stream network.Stream, resp any) error {
	// Get the response type for debug logging
	responseType := fmt.Sprintf("%T", resp)
	// Set read deadline
	if err := stream.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Read response code
	var code [1]byte
	if _, err := stream.Read(code[:]); err != nil {
		return fmt.Errorf("failed to read response code: %w", err)
	}

	if log.GetLevel() >= log.DebugLevel {
		log.WithFields(log.Fields{
			"response_type":     responseType,
			"response_code":     code[0],
			"response_code_hex": fmt.Sprintf("0x%02x", code[0]),
			"is_success":        code[0] == ResponseCodeSuccess,
		}).Debug("Raw response code received")
	}

	success := code[0] == ResponseCodeSuccess
	if !success {
		if log.GetLevel() >= log.DebugLevel {
			errorType := getResponseCodeName(code[0])
			log.WithFields(log.Fields{
				"response_type": responseType,
				"response_code": code[0],
				"error_type":    errorType,
			}).Debug("Non-success response code received")
		}
	}

	// Read uncompressed length prefix
	uncompressedLength, err := readVarint(stream)
	if err != nil {
		return fmt.Errorf("failed to read uncompressed length: %w", err)
	}

	if log.GetLevel() >= log.DebugLevel {
		log.WithFields(log.Fields{
			"response_type":       responseType,
			"uncompressed_length": uncompressedLength,
		}).Debug("Read uncompressed length from response")
	}

	// Validate uncompressed size
	if uncompressedLength > MaxChunkSize {
		return fmt.Errorf("response too large: %d bytes", uncompressedLength)
	}

	// Calculate maximum compressed length
	maxCompressedLen := snappy.MaxEncodedLen(int(uncompressedLength))
	if maxCompressedLen < 0 {
		return fmt.Errorf("max encoded length is negative: %d", maxCompressedLen)
	}

	// Use limited reader to read compressed data
	limitedReader := io.LimitReader(stream, int64(maxCompressedLen))

	// Create buffered snappy reader
	snappyReader := newBufferedReader(limitedReader)
	defer bufReaderPool.Put(snappyReader)

	// Read decompressed data
	data := make([]byte, uncompressedLength)
	if _, err := io.ReadFull(snappyReader, data); err != nil {
		return fmt.Errorf("failed to read decompressed data: %w", err)
	}

	// Log the raw response data
	if log.GetLevel() >= log.DebugLevel {
		log.WithFields(log.Fields{
			"response_type":       responseType,
			"uncompressed_length": uncompressedLength,
			"raw_data_hex":        fmt.Sprintf("0x%x", data),
			"raw_data_len":        len(data),
		}).Debug("Raw response data received")
	}

	if !success {
		var errorMessage ErrorMessage
		l := log.WithFields(log.Fields{
			"response_type": responseType,
			"raw_data_hex":  fmt.Sprintf("0x%x", data),
		})
		if err := sszCodec.UnmarshalSSZ(&errorMessage, data); err != nil {
			l.WithError(err).Error("failed to unmarshal SSZ error message")
			return fmt.Errorf("failed to unmarshal SSZ error message: %w", err)
		}
		msg := string(errorMessage)
		l.Warnf("RPC failed; error message: %s", msg)
		return fmt.Errorf("RPC failed: %s", msg)
	}

	// Unmarshal from SSZ
	if err := sszCodec.UnmarshalSSZ(resp, data); err != nil {
		if log.GetLevel() >= log.DebugLevel {
			log.WithFields(log.Fields{
				"response_type":   responseType,
				"unmarshal_error": err,
				"raw_data_hex":    fmt.Sprintf("0x%x", data),
			}).Debug("Failed to unmarshal SSZ response")
		}
		return fmt.Errorf("failed to unmarshal SSZ: %w", err)
	}

	if log.GetLevel() >= log.DebugLevel {
		log.WithFields(log.Fields{
			"response_type": responseType,
		}).Debug("Successfully unmarshaled response")
	}

	return nil
}

// readChunkedResponse handles each response chunk that is sent by the
// peer and converts it into the given type.
// Adaptation from Prysm's -> https://github.com/prysmaticlabs/prysm/blob/2e29164582c3665cdf5a472cd4ec9838655c9754/beacon-chain/sync/rpc_chunked_response.go#L85
func (r *ReqResp) readChunkedResponse(stream network.Stream, resp any, isFirstChunk bool, forkDigest []byte) error {
	if !isFirstChunk {
		if err := stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
			return fmt.Errorf("failed setting write deadline on stream: %w", err)
		}
	}

	var code [1]byte
	if _, err := stream.Read(code[:]); err != nil {
		return fmt.Errorf("failed to read response code: %w", err)
	}

	if code[0] != ResponseCodeSuccess {
		return fmt.Errorf("RPC error code: %d", code[0])
	}

	// Handle deadlines differently for first chunk
	if isFirstChunk {
		// set deadline for reading from stream
		if err := stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
			return fmt.Errorf("failed setting write deadline on stream: %w", err)
		}
	}

	if len(forkDigest) > 0 {
		// Retrieve the fork digest.
		ctxBytes, err := readContextFromStream(stream)
		if err != nil {
			return errors.Wrap(err, "read context from stream")
		}

		// Check if the fork digest is recognized.
		if string(ctxBytes) != string(forkDigest) {
			return errors.Errorf("unrecognized fork digest %#x", ctxBytes)
		}
	}

	// Read uncompressed length prefix
	uncompressedLength, err := readVarint(stream)
	if err != nil {
		return fmt.Errorf("failed to read uncompressed length: %w", err)
	}

	// Validate uncompressed size
	if uncompressedLength > MaxChunkSize {
		return fmt.Errorf("response too large: %d bytes", uncompressedLength)
	}

	// Calculate maximum compressed length
	maxCompressedLen := snappy.MaxEncodedLen(int(uncompressedLength))
	if maxCompressedLen < 0 {
		return fmt.Errorf("max encoded length is negative: %d", maxCompressedLen)
	}

	// Use limited reader to read compressed data
	limitedReader := io.LimitReader(stream, int64(maxCompressedLen))

	// Create buffered snappy reader
	snappyReader := newBufferedReader(limitedReader)
	defer bufReaderPool.Put(snappyReader)

	// Read decompressed data
	data := make([]byte, uncompressedLength)
	if _, err := io.ReadFull(snappyReader, data); err != nil {
		return fmt.Errorf("failed to read decompressed data: %w", err)
	}

	// Unmarshal from SSZ
	if err := sszCodec.UnmarshalSSZ(resp, data); err != nil {
		return fmt.Errorf("failed to unmarshal SSZ: %w", err)
	}

	return nil
}

// reads any attached context-bytes to the payload.
func readContextFromStream(stream network.Stream) ([]byte, error) {
	// Read context (fork-digest) from stream
	b := make([]byte, 4)
	if _, err := io.ReadFull(stream, b); err != nil {
		return nil, err
	}
	return b, nil
}
