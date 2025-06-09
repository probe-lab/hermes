package benchmarks

import (
	"crypto/rand"
	"time"

	"github.com/probe-lab/hermes/host"
)

var (
	oneMb      int   = 4194304 // 1MB
	byteLimits []int = []int{
		4 * oneMb,
		8 * oneMb,
		16 * oneMb,
		24 * oneMb,
	}
)

// randomTraceGenerator generate as many traces as needed with a given size
// until a limit is softly reached
func randomEventGenerator(
	traceSize int,
	byteIterLimit int,
) (int, int, []any) {
	traces := make([]any, 0)
	totBytes := 0
	for totBytes < byteIterLimit {
		pEvent := host.GenericParquetEvent{
			BaseEvent: host.BaseEvent{
				Timestamp:  time.Now().UnixMilli(),
				Type:       randString(8),
				ProducerID: randString(8),
			},
			Topic:   randString(8),
			Payload: randString(traceSize + (4 * 8)),
		}
		traces = append(traces, pEvent)
		// json bytes
		totBytes += int(host.SizeOfEvent(pEvent))
	}
	return totBytes, len(traces), traces
}

func randString(size int) string {
	b := make([]byte, size)
	_, _ = rand.Read(b)
	return string(b)
}
