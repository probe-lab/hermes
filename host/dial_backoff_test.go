package host

import (
	"math"
	"testing"
	"time"
)

func TestPenaltyForReachesMax(t *testing.T) {
	for _, max := range []time.Duration{
		30 * time.Second,     // below the base
		time.Minute,          // equal to the base
		time.Hour,            // the ansible default
		100 * 24 * time.Hour, // far above the old 1min<<16 ceiling (~45.5 days)
		math.MaxInt64,        // pathological, must not overflow
	} {
		db := &DialBackoff{max: max}
		var prev time.Duration
		reached := false
		for f := 1; f <= 200; f++ {
			p := db.penaltyFor(f)
			if p <= 0 {
				t.Fatalf("max=%v failures=%d: non-positive penalty %v (overflow?)", max, f, p)
			}
			if p > max {
				t.Fatalf("max=%v failures=%d: penalty %v exceeds max", max, f, p)
			}
			if p < prev {
				t.Fatalf("max=%v failures=%d: penalty went backwards %v -> %v", max, f, prev, p)
			}
			prev = p
			if p == max {
				reached = true
			}
		}
		if !reached {
			t.Fatalf("max=%v: never reached the configured maximum, stalled at %v", max, prev)
		}
	}
}
