package throttle

import (
	"strconv"
	"testing"
	"time"
)

func TestEncodeWindowEndIsEpochMilliseconds(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	if got := encodeWindowEnd(now, 1500*time.Millisecond); got != "1700000001500" {
		t.Fatalf("encodeWindowEnd = %q", got)
	}
}

func TestRemainingWindow(t *testing.T) {
	now := time.UnixMilli(10_000)
	const fallback, maxWait = time.Second, 200 * time.Millisecond
	tests := []struct {
		name  string
		value string
		found bool
		want  time.Duration
	}{
		{"missing key: the window is over", "", false, 0},
		{"end in the future", "10150", true, 150 * time.Millisecond},
		{"end beyond the lease TTL is capped", "99999", true, maxWait},
		{"end in the past", "9000", true, 0},
		{"far past does not overflow", "-9223372036854775808", true, 0},
		{"unreadable value falls back", "soon", true, fallback},
		{"empty value falls back", "", true, fallback},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := remainingWindow(tt.value, tt.found, now, fallback, maxWait); got != tt.want {
				t.Fatalf("remainingWindow(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestComputeLeaseTiming(t *testing.T) {
	tests := []struct {
		window, interval, ttl time.Duration
	}{
		{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond},
		{time.Second, 500 * time.Millisecond, time.Second},
		{time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond},
	}
	for _, tt := range tests {
		got := computeLeaseTiming(tt.window)
		if got.interval != tt.interval || got.ttl != tt.ttl {
			t.Errorf("computeLeaseTiming(%v) = %+v, want interval %v ttl %v", tt.window, got, tt.interval, tt.ttl)
		}
	}
}

// Engine values come from outside (a shared Redis), so the parser must never
// panic and must round-trip what encodeWindowEnd writes.
func FuzzParseWindowEnd(f *testing.F) {
	for _, seed := range []string{"0", "1700000001500", "-1", "", " 1", "1e3", "9223372036854775808", "abc"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		endMs, ok := parseWindowEnd(value)
		if ok {
			again, okAgain := parseWindowEnd(strconv.FormatInt(endMs, 10))
			if !okAgain || again != endMs {
				t.Fatalf("parseWindowEnd(%q) = %d, which does not round-trip", value, endMs)
			}
		}
		_ = remainingWindow(value, true, time.Now(), time.Second, time.Second)
	})
}
