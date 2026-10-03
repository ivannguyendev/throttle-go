package throttle

import (
	"strconv"
	"time"
)

// The stored engine value is the window's end time in epoch milliseconds
// (D17), so an instance that loses the window race knows when to retry.

// encodeWindowEnd returns now+ttl in epoch ms, with a single allocation.
func encodeWindowEnd(now time.Time, ttl time.Duration) string {
	var buf [24]byte
	return string(strconv.AppendInt(buf[:0], now.Add(ttl).UnixMilli(), 10))
}

// parseWindowEnd reads an engine value. ok=false means the value is not an
// integer (another writer, or corruption) and the caller falls back.
func parseWindowEnd(value string) (endMs int64, ok bool) {
	endMs, err := strconv.ParseInt(value, 10, 64)
	return endMs, err == nil
}

// remainingWindow converts an engine read into a retry delay, capped at
// maxWait. A missing key means the window is already over.
func remainingWindow(value string, found bool, now time.Time, fallback, maxWait time.Duration) time.Duration {
	if !found {
		return 0
	}
	endMs, ok := parseWindowEnd(value)
	if !ok {
		return fallback
	}
	nowMs := now.UnixMilli()
	if endMs <= nowMs {
		return 0
	}
	leftMs := endMs - nowMs
	if leftMs >= maxWait.Milliseconds() {
		return maxWait
	}
	return time.Duration(leftMs) * time.Millisecond
}
