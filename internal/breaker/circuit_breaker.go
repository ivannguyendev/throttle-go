// Package breaker is a consecutive-failure circuit breaker with a single
// half-open probe.
package breaker

import (
	"sync"
	"time"
)

// Admission is the breaker's answer to "may I call the dependency now?".
type Admission uint8

const (
	// Closed means call normally.
	Closed Admission = iota
	// Probe means the cooldown passed; this one call decides the breaker's state.
	Probe
	// Open means skip the call.
	Open
)

// Breaker opens after Threshold consecutive failures and stays open for
// Cooldown. Then exactly one probe is admitted: success closes it, failure
// reopens it for another Cooldown. It is safe for concurrent use.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	failures  int
	open      bool
	probing   bool
	openUntil time.Time
}

// New returns a closed breaker. threshold < 1 is treated as 1.
func New(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{threshold: max(threshold, 1), cooldown: cooldown}
}

// IsOpen reports whether calls are currently being skipped.
func (b *Breaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

// Admit decides whether the next call may go through. A Probe admission must
// be followed by exactly one RecordSuccess, RecordFailure or RecordIgnored.
func (b *Breaker) Admit() Admission {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return Closed
	}
	if b.probing || time.Now().Before(b.openUntil) {
		return Open
	}
	b.probing = true
	return Probe
}

// RecordSuccess closes the breaker and resets the failure count.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.open = false
	b.probing = false
}

// RecordFailure counts a failure and reports whether it opened the breaker
// (threshold reached, or the probe failed).
func (b *Breaker) RecordFailure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	probeFailed := b.probing
	b.probing = false
	thresholdReached := !b.open && b.failures >= b.threshold
	if !probeFailed && !thresholdReached {
		return false
	}
	b.open = true
	b.openUntil = time.Now().Add(b.cooldown)
	return true
}

// RecordIgnored releases a probe whose outcome says nothing about the
// dependency (for example the caller's context was cancelled), so the next
// call can probe again instead of the breaker staying open forever.
func (b *Breaker) RecordIgnored() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
}
