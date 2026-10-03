package throttle

import "sync/atomic"

// Stats is a point-in-time view of a Window.
type Stats struct {
	// Active counts keys with a run in progress, including keys still
	// waiting on the engine.
	Active int
	// Pending counts keys with calls waiting for a trailing run.
	Pending int
	// Keys counts keys tracked in memory.
	Keys int
	// Timers counts window timers and leases currently scheduled.
	Timers int
}

// counters are updated at every state transition, so Stats is O(1) and
// lock-free instead of scanning every key.
type counters struct {
	active  atomic.Int64
	pending atomic.Int64
	keys    atomic.Int64
	timers  atomic.Int64
}

func (c *counters) snapshot() Stats {
	return Stats{
		Active:  int(c.active.Load()),
		Pending: int(c.pending.Load()),
		Keys:    int(c.keys.Load()),
		Timers:  int(c.timers.Load()),
	}
}
