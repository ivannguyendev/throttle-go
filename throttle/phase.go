package throttle

import "context"

// Phase tells a run whether it is the leading run of an idle key or the
// trailing run that covers the calls coalesced during a window.
type Phase uint8

const (
	// Leading is the immediate run of the first call for an idle key.
	Leading Phase = iota + 1
	// Trailing is the single run at the end of a window that covers every
	// call coalesced during it.
	Trailing
)

// String returns "leading" or "trailing".
func (p Phase) String() string {
	switch p {
	case Leading:
		return "leading"
	case Trailing:
		return "trailing"
	default:
		return "unknown"
	}
}

// Func is the work a Window throttles. The trailing run executes the latest
// Func passed for the key, so write it to recompute from source.
//
// ctx is the caller's context for a leading Exec, the caller's values without
// its cancellation for a leading Wait (the run is shared with other waiters),
// and a background context for a trailing run.
type Func[T any] func(ctx context.Context, phase Phase) (T, error)
