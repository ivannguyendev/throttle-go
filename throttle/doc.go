// Package throttle is a keyed leading + trailing throttle with a pluggable
// window store: Redis for clusters (package redisengine), in-memory for a
// single process (MemoryEngine, the default).
//
// The first call for a key runs immediately (Leading). Calls made while the
// key runs or while its window is open collapse into exactly one Trailing
// run at the end of the window. Each window starts when a run finishes, so
// runs of one key never overlap, and a lease keeps long runs exclusive
// across instances.
//
//	w, err := throttle.New[Room]("sync:room:", throttle.Config{Window: time.Second})
//	if err != nil {
//		return err
//	}
//	defer w.Close()
//
//	ran, err := w.Exec(ctx, roomID, syncRoom)  // true: ran now; false: coalesced
//	room, err := w.Wait(ctx, roomID, syncRoom) // value of the run covering this call
//
// The Window fails open: on an engine error or an open circuit breaker the
// job still runs, and in-memory state keeps throttling per instance.
package throttle
