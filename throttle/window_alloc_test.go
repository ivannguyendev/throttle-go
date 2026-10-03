//go:build !race

package throttle

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// Allocation budgets for the hot paths. Race instrumentation changes the
// counts, hence the build tag. A budget is pinned at the measured value:
// raising one needs a justification in review.

func nop(context.Context, Phase) (int, error) { return 0, nil }

func newAllocWindow(t *testing.T) *Window[int] {
	t.Helper()
	w, err := New[int]("alloc:", Config{Window: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestExecCoalescedDoesNotAllocate(t *testing.T) {
	w := newAllocWindow(t)
	ctx := context.Background()
	if ran, _ := w.Exec(ctx, "k", nop); !ran {
		t.Fatal("leading call did not run")
	}
	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = w.Exec(ctx, "k", nop)
	})
	if allocs != 0 {
		t.Fatalf("coalesced Exec allocates %v times, budget 0", allocs)
	}
}

func TestWaitRegistrationOnAnExistingCellDoesNotAllocate(t *testing.T) {
	w := newAllocWindow(t)
	ctx := context.Background()
	_, _ = w.Exec(ctx, "k", nop)
	if _, _, _, err := w.enter("k", nop, true); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		_, _, _, _ = w.enter("k", nop, true)
	})
	if allocs != 0 {
		t.Fatalf("coalesced Wait registration allocates %v times, budget 0", allocs)
	}
}

func TestOwnedEngineIsClearedOnClose(t *testing.T) {
	w := newAllocWindow(t)
	_, _ = w.Exec(context.Background(), "k", nop)
	if w.owned.Len() != 1 {
		t.Fatalf("owned engine Len = %d, want 1", w.owned.Len())
	}
	_ = w.Close()
	if w.owned.Len() != 0 {
		t.Fatalf("owned engine Len = %d after Close, want 0", w.owned.Len())
	}
}

func TestMemoryEngineSetOnAnExistingKeyAllocatesOnlyTheEntry(t *testing.T) {
	e, _ := NewMemoryEngine(MemoryEngineConfig{})
	defer e.Clear()
	ctx := context.Background()
	opt := SetOptions{TTL: time.Hour}
	_, _ = e.Set(ctx, "k", "v", opt)
	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = e.Set(ctx, "k", "v", opt)
		_, _, _ = e.Get(ctx, "k")
	})
	if allocs != 0 {
		t.Fatalf("Set+Get on an existing key allocates %v times, budget 0", allocs)
	}
}

func TestExecLeadingOnANewKeyStaysWithinBudget(t *testing.T) {
	w := newAllocWindow(t)
	ctx := context.Background()
	keys := make([]string, 2001)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	i := 0
	allocs := testing.AllocsPerRun(2000, func() {
		_, _ = w.Exec(ctx, keys[i], nop)
		i++
	})
	// keyState, fullKey, its timer and timer callback, the engine value and
	// the engine entry: everything a new key needs, nothing per call.
	if allocs > 6 {
		t.Fatalf("leading Exec on a new key allocates %v times, budget 6", allocs)
	}
}
