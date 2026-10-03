package shard

import (
	"strconv"
	"testing"
)

func TestOfIsStableAndInRange(t *testing.T) {
	idx := New()
	seen := make(map[int]bool)
	for i := range 1000 {
		key := "key:" + strconv.Itoa(i)
		got := idx.Of(key)
		if got < 0 || got >= Count {
			t.Fatalf("Of(%q) = %d, want [0,%d)", key, got, Count)
		}
		if again := idx.Of(key); again != got {
			t.Fatalf("Of(%q) not stable: %d then %d", key, got, again)
		}
		seen[got] = true
	}
	if len(seen) < Count/2 {
		t.Fatalf("1000 keys landed on only %d of %d shards", len(seen), Count)
	}
}
