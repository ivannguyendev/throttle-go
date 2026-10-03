// Package shard maps string keys onto a fixed number of lock shards.
package shard

import "hash/maphash"

// Count is the number of shards every sharded structure in this module uses.
// A power of two keeps the index a mask instead of a division.
const Count = 32

// Index picks the shard for a key. The zero value is not usable; use New.
type Index struct {
	seed maphash.Seed
}

// New returns an Index with a random per-process seed, so shard placement
// cannot be predicted from outside.
func New() Index {
	return Index{seed: maphash.MakeSeed()}
}

// Of returns the shard number for key, in [0, Count).
func (i Index) Of(key string) int {
	return int(maphash.String(i.seed, key) & (Count - 1))
}
