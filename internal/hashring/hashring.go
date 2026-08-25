// Package hashring implements a weighted consistent hash ring with a lock-free read path.
package hashring

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// Member is a participant on the ring. Weight is relative to 100: a member with Weight 200 owns
// roughly twice the keyspace of a member with Weight 100.
type Member struct {
	ID     string
	Weight int
}

// snapshot is immutable once published. hashes and owners are parallel arrays ordered by
// (hash, ownerID); the tiebreak on ownerID keeps the ring byte-identical for identical membership
// regardless of the order members were supplied in.
type snapshot struct {
	hashes  []uint64
	owners  []string
	members []Member // sorted by ID
}

// Ring is safe for concurrent use. Readers only load the current snapshot; writers build a
// replacement under mu and swap it in.
type Ring struct {
	virtualNodes int
	mu           sync.Mutex
	snap         atomic.Pointer[snapshot]
}

// New builds a ring with virtualNodes points per unit-weight (100) member.
func New(virtualNodes int, members ...Member) *Ring {
	r := &Ring{virtualNodes: virtualNodes}
	r.snap.Store(build(virtualNodes, members))
	return r
}

// Set atomically replaces the whole membership. Duplicate IDs collapse to the last occurrence.
func (r *Ring) Set(members []Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snap.Store(build(r.virtualNodes, members))
}

// Add inserts m, replacing any existing member with the same ID.
func (r *Ring) Add(m Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.snap.Load().members
	next := make([]Member, 0, len(cur)+1)
	next = append(next, cur...)
	next = append(next, m)
	r.snap.Store(build(r.virtualNodes, next))
}

// Remove drops the member with the given ID; unknown IDs are a no-op.
func (r *Ring) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.snap.Load().members
	next := make([]Member, 0, len(cur))
	for _, m := range cur {
		if m.ID != id {
			next = append(next, m)
		}
	}
	if len(next) == len(cur) {
		return
	}
	r.snap.Store(build(r.virtualNodes, next))
}

// Members returns a copy of the membership, sorted by ID.
func (r *Ring) Members() []Member {
	return slices.Clone(r.snap.Load().members)
}

// Len reports the number of distinct members.
func (r *Ring) Len() int {
	return len(r.snap.Load().members)
}

// VirtualNodes reports the number of points on the ring.
func (r *Ring) VirtualNodes() int {
	return len(r.snap.Load().hashes)
}

// Get returns the primary owner of key. The second result is false only for an empty ring.
func (r *Ring) Get(key string) (string, bool) {
	s := r.snap.Load()
	if len(s.hashes) == 0 {
		return "", false
	}
	return s.owners[s.index(xxhash.Sum64String(key))], true
}

// GetN walks the ring clockwise from key's position and returns up to n distinct member ids in
// preference order. It returns fewer than n only when the ring holds fewer members.
func (r *Ring) GetN(key string, n int) []string {
	if n <= 0 {
		return nil
	}
	s := r.snap.Load()
	if len(s.hashes) == 0 {
		return nil
	}
	if n > len(s.members) {
		n = len(s.members)
	}
	out := make([]string, 0, n)
	start := s.index(xxhash.Sum64String(key))
	// Bounded by the ring size: a full lap can only ever yield every distinct owner once.
	for i := range s.hashes {
		owner := s.owners[(start+i)%len(s.hashes)]
		if slices.Contains(out, owner) {
			continue
		}
		out = append(out, owner)
		if len(out) == n {
			break
		}
	}
	return out
}

// index returns the position of the first point at or after h, wrapping to 0.
func (s *snapshot) index(h uint64) int {
	i := sort.Search(len(s.hashes), func(i int) bool { return s.hashes[i] >= h })
	if i == len(s.hashes) {
		return 0
	}
	return i
}

func build(virtualNodes int, members []Member) *snapshot {
	uniq := make(map[string]Member, len(members))
	for _, m := range members {
		uniq[m.ID] = m
	}
	if len(uniq) == 0 {
		return &snapshot{}
	}

	sorted := make([]Member, 0, len(uniq))
	total := 0
	for _, m := range uniq {
		sorted = append(sorted, m)
		total += vnodeCount(virtualNodes, m.Weight)
	}
	slices.SortFunc(sorted, func(a, b Member) int { return cmp.Compare(a.ID, b.ID) })

	type point struct {
		hash  uint64
		owner string
	}
	points := make([]point, 0, total)
	for _, m := range sorted {
		for i := range vnodeCount(virtualNodes, m.Weight) {
			points = append(points, point{hash: xxhash.Sum64String(fmt.Sprintf("%s#%d", m.ID, i)), owner: m.ID})
		}
	}
	slices.SortFunc(points, func(a, b point) int {
		return cmp.Or(cmp.Compare(a.hash, b.hash), cmp.Compare(a.owner, b.owner))
	})

	s := &snapshot{
		hashes:  make([]uint64, len(points)),
		owners:  make([]string, len(points)),
		members: sorted,
	}
	for i, p := range points {
		s.hashes[i] = p.hash
		s.owners[i] = p.owner
	}
	return s
}

// vnodeCount is defensive about weight: config validation rejects non-positive weights, so a
// zero here means an unvalidated caller and is treated as the default rather than erased.
func vnodeCount(virtualNodes, weight int) int {
	if weight <= 0 {
		weight = 100
	}
	n := virtualNodes * weight / 100
	if n < 1 {
		return 1
	}
	return n
}
