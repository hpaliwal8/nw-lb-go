// Package balancer turns an affinity key into an ordered list of backends to try.
//
// The ring holds every configured backend at all times: membership is a function of configuration,
// not of health. Dropping a sick backend from the ring would move the keys of every *other* backend
// as well, on every flap — precisely the churn consistent hashing exists to prevent. Health and
// breaker state are instead applied as a filter over the ordered candidate list the ring produces,
// so a flap only affects the keys the flapping backend owned.
package balancer

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/hashring"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
)

// snapshot is the immutable view Pick reads. Rebuild publishes a replacement, so the hot path
// neither copies the pool's backend slice nor builds an id index per call.
type snapshot struct {
	backends []*pool.Backend
	byID     map[string]*pool.Backend
}

type Balancer struct {
	pool *pool.Pool
	ring *hashring.Ring
	m    *metrics.Metrics

	snap atomic.Pointer[snapshot]

	// rr spreads requests that carry no affinity key. A counter rather than a random start, so
	// N keyless requests over N backends land exactly evenly.
	rr atomic.Uint64

	// mu serialises Rebuild, a control-plane path. Pick never takes it.
	mu     sync.Mutex
	closed atomic.Bool
}

// pickFilter is one rung of the fallback ladder.
type pickFilter uint8

const (
	filterAvailable pickFilter = iota // healthy and the breaker admits
	filterHealthy                     // healthy, breaker ignored
	filterAny                         // fail open: a stale health signal must not black-hole traffic
)

// ladder is walked in order and the first rung that yields any candidate wins. The keyed path runs
// filterAvailable itself, so it resumes at ladder[1:].
var ladder = [...]pickFilter{filterAvailable, filterHealthy, filterAny}

func (f pickFilter) admits(b *pool.Backend) bool {
	switch f {
	case filterAvailable:
		return b.Available()
	case filterHealthy:
		return b.Healthy()
	default:
		return true
	}
}

// New builds a balancer over the pool's current membership and keeps it in sync: Rebuild is
// registered with the pool so both membership changes and health flips refresh the ring gauges.
func New(p *pool.Pool, cfg config.Routing, m *metrics.Metrics) *Balancer {
	vnodes := cfg.VirtualNodes
	if vnodes <= 0 {
		vnodes = config.Default().Routing.VirtualNodes
	}
	b := &Balancer{pool: p, ring: hashring.New(vnodes), m: m}
	b.snap.Store(&snapshot{})
	b.Rebuild()
	p.OnChange(b.Rebuild)
	return b
}

// Pick returns up to maxAttempts distinct backends in preference order. The first is the
// consistent-hash owner of key; the rest are the next distinct ring members and exist only for
// failover. Unhealthy backends and backends whose breaker is open are skipped, except that when a
// rung of the ladder filters everything out the next rung is used: available, then healthy
// regardless of the breaker, then every backend.
//
// Note that filterAvailable consumes a half-open breaker probe for each backend it admits, which is
// why the scan stops as soon as maxAttempts candidates are in hand.
func (b *Balancer) Pick(key string, maxAttempts int) []*pool.Backend {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	s := b.snap.Load()
	n := len(s.backends)
	if n == 0 {
		return nil
	}
	if maxAttempts > n {
		maxAttempts = n
	}
	out := make([]*pool.Backend, 0, maxAttempts)

	if key == "" {
		// Hashing the empty string would pin every keyless request onto a single backend, so
		// anonymous traffic starts at a rotating offset instead.
		start := int((b.rr.Add(1) - 1) % uint64(n))
		for _, f := range ladder {
			if out = collectRotation(out[:0], s.backends, start, maxAttempts, f); len(out) > 0 {
				return out
			}
		}
		return out
	}

	// Rung one, in two steps. Nearly always every backend the key prefers is available, and then
	// the first maxAttempts ring members are the whole answer — walking the ring for the full
	// preference order would be wasted work on every single request.
	ids := b.ring.GetN(key, maxAttempts)
	out = collectRing(out[:0], s, ids, maxAttempts, filterAvailable)
	if len(out) < maxAttempts && len(ids) < n {
		// Something was filtered out, so the rest of the preference order matters after all.
		ids = b.ring.GetN(key, n)
		out = collectRing(out, s, ids, maxAttempts, filterAvailable)
	}
	if len(out) > 0 {
		return out
	}
	for _, f := range ladder[1:] {
		if out = collectRing(out[:0], s, ids, maxAttempts, f); len(out) > 0 {
			return out
		}
	}
	// Reached only if the ring snapshot lags the backend snapshot — the instant between the two
	// stores in Rebuild. Losing preference order for that instant beats returning nothing.
	return collectRotation(out[:0], s.backends, 0, maxAttempts, filterAny)
}

// collectRing appends to out, skipping backends it already holds. Rejecting by identity rather than
// by position is what makes the result duplicate-free even across the two GetN calls in Pick, and it
// also stops an already-admitted backend from spending a second half-open breaker probe.
func collectRing(out []*pool.Backend, s *snapshot, ids []string, maxAttempts int, f pickFilter) []*pool.Backend {
	for _, id := range ids {
		bk, ok := s.byID[id]
		if !ok || slices.Contains(out, bk) || !f.admits(bk) {
			continue
		}
		out = append(out, bk)
		if len(out) == maxAttempts {
			break
		}
	}
	return out
}

// collectRotation walks the backends once from start, so the result holds no duplicates.
func collectRotation(out []*pool.Backend, backends []*pool.Backend, start, maxAttempts int, f pickFilter) []*pool.Backend {
	for i := range backends {
		bk := backends[(start+i)%len(backends)]
		if !f.admits(bk) {
			continue
		}
		out = append(out, bk)
		if len(out) == maxAttempts {
			break
		}
	}
	return out
}

func (b *Balancer) Ring() *hashring.Ring { return b.ring }

// Rebuild re-syncs ring membership from the pool. It is wired to pool.OnChange, which fires on
// every health flip, so the ring itself is rebuilt only when the configured backend set actually
// changed; the rest of the time this just refreshes the gauges.
func (b *Balancer) Rebuild() {
	if b.closed.Load() {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	backends := b.pool.Backends()
	if !sameMembership(b.snap.Load().backends, backends) {
		members := make([]hashring.Member, len(backends))
		byID := make(map[string]*pool.Backend, len(backends))
		for i, bk := range backends {
			members[i] = hashring.Member{ID: bk.ID(), Weight: bk.Weight()}
			byID[bk.ID()] = bk
		}
		b.ring.Set(members)
		b.snap.Store(&snapshot{backends: backends, byID: byID})
	}
	if b.m != nil {
		b.m.SetRingMembers(b.ring.Len())
		b.m.SetRingVirtualNodes(b.ring.VirtualNodes())
	}
}

// sameMembership compares by pointer: a Backend's id and weight are immutable, so identical
// pointers in identical order mean the ring is already correct.
func sameMembership(cur, next []*pool.Backend) bool {
	if len(cur) != len(next) {
		return false
	}
	for i := range next {
		if cur[i] != next[i] {
			return false
		}
	}
	return true
}

// Close stops the balancer from touching the ring or the gauges again. The pool owns the change
// callbacks and never unregisters them, so Rebuild must stay callable — and inert — afterwards.
// Pick keeps serving from the last snapshot rather than black-holing an in-flight request.
func (b *Balancer) Close() { b.closed.Store(true) }
