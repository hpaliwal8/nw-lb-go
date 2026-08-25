package balancer

import (
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
)

// startServers brings up n bare gRPC servers on loopback so the pool holds real connections, the
// way internal/pool's own tests do. No service is registered: the balancer never sends anything.
func startServers(tb testing.TB, n int) []string {
	tb.Helper()
	addrs := make([]string, n)
	for i := range n {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatalf("listen: %v", err)
		}
		srv := grpc.NewServer()
		go func() { _ = srv.Serve(lis) }()
		tb.Cleanup(srv.Stop)
		addrs[i] = lis.Addr().String()
	}
	return addrs
}

// testConfig gives every backend the same weight so distribution assertions are about the ring, not
// about weighting, and an open breaker stays open for the whole test.
func testConfig(tb testing.TB, n int, mutate ...func(*config.Config)) config.Config {
	tb.Helper()
	c := config.Default()
	c.CircuitBreaker = config.CircuitBreaker{
		Enabled:      true,
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  time.Minute,
		HalfOpenMax:  1,
	}
	for i, a := range startServers(tb, n) {
		c.Backends = append(c.Backends, config.Backend{
			ID:     fmt.Sprintf("b%d", i+1),
			Addr:   a,
			Weight: config.DefaultWeight,
		})
	}
	for _, f := range mutate {
		f(&c)
	}
	return c
}

func newBalancer(tb testing.TB, n int, mutate ...func(*config.Config)) (*Balancer, *pool.Pool, *metrics.Metrics) {
	tb.Helper()
	cfg := testConfig(tb, n, mutate...)
	m := metrics.New()
	p, err := pool.New(cfg, m)
	if err != nil {
		tb.Fatalf("pool.New: %v", err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	b := New(p, cfg.Routing, m)
	tb.Cleanup(b.Close)
	return b, p, m
}

func setAllStates(p *pool.Pool, s pool.State) {
	for _, b := range p.Backends() {
		b.SetState(s)
	}
	p.NotifyChange()
}

func setState(tb testing.TB, p *pool.Pool, id string, s pool.State) {
	tb.Helper()
	b, ok := p.Get(id)
	if !ok {
		tb.Fatalf("backend %q is not in the pool", id)
	}
	b.SetState(s)
	p.NotifyChange()
}

// trip drives a breaker open: two failures reach MinRequests at a 1.0 failure ratio.
func trip(tb testing.TB, b *pool.Backend) {
	tb.Helper()
	b.Breaker().Failure()
	b.Breaker().Failure()
	if got := b.Breaker().State(); got != circuit.StateOpen {
		tb.Fatalf("backend %s: breaker state = %v, want %v", b.ID(), got, circuit.StateOpen)
	}
}

func pickIDs(b *Balancer, key string, maxAttempts int) []string {
	cands := b.Pick(key, maxAttempts)
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.ID()
	}
	return out
}

func primary(tb testing.TB, b *Balancer, key string) string {
	tb.Helper()
	got := b.Pick(key, 1)
	if len(got) != 1 {
		tb.Fatalf("Pick(%q, 1) returned %d candidates, want 1", key, len(got))
	}
	return got[0].ID()
}

func assertDistinct(tb testing.TB, got []string) {
	tb.Helper()
	for i, id := range got {
		if slices.Index(got, id) != i {
			tb.Fatalf("duplicate backend %q in %v", id, got)
		}
	}
}

func gaugeValue(tb testing.TB, reg *prometheus.Registry, name string) float64 {
	tb.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		tb.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		ms := mf.GetMetric()
		if len(ms) != 1 {
			tb.Fatalf("%s has %d series, want 1", name, len(ms))
		}
		return ms[0].GetGauge().GetValue()
	}
	tb.Fatalf("%s is not exported", name)
	return 0
}

func TestPickIsDeterministicForTheSameKey(t *testing.T) {
	b, p, _ := newBalancer(t, 5)
	setAllStates(p, pool.StateHealthy)

	keys := []string{"session-1", "session-2", "user:42", "0", "a-much-longer-affinity-key"}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			want := pickIDs(b, key, 3)
			if len(want) != 3 {
				t.Fatalf("Pick(%q, 3) = %v, want 3 candidates", key, want)
			}
			assertDistinct(t, want)
			for i := range 200 {
				if got := pickIDs(b, key, 3); !slices.Equal(got, want) {
					t.Fatalf("call %d: Pick(%q, 3) = %v, want %v", i, key, got, want)
				}
			}
		})
	}
}

func TestPickFollowsRingPreferenceOrder(t *testing.T) {
	const n = 5
	b, p, _ := newBalancer(t, n)
	setAllStates(p, pool.StateHealthy)

	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		t.Run(key, func(t *testing.T) {
			want := b.Ring().GetN(key, n)
			if len(want) != n {
				t.Fatalf("ring returned %d ids, want %d", len(want), n)
			}
			if got := pickIDs(b, key, n); !slices.Equal(got, want) {
				t.Errorf("Pick(%q, %d) = %v, want ring order %v", key, n, got, want)
			}
		})
	}
}

func TestPickSkipsUnhealthyPrimaryAndReverts(t *testing.T) {
	const n = 5
	b, p, _ := newBalancer(t, n)

	for _, key := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		t.Run(key, func(t *testing.T) {
			setAllStates(p, pool.StateHealthy)
			order := b.Ring().GetN(key, n)

			if got := primary(t, b, key); got != order[0] {
				t.Fatalf("healthy primary = %s, want ring owner %s", got, order[0])
			}

			setState(t, p, order[0], pool.StateUnhealthy)
			if got := primary(t, b, key); got != order[1] {
				t.Errorf("primary with %s unhealthy = %s, want next ring member %s", order[0], got, order[1])
			}
			// The ring must not have shrunk: membership is configuration, not health.
			if got := b.Ring().Len(); got != n {
				t.Errorf("ring members during outage = %d, want %d", got, n)
			}

			setState(t, p, order[0], pool.StateHealthy)
			if got := primary(t, b, key); got != order[0] {
				t.Errorf("primary after recovery = %s, want %s", got, order[0])
			}
		})
	}
}

// TestPickFillsFailoverSlotsFromDeeperInTheRing pins the property the two-step rung-one scan in
// Pick must preserve: the candidates are the first maxAttempts *available* members of the full
// preference order, not the available subset of its first maxAttempts members.
func TestPickFillsFailoverSlotsFromDeeperInTheRing(t *testing.T) {
	const (
		n           = 6
		key         = "deep-key"
		maxAttempts = 3
	)
	b, p, _ := newBalancer(t, n)
	order := b.Ring().GetN(key, n)

	tests := []struct {
		name        string
		unhealthy   []int // indexes into the ring preference order
		openBreaker []int
		want        []int
	}{
		{name: "all available", want: []int{0, 1, 2}},
		{name: "owner down", unhealthy: []int{0}, want: []int{1, 2, 3}},
		{name: "owner and second down", unhealthy: []int{0, 1}, want: []int{2, 3, 4}},
		{name: "alternating", unhealthy: []int{0, 2}, want: []int{1, 3, 4}},
		{name: "breaker open on the owner", openBreaker: []int{0}, want: []int{1, 2, 3}},
		{name: "mixed health and breakers", unhealthy: []int{1}, openBreaker: []int{0, 3}, want: []int{2, 4, 5}},
		{name: "only three left", unhealthy: []int{0, 1, 2}, want: []int{3, 4, 5}},
		{name: "only two left", unhealthy: []int{0, 1, 2, 3}, want: []int{4, 5}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, bk := range p.Backends() {
				bk.SetState(pool.StateHealthy)
				bk.Breaker().Reset()
			}
			for _, i := range tc.unhealthy {
				setState(t, p, order[i], pool.StateUnhealthy)
			}
			for _, i := range tc.openBreaker {
				bk, ok := p.Get(order[i])
				if !ok {
					t.Fatalf("backend %q missing from the pool", order[i])
				}
				trip(t, bk)
			}
			p.NotifyChange()

			want := make([]string, len(tc.want))
			for i, idx := range tc.want {
				want[i] = order[idx]
			}
			got := pickIDs(b, key, maxAttempts)
			assertDistinct(t, got)
			if !slices.Equal(got, want) {
				t.Errorf("Pick(%q, %d) = %v, want %v (ring order %v)", key, maxAttempts, got, want, order)
			}
		})
	}
}

// TestUnhealthyBackendDoesNotRemapOtherKeys is the whole point of filtering instead of re-ringing:
// taking one backend out must move only the keys it owned.
func TestUnhealthyBackendDoesNotRemapOtherKeys(t *testing.T) {
	const (
		n     = 5
		nKeys = 10000
		down  = "b3"
	)
	b, p, _ := newBalancer(t, n)
	setAllStates(p, pool.StateHealthy)

	keys := make([]string, nKeys)
	before := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		before[i] = primary(t, b, keys[i])
	}

	setState(t, p, down, pool.StateUnhealthy)

	owned := 0
	for i, k := range keys {
		after := primary(t, b, k)
		if before[i] == down {
			owned++
			if after == down {
				t.Fatalf("key %q still routes to the unhealthy backend %s", k, down)
			}
			continue
		}
		if after != before[i] {
			t.Fatalf("key %q remapped from %s to %s when %s went unhealthy", k, before[i], after, down)
		}
	}
	if owned == 0 {
		t.Fatalf("no key was owned by %s; the test proves nothing", down)
	}
	if got := b.Ring().Len(); got != n {
		t.Errorf("ring members = %d, want %d (health must not change membership)", got, n)
	}

	setState(t, p, down, pool.StateHealthy)
	for i, k := range keys {
		if got := primary(t, b, k); got != before[i] {
			t.Fatalf("key %q = %s after recovery, want its original owner %s", k, got, before[i])
		}
	}
}

func TestPickSkipsOpenBreakersAndFallsBackWhenAllOpen(t *testing.T) {
	const (
		n   = 4
		key = "breaker-key"
	)
	b, p, _ := newBalancer(t, n)
	setAllStates(p, pool.StateHealthy)
	order := b.Ring().GetN(key, n)

	first, ok := p.Get(order[0])
	if !ok {
		t.Fatalf("backend %q missing from the pool", order[0])
	}
	trip(t, first)
	if got := primary(t, b, key); got != order[1] {
		t.Errorf("primary with %s's breaker open = %s, want %s", order[0], got, order[1])
	}

	for _, bk := range p.Backends() {
		if bk.Breaker().State() == circuit.StateClosed {
			trip(t, bk)
		}
	}
	// Every breaker is open: the healthy rung ignores the breaker and restores ring order rather
	// than returning nothing.
	if got := pickIDs(b, key, n); !slices.Equal(got, order) {
		t.Errorf("Pick with every breaker open = %v, want the full ring order %v", got, order)
	}

	first.Breaker().Reset()
	if got := primary(t, b, key); got != order[0] {
		t.Errorf("primary after breaker reset = %s, want %s", got, order[0])
	}
}

func TestPickFailsOpenWhenNothingIsHealthy(t *testing.T) {
	const (
		n   = 4
		key = "fail-open-key"
	)
	b, p, _ := newBalancer(t, n)
	order := b.Ring().GetN(key, n)

	tests := []struct {
		name         string
		state        pool.State
		tripBreakers bool
	}{
		{name: "never probed", state: pool.StateUnknown},
		{name: "all unhealthy", state: pool.StateUnhealthy},
		{name: "all unhealthy and every breaker open", state: pool.StateUnhealthy, tripBreakers: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setAllStates(p, tc.state)
			if tc.tripBreakers {
				for _, bk := range p.Backends() {
					if bk.Breaker().State() == circuit.StateClosed {
						trip(t, bk)
					}
				}
			}
			got := pickIDs(b, key, 2)
			if want := order[:2]; !slices.Equal(got, want) {
				t.Errorf("Pick(%q, 2) = %v, want %v (fail open, in ring order)", key, got, want)
			}
			if gotEmpty := pickIDs(b, "", 2); len(gotEmpty) != 2 {
				t.Errorf("Pick(\"\", 2) = %v, want 2 candidates", gotEmpty)
			}
		})
	}
}

// TestPickEmptyKeyRoundRobins pins the bug this avoids: hashing "" would send every request without
// an affinity key to a single backend.
func TestPickEmptyKeyRoundRobins(t *testing.T) {
	const n = 4
	b, p, _ := newBalancer(t, n)
	setAllStates(p, pool.StateHealthy)

	counts := make(map[string]int, n)
	const rounds = 100
	for range rounds * n {
		counts[primary(t, b, "")]++
	}
	for _, bk := range p.Backends() {
		if got := counts[bk.ID()]; got != rounds {
			t.Errorf("backend %s served %d keyless requests, want %d (counts: %v)", bk.ID(), got, rounds, counts)
		}
	}

	// The empty key must not collide with the hash of "", which would pin everything to one owner.
	if owner, _ := b.Ring().Get(""); counts[owner] == rounds*n {
		t.Errorf("every keyless request went to %s, the hash owner of the empty string", owner)
	}
}

func TestPickEmptyKeySkipsUnavailableBackends(t *testing.T) {
	const n = 4
	b, p, _ := newBalancer(t, n)
	setAllStates(p, pool.StateHealthy)
	setState(t, p, "b2", pool.StateUnhealthy)

	counts := make(map[string]int, n)
	for range 400 {
		got := pickIDs(b, "", 2)
		if len(got) != 2 {
			t.Fatalf("Pick(\"\", 2) = %v, want 2 candidates", got)
		}
		assertDistinct(t, got)
		counts[got[0]]++
	}
	// The rotation visits b2's slot and moves on to the next backend, so b3 takes both turns.
	want := map[string]int{"b1": 100, "b3": 200, "b4": 100}
	for id, wantCount := range want {
		if counts[id] != wantCount {
			t.Errorf("backend %s was primary %d times, want %d (counts: %v)", id, counts[id], wantCount, counts)
		}
	}
	if counts["b2"] != 0 {
		t.Errorf("unhealthy backend b2 was primary %d times, want 0", counts["b2"])
	}
}

func TestPickMaxAttempts(t *testing.T) {
	const (
		n   = 4
		key = "cap-key"
	)
	b, p, _ := newBalancer(t, n)
	setAllStates(p, pool.StateHealthy)
	order := b.Ring().GetN(key, n)

	tests := []struct {
		name        string
		maxAttempts int
		wantLen     int
	}{
		{name: "negative is one attempt", maxAttempts: -3, wantLen: 1},
		{name: "zero is one attempt", maxAttempts: 0, wantLen: 1},
		{name: "one", maxAttempts: 1, wantLen: 1},
		{name: "two", maxAttempts: 2, wantLen: 2},
		{name: "every backend", maxAttempts: n, wantLen: n},
		{name: "more than the pool holds", maxAttempts: 10, wantLen: n},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pickIDs(b, key, tc.maxAttempts)
			if len(got) != tc.wantLen {
				t.Fatalf("Pick(%q, %d) = %v, want %d candidates", key, tc.maxAttempts, got, tc.wantLen)
			}
			assertDistinct(t, got)
			if want := order[:tc.wantLen]; !slices.Equal(got, want) {
				t.Errorf("Pick(%q, %d) = %v, want %v", key, tc.maxAttempts, got, want)
			}

			gotEmpty := pickIDs(b, "", tc.maxAttempts)
			if len(gotEmpty) != tc.wantLen {
				t.Errorf("Pick(\"\", %d) = %v, want %d candidates", tc.maxAttempts, gotEmpty, tc.wantLen)
			}
			assertDistinct(t, gotEmpty)
		})
	}
}

func TestPickHasNoDuplicatesUnderMixedStates(t *testing.T) {
	const n = 6
	b, p, _ := newBalancer(t, n)
	backends := p.Backends()

	tests := []struct {
		name    string
		healthy []int
		open    []int
	}{
		{name: "all healthy", healthy: []int{0, 1, 2, 3, 4, 5}},
		{name: "half healthy", healthy: []int{0, 2, 4}},
		{name: "one healthy", healthy: []int{3}},
		{name: "healthy but every breaker open", healthy: []int{0, 1, 2, 3, 4, 5}, open: []int{0, 1, 2, 3, 4, 5}},
		{name: "none healthy", healthy: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, bk := range backends {
				bk.SetState(pool.StateUnhealthy)
				bk.Breaker().Reset()
			}
			for _, i := range tc.healthy {
				backends[i].SetState(pool.StateHealthy)
			}
			for _, i := range tc.open {
				trip(t, backends[i])
			}
			p.NotifyChange()

			for i := range 500 {
				for _, key := range []string{fmt.Sprintf("dup-%d", i), ""} {
					got := pickIDs(b, key, n)
					if len(got) == 0 {
						t.Fatalf("Pick(%q, %d) returned nothing while %d backends exist", key, n, n)
					}
					assertDistinct(t, got)
				}
			}
		})
	}
}

func TestPickOnEmptyPool(t *testing.T) {
	cfg := config.Default()
	m := metrics.New()
	p, err := pool.New(cfg, m)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	b := New(p, cfg.Routing, m)
	t.Cleanup(b.Close)

	for _, key := range []string{"", "anything"} {
		if got := b.Pick(key, 3); got != nil {
			t.Errorf("Pick(%q, 3) on an empty pool = %v, want nil", key, got)
		}
	}
	if got := b.Ring().Len(); got != 0 {
		t.Errorf("ring members = %d, want 0", got)
	}
	if got := gaugeValue(t, m.Registry(), "lb_ring_members"); got != 0 {
		t.Errorf("lb_ring_members = %v, want 0", got)
	}
}

func TestRingHoldsEveryBackendAndPublishesGauges(t *testing.T) {
	const n = 4
	b, p, m := newBalancer(t, n)

	want := []string{"b1", "b2", "b3", "b4"}
	members := b.Ring().Members()
	got := make([]string, len(members))
	for i, mem := range members {
		got[i] = mem.ID
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ring members = %v, want %v", got, want)
	}
	if v := gaugeValue(t, m.Registry(), "lb_ring_members"); v != float64(n) {
		t.Errorf("lb_ring_members = %v, want %d", v, n)
	}
	if v, want := gaugeValue(t, m.Registry(), "lb_ring_virtual_nodes"), float64(b.Ring().VirtualNodes()); v != want {
		t.Errorf("lb_ring_virtual_nodes = %v, want %v", v, want)
	}

	// A health flip fires OnChange, which must refresh gauges without disturbing membership.
	setAllStates(p, pool.StateUnhealthy)
	if after := b.Ring().Members(); !slices.Equal(after, members) {
		t.Errorf("ring members after a health flip = %v, want %v", after, members)
	}
	if v := gaugeValue(t, m.Registry(), "lb_ring_members"); v != float64(n) {
		t.Errorf("lb_ring_members after a health flip = %v, want %d", v, n)
	}

	b.Rebuild()
	if after := b.Ring().Members(); !slices.Equal(after, members) {
		t.Errorf("ring members after an explicit Rebuild = %v, want %v", after, members)
	}
}

func TestWeightsReachTheRing(t *testing.T) {
	b, p, _ := newBalancer(t, 2, func(c *config.Config) {
		c.Backends[0].Weight = 100
		c.Backends[1].Weight = 300
	})
	setAllStates(p, pool.StateHealthy)

	if got, want := b.Ring().VirtualNodes(), 200+600; got != want {
		t.Fatalf("ring points = %d, want %d", got, want)
	}

	counts := map[string]int{}
	const nKeys = 20000
	for i := range nKeys {
		counts[primary(t, b, fmt.Sprintf("w-%d", i))]++
	}
	if counts["b1"] == 0 {
		t.Fatal("b1 owns no keys")
	}
	ratio := float64(counts["b2"]) / float64(counts["b1"])
	if ratio < 2 || ratio > 4.5 {
		t.Errorf("b2:b1 key share = %.2f (counts %v), want roughly the 3:1 weight ratio", ratio, counts)
	}
}

func TestVirtualNodesFallsBackToTheDefault(t *testing.T) {
	tests := []struct {
		name         string
		virtualNodes int
		want         int
	}{
		{name: "zero", virtualNodes: 0, want: config.Default().Routing.VirtualNodes},
		{name: "negative", virtualNodes: -5, want: config.Default().Routing.VirtualNodes},
		{name: "configured", virtualNodes: 32, want: 32},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, _, _ := newBalancer(t, 3, func(c *config.Config) { c.Routing.VirtualNodes = tc.virtualNodes })
			if got, want := b.Ring().VirtualNodes(), tc.want*3; got != want {
				t.Errorf("ring points = %d, want %d", got, want)
			}
		})
	}
}

func TestNilMetricsIsTolerated(t *testing.T) {
	cfg := testConfig(t, 3)
	p, err := pool.New(cfg, nil)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	b := New(p, cfg.Routing, nil)
	t.Cleanup(b.Close)
	setAllStates(p, pool.StateHealthy)

	if got := pickIDs(b, "nil-metrics", 2); len(got) != 2 {
		t.Errorf("Pick = %v, want 2 candidates", got)
	}
}

func TestSameMembership(t *testing.T) {
	_, p, _ := newBalancer(t, 3)
	all := p.Backends()

	tests := []struct {
		name string
		cur  []*pool.Backend
		next []*pool.Backend
		want bool
	}{
		{name: "identical slice", cur: all, next: all, want: true},
		{name: "equal copy", cur: all, next: slices.Clone(all), want: true},
		{name: "both empty", want: true},
		{name: "grown", cur: all[:2], next: all},
		{name: "shrunk", cur: all, next: all[:2]},
		{name: "reordered", cur: all, next: []*pool.Backend{all[1], all[0], all[2]}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameMembership(tc.cur, tc.next); got != tc.want {
				t.Errorf("sameMembership() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCloseIsIdempotentAndPickKeepsServing(t *testing.T) {
	b, p, _ := newBalancer(t, 3)
	setAllStates(p, pool.StateHealthy)
	before := pickIDs(b, "close-key", 2)

	for range 3 {
		b.Close()
	}
	// The pool never unregisters callbacks, so a notify after Close must be inert, not a panic.
	p.NotifyChange()

	if got := pickIDs(b, "close-key", 2); !slices.Equal(got, before) {
		t.Errorf("Pick after Close = %v, want %v", got, before)
	}
}

func TestConcurrentPickWhileStatesFlip(t *testing.T) {
	const (
		n          = 5
		iterations = 2000
		attempts   = 3
	)
	b, p, _ := newBalancer(t, n)
	setAllStates(p, pool.StateHealthy)

	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				key := ""
				if (i+g)%2 == 0 {
					key = fmt.Sprintf("k-%d", i)
				}
				got := b.Pick(key, attempts)
				if len(got) == 0 || len(got) > attempts {
					t.Errorf("Pick(%q, %d) returned %d candidates", key, attempts, len(got))
					return
				}
				for x := range got {
					for y := x + 1; y < len(got); y++ {
						if got[x] == got[y] {
							t.Errorf("Pick(%q, %d) returned %s twice", key, attempts, got[x].ID())
							return
						}
					}
				}
			}
		}()
	}
	for i, bk := range p.Backends() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range iterations {
				s := pool.StateHealthy
				if (i+j)%3 == 0 {
					s = pool.StateUnhealthy
				}
				if bk.SetState(s) {
					p.NotifyChange()
				}
				switch j % 5 {
				case 0:
					bk.Breaker().Failure()
				case 3:
					bk.Breaker().Reset()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iterations {
			b.Rebuild()
			if got := b.Ring().Len(); got != n {
				t.Errorf("ring members = %d, want %d", got, n)
				return
			}
		}
	}()
	wg.Wait()
}

func BenchmarkPick(b *testing.B) {
	bal, p, _ := newBalancer(b, 8)
	setAllStates(p, pool.StateHealthy)

	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("session-%d", i)
	}

	cases := []struct {
		name     string
		keyed    bool
		attempts int
	}{
		{name: "keyed/attempts=1", keyed: true, attempts: 1},
		{name: "keyed/attempts=3", keyed: true, attempts: 3},
		{name: "keyless/attempts=3", attempts: 3},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				key := ""
				if tc.keyed {
					key = keys[i&1023]
				}
				if got := bal.Pick(key, tc.attempts); len(got) != tc.attempts {
					b.Fatalf("Pick returned %d candidates, want %d", len(got), tc.attempts)
				}
			}
		})
	}
}
