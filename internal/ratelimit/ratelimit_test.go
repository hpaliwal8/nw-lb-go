package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time                 { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration)        { c.ns.Add(int64(d)) }
func (c *fakeClock) cutoff(ttl time.Duration) int64 { return c.Now().Add(-ttl).UnixNano() }

// countAllowed drives n calls for key and reports how many were admitted.
func countAllowed(l *Limiter, key string, n int) int {
	allowed := 0
	for range n {
		if l.Allow(key) {
			allowed++
		}
	}
	return allowed
}

func TestAllowDisabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"zero config", Config{}},
		{"disabled with limits set", Config{RPS: 1, Burst: 1, PerClient: true, PerClientRPS: 1, PerClientBurst: 1}},
		{"disabled with ttl", Config{TTL: time.Millisecond, PerClient: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(tt.cfg)
			defer l.Close()

			if got := countAllowed(l, "client-a", 1000); got != 1000 {
				t.Fatalf("allowed = %d, want 1000", got)
			}
			if got := l.Clients(); got != 0 {
				t.Fatalf("Clients() = %d, want 0 (disabled limiter must not track clients)", got)
			}
		})
	}
}

func TestGlobalBurst(t *testing.T) {
	tests := []struct {
		name  string
		burst int
	}{
		{"burst 1", 1},
		{"burst 2", 2},
		{"burst 5", 5},
		{"burst 64", 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A refill of one token per 1000s cannot land inside this test.
			l := New(Config{Enabled: true, RPS: 0.001, Burst: tt.burst})
			defer l.Close()

			for i := range tt.burst {
				if !l.Allow("client-a") {
					t.Fatalf("call %d denied, want allowed within burst %d", i+1, tt.burst)
				}
			}
			if l.Allow("client-a") {
				t.Fatalf("call %d allowed, want denied past burst %d", tt.burst+1, tt.burst)
			}
		})
	}
}

func TestGlobalRefill(t *testing.T) {
	// One token per 10ms, capped at 1 by the burst: after a 30ms idle window exactly one call
	// should get through, and a second only if the two calls themselves straddle a refill.
	l := New(Config{Enabled: true, RPS: 100, Burst: 1})
	defer l.Close()

	if !l.Allow("client-a") {
		t.Fatal("first call denied, want allowed")
	}
	if l.Allow("client-a") {
		t.Fatal("second call allowed, want denied (burst exhausted)")
	}

	time.Sleep(30 * time.Millisecond)

	got := countAllowed(l, "client-a", 5)
	if got < 1 || got > 2 {
		t.Fatalf("allowed after refill window = %d, want 1..2", got)
	}
}

func TestGlobalDenialSkipsPerClientState(t *testing.T) {
	l := New(Config{
		Enabled: true, RPS: 0.001, Burst: 1,
		PerClient: true, PerClientRPS: 1000, PerClientBurst: 1000,
	})
	defer l.Close()

	if !l.Allow("client-a") {
		t.Fatal("first call denied, want allowed")
	}
	if got := l.Clients(); got != 1 {
		t.Fatalf("Clients() = %d, want 1", got)
	}
	if l.Allow("client-b") {
		t.Fatal("call allowed, want denied by the global bucket")
	}
	if got := l.Clients(); got != 1 {
		t.Fatalf("Clients() = %d, want 1: a globally denied request must not create a bucket", got)
	}
}

func TestPerClientIsolation(t *testing.T) {
	l := New(Config{
		Enabled: true, RPS: 0, Burst: 0, // unlimited global
		PerClient: true, PerClientRPS: 0.001, PerClientBurst: 3,
	})
	defer l.Close()

	if got := countAllowed(l, "client-a", 3); got != 3 {
		t.Fatalf("client-a allowed = %d, want 3", got)
	}
	if l.Allow("client-a") {
		t.Fatal("client-a allowed past its burst, want denied")
	}
	if got := countAllowed(l, "client-b", 3); got != 3 {
		t.Fatalf("client-b allowed = %d, want 3: exhausting client-a must not affect client-b", got)
	}
	if l.Allow("client-b") {
		t.Fatal("client-b allowed past its burst, want denied")
	}
	if got := l.Clients(); got != 2 {
		t.Fatalf("Clients() = %d, want 2", got)
	}
}

func TestClientsCountsDistinctKeysAcrossShards(t *testing.T) {
	l := New(Config{Enabled: true, PerClient: true})
	defer l.Close()

	const keys = 500
	for i := range keys {
		key := fmt.Sprintf("client-%d", i)
		if !l.Allow(key) {
			t.Fatalf("%s denied, want allowed with unlimited rates", key)
		}
		l.Allow(key) // repeat access must not add a second entry
	}
	if got := l.Clients(); got != keys {
		t.Fatalf("Clients() = %d, want %d", got, keys)
	}

	populated := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.RLock()
		if len(sh.m) > 0 {
			populated++
		}
		sh.mu.RUnlock()
	}
	if populated < shardCount/2 {
		t.Fatalf("only %d/%d shards populated, keys are not spreading", populated, shardCount)
	}
}

func TestSweepEviction(t *testing.T) {
	const ttl = time.Minute
	clk := newFakeClock()
	l := newWithClock(Config{Enabled: true, PerClient: true, TTL: ttl}, clk.Now)
	defer l.Close()

	tests := []struct {
		name    string
		advance time.Duration
		touch   []string
		want    int
	}{
		{"nothing idle yet", ttl / 2, nil, 3},
		{"idle past ttl", 2 * ttl, nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"a", "b", "c"} {
				if !l.Allow(k) {
					t.Fatalf("%s denied, want allowed", k)
				}
			}
			clk.Advance(tt.advance)
			for _, k := range tt.touch {
				l.Allow(k)
			}
			l.sweep(clk.cutoff(ttl))
			if got := l.Clients(); got != tt.want {
				t.Fatalf("Clients() after sweep = %d, want %d", got, tt.want)
			}
		})
	}

	// A key touched inside the window survives a sweep that evicts its idle neighbour.
	l.Allow("fresh")
	l.Allow("stale")
	clk.Advance(2 * ttl)
	l.Allow("fresh")
	l.sweep(clk.cutoff(ttl))
	if got := l.Clients(); got != 1 {
		t.Fatalf("Clients() = %d, want 1 (only the freshly touched key survives)", got)
	}
}

func TestJanitorEvicts(t *testing.T) {
	l := New(Config{Enabled: true, PerClient: true, TTL: 20 * time.Millisecond})
	defer l.Close()

	for i := range 10 {
		l.Allow(fmt.Sprintf("client-%d", i))
	}
	if got := l.Clients(); got != 10 {
		t.Fatalf("Clients() = %d, want 10", got)
	}

	deadline := time.Now().Add(5 * time.Second)
	for l.Clients() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("janitor did not evict idle entries: Clients() = %d", l.Clients())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDefaultTTL(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"zero defaults", 0, defaultTTL},
		{"negative defaults", -time.Second, defaultTTL},
		{"explicit kept", 90 * time.Second, 90 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(Config{TTL: tt.in})
			defer l.Close()
			if l.ttl != tt.want {
				t.Fatalf("ttl = %v, want %v", l.ttl, tt.want)
			}
		})
	}
}

func TestConcurrentAllow(t *testing.T) {
	l := New(Config{
		Enabled: true, PerClient: true, TTL: time.Minute,
	})
	defer l.Close()

	const (
		goroutines = 50
		keys       = 10
		iterations = 200
	)
	var denied atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range iterations {
				if !l.Allow(fmt.Sprintf("client-%d", (g+i)%keys)) {
					denied.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if n := denied.Load(); n != 0 {
		t.Fatalf("denied = %d, want 0 with unlimited rates", n)
	}
	if got := l.Clients(); got != keys {
		t.Fatalf("Clients() = %d, want %d: racing goroutines must share one bucket per key", got, keys)
	}
}

func TestCloseIdempotent(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"disabled", Config{}},
		{"enabled without per-client", Config{Enabled: true, RPS: 10, Burst: 10}},
		{"enabled with janitor", Config{Enabled: true, PerClient: true, TTL: 5 * time.Millisecond}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(tt.cfg)
			l.Allow("client-a")

			done := make(chan struct{})
			go func() {
				defer close(done)
				var wg sync.WaitGroup
				for range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						l.Close()
					}()
				}
				wg.Wait()
				l.Close()
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not return, want idempotent non-blocking close")
			}
		})
	}
}

func TestCloseDuringSweeps(t *testing.T) {
	// A 1ms TTL keeps the janitor sweeping continuously while traffic keeps taking shard locks;
	// Close must still return promptly.
	l := New(Config{Enabled: true, PerClient: true, TTL: time.Millisecond})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					l.Allow(fmt.Sprintf("client-%d-%d", g, i%64))
				}
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		l.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked while the janitor was sweeping")
	}

	close(stop)
	wg.Wait()
	l.Close()
}

func BenchmarkAllowHotKey(b *testing.B) {
	l := New(Config{Enabled: true, PerClient: true, TTL: time.Hour})
	defer l.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		l.Allow("hot-key")
	}
}

func BenchmarkAllowHotKeyParallel(b *testing.B) {
	l := New(Config{Enabled: true, PerClient: true, TTL: time.Hour})
	defer l.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow("hot-key")
		}
	})
}
