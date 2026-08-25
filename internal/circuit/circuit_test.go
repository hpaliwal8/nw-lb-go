package circuit

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// recorder collects state changes; it is safe to call from OnStateChange.
type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *recorder) hook(name string, from, to State) {
	r.mu.Lock()
	r.seen = append(r.seen, fmt.Sprintf("%s:%s->%s", name, from, to))
	r.mu.Unlock()
}

func (r *recorder) events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

// apply feeds n outcomes to b.
func apply(b *Breaker, failures, successes int) {
	for range failures {
		b.Failure()
	}
	for range successes {
		b.Success()
	}
}

// tripClosed drives a closed breaker to open using its own settings.
func tripClosed(t *testing.T, b *Breaker) {
	t.Helper()
	for range b.set.MinRequests {
		b.Failure()
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("breaker did not trip: state = %v", got)
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(9), "unknown(9)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.state.String(); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSettingsNormalize(t *testing.T) {
	tests := []struct {
		name string
		in   Settings
		want Settings
	}{
		{
			name: "zero value gets defaults",
			in:   Settings{},
			want: Settings{Window: time.Second, Buckets: 1, MinRequests: 1, FailureRatio: 0.5, HalfOpenMax: 1},
		},
		{
			name: "negative values get defaults",
			in:   Settings{Window: -1, Buckets: -4, MinRequests: -2, FailureRatio: -0.3, HalfOpenMax: -7},
			want: Settings{Window: time.Second, Buckets: 1, MinRequests: 1, FailureRatio: 0.5, HalfOpenMax: 1},
		},
		{
			name: "valid values are preserved",
			in:   Settings{Window: 4 * time.Second, Buckets: 8, MinRequests: 20, FailureRatio: 0.9, OpenTimeout: time.Minute, HalfOpenMax: 3},
			want: Settings{Window: 4 * time.Second, Buckets: 8, MinRequests: 20, FailureRatio: 0.9, OpenTimeout: time.Minute, HalfOpenMax: 3},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in.normalize()
			if got.Window != tc.want.Window || got.Buckets != tc.want.Buckets ||
				got.MinRequests != tc.want.MinRequests || got.FailureRatio != tc.want.FailureRatio ||
				got.OpenTimeout != tc.want.OpenTimeout || got.HalfOpenMax != tc.want.HalfOpenMax {
				t.Errorf("normalize() = %+v, want %+v", got, tc.want)
			}
			if got.Now == nil {
				t.Error("normalize() left Now nil")
			}
		})
	}
}

func TestZeroSettingsBreakerIsUsable(t *testing.T) {
	clock := newFakeClock()
	rec := &recorder{}
	b := NewBreaker("zero", Settings{Now: clock.Now, OnStateChange: rec.hook})
	if _, ok := b.Admit(); !ok {
		t.Fatal("Admit() = false on a fresh breaker")
	}
	// Defaults: MinRequests 1, FailureRatio 0.5 => a single failure trips.
	b.Failure()
	if req, fail := b.Counts(); req != 1 || fail != 1 {
		t.Fatalf("Counts() = (%d, %d), want (1, 1)", req, fail)
	}
	// OpenTimeout is not defaulted, so a zero-value breaker leaves the open state as soon as it is
	// observed. The trip itself must still be reported.
	if got := rec.events(); len(got) == 0 || got[0] != "zero:closed->open" {
		t.Fatalf("transitions = %v, want the first to be zero:closed->open", got)
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v, want %v with OpenTimeout 0", got, StateHalfOpen)
	}
}

func TestClosedTripsOnRatio(t *testing.T) {
	tests := []struct {
		name      string
		min       int
		ratio     float64
		failures  int
		successes int
		wantState State
	}{
		{name: "exactly at ratio and min", min: 4, ratio: 0.5, failures: 2, successes: 2, wantState: StateOpen},
		{name: "above ratio", min: 4, ratio: 0.5, failures: 3, successes: 1, wantState: StateOpen},
		{name: "below ratio", min: 4, ratio: 0.5, failures: 1, successes: 3, wantState: StateClosed},
		{name: "all failures but below min requests", min: 5, ratio: 0.5, failures: 4, wantState: StateClosed},
		{name: "min requests reached by a success", min: 4, ratio: 0.5, failures: 3, successes: 1, wantState: StateOpen},
		{name: "ratio 1 requires every request to fail", min: 3, ratio: 1, failures: 2, successes: 1, wantState: StateClosed},
		{name: "ratio 1 all failed", min: 3, ratio: 1, failures: 3, wantState: StateOpen},
		{name: "no traffic stays closed", min: 1, ratio: 0.5, wantState: StateClosed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			b := NewBreaker("b", Settings{
				Window:       time.Second,
				Buckets:      10,
				MinRequests:  tc.min,
				FailureRatio: tc.ratio,
				OpenTimeout:  time.Second,
				HalfOpenMax:  1,
				Now:          clock.Now,
			})
			apply(b, tc.failures, tc.successes)
			if got := b.State(); got != tc.wantState {
				t.Errorf("State() = %v, want %v", got, tc.wantState)
			}
			if tc.wantState == StateClosed {
				if _, ok := b.Admit(); !ok {
					t.Error("Admit() = false while closed")
				}
			}
		})
	}
}

func TestRollingWindow(t *testing.T) {
	type step struct {
		advance   time.Duration
		failures  int
		successes int
	}
	// Window 1s over 10 buckets => 100ms per bucket.
	tests := []struct {
		name         string
		steps        []step
		wantRequests int
		wantFailures int
		wantState    State
	}{
		{
			name:         "everything inside the window counts",
			steps:        []step{{failures: 2}, {advance: 300 * time.Millisecond, successes: 2}},
			wantRequests: 4,
			wantFailures: 2,
			wantState:    StateOpen,
		},
		{
			name:         "old failures expire bucket by bucket",
			steps:        []step{{failures: 3}, {advance: 1100 * time.Millisecond, successes: 1}},
			wantRequests: 1,
			wantFailures: 0,
			wantState:    StateClosed,
		},
		{
			name: "expired failures cannot trip with fresh traffic",
			steps: []step{
				{failures: 3},
				{advance: 1100 * time.Millisecond, failures: 1, successes: 3},
			},
			wantRequests: 4,
			wantFailures: 1,
			wantState:    StateClosed,
		},
		{
			name: "partial expiry retires only the oldest buckets",
			steps: []step{
				{failures: 1},
				{advance: 500 * time.Millisecond, successes: 2},
				{advance: 600 * time.Millisecond},
			},
			wantRequests: 2,
			wantFailures: 0,
			wantState:    StateClosed,
		},
		{
			name: "gap far beyond the window clears everything",
			steps: []step{
				{failures: 3},
				{advance: time.Hour, successes: 1},
			},
			wantRequests: 1,
			wantFailures: 0,
			wantState:    StateClosed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			b := NewBreaker("b", Settings{
				Window:       time.Second,
				Buckets:      10,
				MinRequests:  4,
				FailureRatio: 0.5,
				OpenTimeout:  time.Minute,
				HalfOpenMax:  1,
				Now:          clock.Now,
			})
			for _, s := range tc.steps {
				clock.Advance(s.advance)
				apply(b, s.failures, s.successes)
			}
			gotReq, gotFail := b.Counts()
			if gotReq != tc.wantRequests || gotFail != tc.wantFailures {
				t.Errorf("Counts() = (%d, %d), want (%d, %d)", gotReq, gotFail, tc.wantRequests, tc.wantFailures)
			}
			if got := b.State(); got != tc.wantState {
				t.Errorf("State() = %v, want %v", got, tc.wantState)
			}
		})
	}
}

func TestOpenRejectsUntilTimeout(t *testing.T) {
	tests := []struct {
		name string
		// poll selects which accessor drives the open -> half-open transition.
		poll func(*Breaker) State
	}{
		{name: "observed via State", poll: func(b *Breaker) State { return b.State() }},
		{
			name: "observed via Admit",
			poll: func(b *Breaker) State {
				b.Admit()
				return b.State()
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			b := NewBreaker("b", Settings{
				Window:       time.Second,
				Buckets:      10,
				MinRequests:  2,
				FailureRatio: 0.5,
				OpenTimeout:  5 * time.Second,
				HalfOpenMax:  2,
				Now:          clock.Now,
			})
			tripClosed(t, b)
			if _, ok := b.Admit(); ok {
				t.Fatal("Admit() = true while open")
			}

			clock.Advance(5*time.Second - time.Nanosecond)
			if got := tc.poll(b); got != StateOpen {
				t.Fatalf("state before OpenTimeout = %v, want %v", got, StateOpen)
			}

			clock.Advance(time.Nanosecond)
			if got := tc.poll(b); got != StateHalfOpen {
				t.Fatalf("state after OpenTimeout = %v, want %v", got, StateHalfOpen)
			}
			if _, ok := b.Admit(); !ok {
				t.Error("Admit() = false in half-open with no outstanding probes")
			}
		})
	}
}

func TestHalfOpenProbeCap(t *testing.T) {
	tests := []struct {
		name        string
		halfOpenMax int
	}{
		{name: "one probe", halfOpenMax: 1},
		{name: "three probes", halfOpenMax: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			b := NewBreaker("b", Settings{
				Window:       time.Second,
				Buckets:      10,
				MinRequests:  2,
				FailureRatio: 0.5,
				OpenTimeout:  time.Second,
				HalfOpenMax:  tc.halfOpenMax,
				Now:          clock.Now,
			})
			tripClosed(t, b)
			clock.Advance(time.Second)

			probes := make([]Probe, 0, tc.halfOpenMax)
			for i := range tc.halfOpenMax {
				p, ok := b.Admit()
				if !ok {
					t.Fatalf("probe %d rejected, want admitted", i)
				}
				probes = append(probes, p)
			}
			if _, ok := b.Admit(); ok {
				t.Fatal("probe beyond HalfOpenMax admitted")
			}
			if got := b.State(); got != StateHalfOpen {
				t.Fatalf("State() = %v, want %v", got, StateHalfOpen)
			}

			// Resolving one outstanding probe frees a slot (unless that success closed it).
			probes[0].Success()
			if tc.halfOpenMax > 1 {
				if got := b.State(); got != StateHalfOpen {
					t.Fatalf("State() = %v after one success, want %v", got, StateHalfOpen)
				}
				if _, ok := b.Admit(); !ok {
					t.Error("Admit() = false after a probe slot was freed")
				}
			} else if got := b.State(); got != StateClosed {
				t.Fatalf("State() = %v, want %v", got, StateClosed)
			}
		})
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	clock := newFakeClock()
	b := NewBreaker("b", Settings{
		Window:       time.Second,
		Buckets:      10,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  5 * time.Second,
		HalfOpenMax:  3,
		Now:          clock.Now,
	})
	tripClosed(t, b)
	clock.Advance(5 * time.Second)

	if _, ok := b.Admit(); !ok {
		t.Fatal("first probe rejected")
	}
	p2, ok := b.Admit()
	if !ok {
		t.Fatal("second probe rejected")
	}
	p2.Failure()

	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %v after half-open failure, want %v", got, StateOpen)
	}
	if _, ok := b.Admit(); ok {
		t.Fatal("Admit() = true right after reopening")
	}

	// The open timer restarted at the failure, not at the original trip.
	clock.Advance(5*time.Second - time.Nanosecond)
	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %v before the restarted OpenTimeout, want %v", got, StateOpen)
	}
	clock.Advance(time.Nanosecond)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v after the restarted OpenTimeout, want %v", got, StateHalfOpen)
	}
}

func TestHalfOpenSuccessesCloseAndClearCounts(t *testing.T) {
	clock := newFakeClock()
	b := NewBreaker("b", Settings{
		Window:       time.Minute,
		Buckets:      6,
		MinRequests:  4,
		FailureRatio: 0.5,
		OpenTimeout:  time.Second,
		HalfOpenMax:  2,
		Now:          clock.Now,
	})
	apply(b, 4, 0)
	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %v, want %v", got, StateOpen)
	}
	clock.Advance(time.Second)

	first, ok1 := b.Admit()
	second, ok2 := b.Admit()
	if !ok1 || !ok2 {
		t.Fatal("half-open probes rejected")
	}
	first.Success()
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v after 1 of 2 successes, want %v", got, StateHalfOpen)
	}
	second.Success()
	if got := b.State(); got != StateClosed {
		t.Fatalf("State() = %v after HalfOpenMax successes, want %v", got, StateClosed)
	}

	// The failures that tripped the breaker are still inside the window, so closing must clear
	// them or the next request would retrip immediately.
	if req, fail := b.Counts(); req != 0 || fail != 0 {
		t.Fatalf("Counts() = (%d, %d) after closing, want (0, 0)", req, fail)
	}
	apply(b, 1, 3)
	if got := b.State(); got != StateClosed {
		t.Errorf("State() = %v after healthy traffic, want %v", got, StateClosed)
	}
}

func TestOnStateChange(t *testing.T) {
	clock := newFakeClock()
	rec := &recorder{}
	b := NewBreaker("upstream-1", Settings{
		Window:        time.Minute,
		Buckets:       6,
		MinRequests:   2,
		FailureRatio:  0.5,
		OpenTimeout:   time.Second,
		HalfOpenMax:   1,
		Now:           clock.Now,
		OnStateChange: rec.hook,
	})

	apply(b, 2, 0)                  // closed -> open
	clock.Advance(time.Second)      //
	reopen, _ := b.Admit()          // open -> half-open
	reopen.Failure()                // half-open -> open
	clock.Advance(time.Second)      //
	if b.State() != StateHalfOpen { // open -> half-open
		t.Fatalf("State() = %v, want %v", b.State(), StateHalfOpen)
	}
	recover, _ := b.Admit()
	recover.Success() // half-open -> closed

	want := []string{
		"upstream-1:closed->open",
		"upstream-1:open->half-open",
		"upstream-1:half-open->open",
		"upstream-1:open->half-open",
		"upstream-1:half-open->closed",
	}
	got := rec.events()
	if len(got) != len(want) {
		t.Fatalf("transitions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("transition %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestOnStateChangeCallbackMayTouchBreaker(t *testing.T) {
	clock := newFakeClock()
	var observed []State
	var b *Breaker
	b = NewBreaker("reentrant", Settings{
		Window:       time.Second,
		Buckets:      4,
		MinRequests:  1,
		FailureRatio: 0.5,
		OpenTimeout:  time.Second,
		HalfOpenMax:  1,
		Now:          clock.Now,
		OnStateChange: func(_ string, _, _ State) {
			// Would deadlock if the callback ran while the breaker's mutex was held.
			observed = append(observed, b.State())
			b.Counts()
		},
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Failure()
		clock.Advance(time.Second)
		b.Admit()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("OnStateChange deadlocked against the breaker mutex")
	}

	if len(observed) != 2 {
		t.Fatalf("observed = %v, want 2 entries", observed)
	}
	if observed[0] != StateOpen || observed[1] != StateHalfOpen {
		t.Errorf("observed = %v, want [open half-open]", observed)
	}
}

func TestReset(t *testing.T) {
	clock := newFakeClock()
	rec := &recorder{}
	b := NewBreaker("b", Settings{
		Window:        time.Minute,
		Buckets:       6,
		MinRequests:   2,
		FailureRatio:  0.5,
		OpenTimeout:   time.Minute,
		HalfOpenMax:   1,
		Now:           clock.Now,
		OnStateChange: rec.hook,
	})
	tripClosed(t, b)

	b.Reset()
	if got := b.State(); got != StateClosed {
		t.Fatalf("State() = %v after Reset, want %v", got, StateClosed)
	}
	if req, fail := b.Counts(); req != 0 || fail != 0 {
		t.Fatalf("Counts() = (%d, %d) after Reset, want (0, 0)", req, fail)
	}
	if _, ok := b.Admit(); !ok {
		t.Error("Admit() = false after Reset")
	}
	want := []string{"b:closed->open", "b:open->closed"}
	if got := rec.events(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("transitions = %v, want %v", got, want)
	}

	// Reset on an already closed breaker is a no-op transition.
	b.Reset()
	if got := rec.events(); len(got) != 2 {
		t.Errorf("transitions = %v, want no new transition", got)
	}
}

func TestBreakerName(t *testing.T) {
	b := NewBreaker("backend-a", Settings{})
	if got := b.Name(); got != "backend-a" {
		t.Errorf("Name() = %q, want %q", got, "backend-a")
	}
}

func TestRegistryGetCreatesOnMiss(t *testing.T) {
	clock := newFakeClock()
	r := NewRegistry(Settings{
		Window:       time.Second,
		Buckets:      4,
		MinRequests:  1,
		FailureRatio: 0.5,
		OpenTimeout:  time.Second,
		HalfOpenMax:  1,
		Now:          clock.Now,
	})

	a1 := r.Get("a")
	a2 := r.Get("a")
	bb := r.Get("b")
	if a1 != a2 {
		t.Error("Get(\"a\") returned different breakers")
	}
	if a1 == bb {
		t.Error("Get(\"a\") and Get(\"b\") returned the same breaker")
	}
	if a1.Name() != "a" || bb.Name() != "b" {
		t.Errorf("names = %q, %q; want \"a\", \"b\"", a1.Name(), bb.Name())
	}

	// Registry settings reach the breakers it builds.
	a1.Failure()
	if got := a1.State(); got != StateOpen {
		t.Errorf("State() = %v, want %v (registry settings not applied)", got, StateOpen)
	}
}

func TestRegistryConcurrentGetReturnsOneInstance(t *testing.T) {
	r := NewRegistry(Settings{Window: time.Second, Buckets: 4, MinRequests: 1, HalfOpenMax: 1})

	const goroutines = 100
	got := make([]*Breaker, goroutines)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got[i] = r.Get("shared")
		}()
	}
	close(start)
	wg.Wait()

	for i, b := range got {
		if b != got[0] {
			t.Fatalf("goroutine %d got a different breaker instance", i)
		}
	}
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	clock := newFakeClock()
	b := NewBreaker("b", Settings{
		Window:        50 * time.Millisecond,
		Buckets:       5,
		MinRequests:   10,
		FailureRatio:  0.5,
		OpenTimeout:   10 * time.Millisecond,
		HalfOpenMax:   3,
		Now:           clock.Now,
		OnStateChange: func(string, State, State) {},
	})

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 500 {
				if p, ok := b.Admit(); ok {
					if (i+j)%3 == 0 {
						p.Failure()
					} else {
						p.Success()
					}
				}
				b.State()
				b.Counts()
				clock.Advance(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	req, fail := b.Counts()
	if req < 0 || fail < 0 || fail > req {
		t.Errorf("Counts() = (%d, %d), want 0 <= failures <= requests", req, fail)
	}
}

// A gRPC proxy forwards whatever method name arrives on the wire, so an unauthenticated caller can
// mint names indefinitely. Past MaxEntries every further name has to share one breaker instead of
// allocating another, or a stream of junk method names walks the process into an OOM.
func TestRegistryBoundsDistinctBreakers(t *testing.T) {
	const max = 8
	r := NewRegistry(Settings{
		Window: time.Second, Buckets: 4, MinRequests: 1, HalfOpenMax: 1, MaxEntries: max,
	})

	for i := range max {
		name := fmt.Sprintf("/svc/M%d", i)
		if got := r.Get(name); got.Name() != name {
			t.Fatalf("Get(%q).Name() = %q, want its own breaker under the cap", name, got.Name())
		}
	}

	overflow := r.Get("/svc/one-too-many")
	if overflow.Name() != OverflowName {
		t.Errorf("Get() past the cap returned %q, want the shared %q breaker", overflow.Name(), OverflowName)
	}
	if second := r.Get("/svc/another"); second != overflow {
		t.Error("each name past the cap allocated its own breaker; the registry is unbounded")
	}

	// Names admitted under the cap keep their own breaker.
	if got := r.Get("/svc/M0"); got.Name() != "/svc/M0" {
		t.Errorf("Get(\"/svc/M0\").Name() = %q, want the breaker it was given before the cap", got.Name())
	}
}

func TestRegistryDefaultsMaxEntries(t *testing.T) {
	r := NewRegistry(Settings{Window: time.Second, Buckets: 4, MinRequests: 1, HalfOpenMax: 1})
	for i := range DefaultMaxEntries {
		r.Get(fmt.Sprintf("m%d", i))
	}
	if got := r.Get("one-more"); got.Name() != OverflowName {
		t.Errorf("Get() past DefaultMaxEntries = %q, want %q", got.Name(), OverflowName)
	}
}
