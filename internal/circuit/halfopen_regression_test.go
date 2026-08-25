package circuit

import (
	"testing"
	"time"
)

func halfOpenBreaker(t *testing.T, clock *fakeClock, halfOpenMax int) *Breaker {
	t.Helper()
	b := NewBreaker("b", Settings{
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  4,
		FailureRatio: 0.5,
		OpenTimeout:  5 * time.Second,
		HalfOpenMax:  halfOpenMax,
		Now:          clock.Now,
	})
	for range 4 {
		b.Failure()
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("setup: state = %v, want open", got)
	}
	clock.Advance(6 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("setup: state = %v, want half-open", got)
	}
	return b
}

// Ready must not consume a probe slot. The balancer calls it once per candidate while filtering,
// including for candidates it discards; if that reserved a probe, nothing would ever resolve it and
// a recovering backend would wedge half-open and never receive traffic again.
func TestReadyDoesNotConsumeProbes(t *testing.T) {
	clock := newFakeClock()
	b := halfOpenBreaker(t, clock, 2)

	for i := range 100 {
		if !b.Ready() {
			t.Fatalf("Ready() = false on call %d; probe slots were consumed by a read-only check", i)
		}
	}

	// The slots must still all be there for real requests.
	for i := range 2 {
		if _, ok := b.Admit(); !ok {
			t.Fatalf("Admit() = false on probe %d, want the full HalfOpenMax budget", i)
		}
	}
	if _, ok := b.Admit(); ok {
		t.Fatal("Admit() = true past HalfOpenMax; the probe cap is not enforced")
	}
	if b.Ready() {
		t.Fatal("Ready() = true with every probe outstanding, want false")
	}
}

// The wedge this guards against: speculative checks that outnumber committed requests must still
// leave the breaker able to close.
func TestHalfOpenRecoversDespiteSpeculativeChecks(t *testing.T) {
	clock := newFakeClock()
	b := halfOpenBreaker(t, clock, 2)

	for range 2 {
		for range 50 { // a burst of candidate filtering
			b.Ready()
		}
		p, ok := b.Admit()
		if !ok {
			t.Fatal("Admit() = false, want an available probe slot")
		}
		p.Success()
	}

	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v after HalfOpenMax successes, want closed", got)
	}
	if _, ok := b.Admit(); !b.Ready() || !ok {
		t.Fatal("closed breaker must admit requests")
	}
}
