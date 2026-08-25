package circuit

import (
	"testing"
	"time"
)

// attributionBreaker returns a breaker with HalfOpenMax=1 so a single stray outcome is enough to
// resolve the whole half-open episode if attribution is wrong.
func attributionBreaker(clock *fakeClock, rec *recorder) *Breaker {
	s := Settings{
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  5 * time.Second,
		HalfOpenMax:  1,
		Now:          clock.Now,
	}
	if rec != nil {
		s.OnStateChange = rec.hook
	}
	return NewBreaker("attribution", s)
}

// This proxy forwards long-lived streams, so an outcome routinely arrives from a request that was
// admitted while the breaker was still closed — long before it tripped. Such an outcome must never
// close the breaker: doing so restores full traffic to a backend that was never re-validated, and
// clearing the window on the way out means the genuine probe's later failure cannot re-trip it.
func TestPreTripOutcomeCannotCloseHalfOpenBreaker(t *testing.T) {
	clock := newFakeClock()
	b := attributionBreaker(clock, nil)

	// R1 is admitted while the breaker is closed and stays in flight for the whole test.
	r1, ok := b.Admit()
	if !ok {
		t.Fatal("Admit() = false on a closed breaker")
	}
	if r1.IsProbe() {
		t.Fatal("a request admitted while closed must not be marked a half-open probe")
	}

	// Other traffic trips the breaker, and it later becomes half-open.
	b.Failure()
	b.Failure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %v, want open", got)
	}
	clock.Advance(5 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v, want half-open", got)
	}

	// The genuine probe is admitted and is still in flight.
	probe, ok := b.Admit()
	if !ok {
		t.Fatal("Admit() = false in half-open with a free slot")
	}
	if !probe.IsProbe() {
		t.Fatal("a request admitted in half-open must be marked a probe")
	}

	// R1 now finishes successfully. It predates the trip and says nothing about the recovery.
	r1.Success()

	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v after a pre-trip success, want the breaker still half-open", got)
	}
	if _, ok := b.Admit(); ok {
		t.Fatal("the pre-trip outcome released the genuine probe's slot")
	}

	// The real probe's verdict is what decides it.
	probe.Failure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %v after the genuine probe failed, want open", got)
	}
}

// The mirror case: a pre-trip stream that ends with an error during half-open must not reopen a
// breaker whose genuine probes are succeeding.
func TestPreTripFailureCannotReopenHalfOpenBreaker(t *testing.T) {
	clock := newFakeClock()
	b := attributionBreaker(clock, nil)

	r1, _ := b.Admit() // admitted while closed

	b.Failure()
	b.Failure()
	clock.Advance(5 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v, want half-open", got)
	}

	// The stale stream fails now.
	r1.Failure()
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v after a pre-trip failure, want the breaker still half-open", got)
	}

	// The genuine probe succeeds and closes it.
	probe, ok := b.Admit()
	if !ok {
		t.Fatal("Admit() = false, the stale failure consumed the probe slot")
	}
	probe.Success()
	if got := b.State(); got != StateClosed {
		t.Fatalf("State() = %v after the genuine probe succeeded, want closed", got)
	}
}

// A probe from an earlier half-open episode must not resolve a slot in a later one, or the cap on
// concurrent trial requests is not a cap at all.
func TestStaleProbeFromEarlierEpisodeIsIgnored(t *testing.T) {
	clock := newFakeClock()
	// Two slots, so the first episode can be ended by one probe while another stays outstanding.
	b := NewBreaker("stale", Settings{
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  5 * time.Second,
		HalfOpenMax:  2,
		Now:          clock.Now,
	})

	b.Failure()
	b.Failure()
	clock.Advance(5 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v, want half-open", got)
	}

	stale, ok := b.Admit() // episode 1, never resolved until the very end
	if !ok {
		t.Fatal("Admit() = false entering the first half-open episode")
	}
	ender, ok := b.Admit()
	if !ok {
		t.Fatal("Admit() = false for the second slot of episode 1")
	}

	// Episode 1 ends on its own probe's verdict, and the breaker later half-opens again.
	ender.Failure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %v, want open", got)
	}
	clock.Advance(5 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v, want half-open again", got)
	}

	// Episode 2 fills both of its slots.
	p1, ok1 := b.Admit()
	p2, ok2 := b.Admit()
	if !ok1 || !ok2 {
		t.Fatal("the second episode did not get its full probe budget")
	}
	if _, ok := b.Admit(); ok {
		t.Fatal("Admit() = true past HalfOpenMax in the second episode")
	}

	// The stale probe from episode 1 reports success. It must neither free a slot in episode 2 nor
	// count toward closing it.
	stale.Success()
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v after a stale probe succeeded, want half-open", got)
	}
	if _, ok := b.Admit(); ok {
		t.Fatal("a stale probe from an earlier episode released the current episode's slot")
	}

	p1.Success()
	p2.Success()
	if got := b.State(); got != StateClosed {
		t.Fatalf("State() = %v after the current probes succeeded, want closed", got)
	}
}

// Unattributed outcomes still have to be counted, or the rolling window silently under-reports and
// a genuinely failing backend never trips.
func TestUnattributedOutcomesStillCountInTheWindow(t *testing.T) {
	clock := newFakeClock()
	b := attributionBreaker(clock, nil)

	b.Success()
	b.Failure()
	if req, fail := b.Counts(); req != 2 || fail != 1 {
		t.Fatalf("Counts() = (%d, %d), want (2, 1)", req, fail)
	}

	// And they can still trip a closed breaker: that is ordinary traffic accounting.
	b.Failure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("State() = %v, want open", got)
	}
}

// An admission that never became a request must give its slot back. The proxy reserves before it
// knows whether it can still replay the buffered first message; when it cannot, it abandons the
// attempt without contacting the backend, and that slot has to return or the breaker wedges.
func TestReleaseReturnsTheSlotWithoutRecordingAnOutcome(t *testing.T) {
	clock := newFakeClock()
	b := attributionBreaker(clock, nil) // HalfOpenMax 1

	b.Failure()
	b.Failure()
	clock.Advance(5 * time.Second)
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("State() = %v, want half-open", got)
	}
	reqBefore, failBefore := b.Counts()

	p, ok := b.Admit()
	if !ok {
		t.Fatal("Admit() = false in half-open")
	}
	if _, ok := b.Admit(); ok {
		t.Fatal("Admit() = true past HalfOpenMax")
	}

	p.Release()

	if req, fail := b.Counts(); req != reqBefore || fail != failBefore {
		t.Errorf("Counts() = (%d, %d) after Release, want them unchanged at (%d, %d)",
			req, fail, reqBefore, failBefore)
	}
	if got := b.State(); got != StateHalfOpen {
		t.Errorf("State() = %v after Release, want half-open — releasing judges nothing", got)
	}
	next, ok := b.Admit()
	if !ok {
		t.Fatal("Admit() = false after Release; the slot leaked")
	}
	next.Success()
	if got := b.State(); got != StateClosed {
		t.Fatalf("State() = %v after the replacement probe succeeded, want closed", got)
	}
}

// Releasing something that was never a probe, or releasing twice, must not corrupt the count.
func TestReleaseIsSafeForNonProbesAndRepeats(t *testing.T) {
	clock := newFakeClock()
	b := attributionBreaker(clock, nil)

	Probe{}.Release()          // zero value
	b.Unattributed().Release() // bound but never admitted

	closed, _ := b.Admit()
	closed.Release() // admitted while closed, so not a probe

	b.Failure()
	b.Failure()
	clock.Advance(5 * time.Second)
	p, ok := b.Admit()
	if !ok {
		t.Fatal("Admit() = false in half-open")
	}
	p.Release()
	p.Release() // double release must not free a slot twice

	if _, ok := b.Admit(); !ok {
		t.Fatal("Admit() = false after release")
	}
	if _, ok := b.Admit(); ok {
		t.Fatal("double Release freed an extra slot past HalfOpenMax")
	}
}
