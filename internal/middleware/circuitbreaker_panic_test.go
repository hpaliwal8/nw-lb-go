package middleware

import (
	"testing"
	"time"

	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func panicSettings(halfOpenMax int) circuit.Settings {
	return circuit.Settings{
		Window:       time.Minute,
		Buckets:      10,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  time.Millisecond,
		HalfOpenMax:  halfOpenMax,
	}
}

// A panicking handler must still resolve the half-open probe it was admitted on. Before this was
// fixed, HalfOpenMax panics left that many probes outstanding with no in-flight request, and the
// method returned Unavailable "circuit open" for the rest of the process's life.
func TestPanicDoesNotWedgeBreakerHalfOpen(t *testing.T) {
	const halfOpenMax = 2
	reg := circuit.NewRegistry(panicSettings(halfOpenMax))
	interceptor := CircuitBreak(reg, nil)
	info := &grpc.StreamServerInfo{FullMethod: "/svc/M"}

	panicking := func(any, grpc.ServerStream) error { panic("boom") }
	healthy := func(any, grpc.ServerStream) error { return nil }

	callPanicking := func() {
		defer func() { _ = recover() }() // stand in for the Recovery interceptor
		_ = interceptor(nil, nil, info, panicking)
	}

	// Trip the breaker with panics, which must count as failures.
	for range 4 {
		callPanicking()
	}
	b := reg.Get("/svc/M")
	if got := b.State(); got != circuit.StateOpen {
		t.Fatalf("state = %v after repeated panics, want open (panics must count as failures)", got)
	}

	// Let it half-open, then burn every probe slot on panics.
	time.Sleep(5 * time.Millisecond)
	for range halfOpenMax {
		callPanicking()
	}

	// The breaker must not be stuck: healthy traffic has to be able to close it again.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := interceptor(nil, nil, info, healthy); err == nil {
			if b.State() == circuit.StateClosed {
				return
			}
			continue
		} else if status.Code(err) != codes.Unavailable {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("breaker never recovered; state = %v (probes leaked by panicking handlers)", b.State())
}
