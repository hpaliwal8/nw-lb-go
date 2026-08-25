package middleware

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"google.golang.org/grpc"
)

// A transparent proxy forwards whatever method name arrives, so an unauthenticated client can
// stream junk names. Neither the metric label set nor the breaker registry may grow with them.
func TestMethodLabelsAreBounded(t *testing.T) {
	m := metrics.New()
	interceptor := Metrics(m)
	handler := func(any, grpc.ServerStream) error { return nil }

	const flood = maxMethodLabels * 3
	for i := range flood {
		info := &grpc.StreamServerInfo{FullMethod: fmt.Sprintf("/junk.Svc/M%d", i)}
		if err := interceptor(nil, nil, info, handler); err != nil {
			t.Fatalf("interceptor error: %v", err)
		}
	}

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var series int
	for _, f := range families {
		if f.GetName() == "lb_requests_total" {
			series = len(f.GetMetric())
		}
	}
	if series == 0 {
		t.Fatal("no lb_requests_total series were recorded")
	}
	if series > maxMethodLabels+1 { // +1 for the shared "other" bucket
		t.Errorf("lb_requests_total has %d series after %d distinct methods; label cardinality is unbounded", series, flood)
	}
}

func TestBoundedLabelsCollapsePastCap(t *testing.T) {
	b := newBoundedLabels(3)
	for _, name := range []string{"a", "b", "c"} {
		if got := b.label(name); got != name {
			t.Errorf("label(%q) = %q, want it kept under the cap", name, got)
		}
	}
	if got := b.label("d"); got != otherMethod {
		t.Errorf("label(\"d\") = %q, want %q past the cap", got, otherMethod)
	}
	// Names admitted before the cap keep their identity.
	if got := b.label("a"); got != "a" {
		t.Errorf("label(\"a\") = %q, want it still tracked", got)
	}
}

func TestBoundedLabelsConcurrent(t *testing.T) {
	b := newBoundedLabels(64)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 100 {
				b.label(fmt.Sprintf("/svc/m%d", (i*100+j)%256))
			}
		}()
	}
	wg.Wait()
	// The cap is approximate under races, but must not be wildly exceeded.
	if n := b.n.Load(); n > 96 {
		t.Errorf("tracked %d labels, want the cap of 64 to hold approximately", n)
	}
	if got := b.label("/svc/definitely-new"); !strings.HasPrefix(got, otherMethod) {
		t.Errorf("label() = %q past a saturated cap, want %q", got, otherMethod)
	}
}
