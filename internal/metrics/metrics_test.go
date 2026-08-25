package metrics

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const echoMethod = "/echo.v1.EchoService/Echo"

func TestCollectorCounts(t *testing.T) {
	m := New()

	m.ObserveRequest(echoMethod, "OK", 10*time.Millisecond)
	m.ObserveRequest(echoMethod, "Unavailable", 3*time.Millisecond)
	m.IncInflight(echoMethod)
	m.ObserveUpstream("b1", "OK", 5*time.Millisecond)
	m.ObserveUpstream("b2", "OK", 7*time.Millisecond)
	m.IncFailover(echoMethod, "unavailable")
	m.IncRateLimited(echoMethod)
	m.IncRejected(echoMethod, "circuit-open")
	m.IncPanic(echoMethod)
	m.SetBackendHealth("b1", true)
	m.SetBackendHealth("b2", false)
	m.SetCircuitState("b1", 2)
	m.SetRingMembers(3)
	m.SetRingVirtualNodes(600)
	m.ObserveHealthCheck("b1", "serving", 2*time.Millisecond)
	m.ObserveHealthCheck("b1", "error", 400*time.Millisecond)

	tests := []struct {
		name string
		c    prometheus.Collector
		want int
	}{
		{"lb_requests_total", m.requests, 2},
		{"lb_request_duration_seconds", m.requestDuration, 2},
		{"lb_requests_inflight", m.inflight, 1},
		{"lb_upstream_requests_total", m.upstreamRequests, 2},
		{"lb_upstream_duration_seconds", m.upstreamDuration, 2},
		{"lb_failovers_total", m.failovers, 1},
		{"lb_rate_limited_total", m.rateLimited, 1},
		{"lb_rejected_total", m.rejected, 1},
		{"lb_panics_total", m.panics, 1},
		{"lb_backend_healthy", m.backendHealthy, 2},
		{"lb_circuit_state", m.circuitState, 1},
		{"lb_ring_members", m.ringMembers, 1},
		{"lb_ring_virtual_nodes", m.ringVirtualNodes, 1},
		{"lb_health_check_duration_seconds", m.healthCheckDuration, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := testutil.CollectAndCount(tc.c, tc.name); got != tc.want {
				t.Errorf("CollectAndCount(%s) = %d, want %d", tc.name, got, tc.want)
			}
		})
	}
}

func TestExpositionNamesAndLabels(t *testing.T) {
	m := New()

	m.ObserveRequest(echoMethod, "OK", time.Millisecond)
	m.ObserveRequest(echoMethod, "OK", time.Millisecond)
	m.ObserveRequest(echoMethod, "Unavailable", time.Millisecond)
	m.IncRateLimited(echoMethod)
	m.IncRateLimited(echoMethod)
	m.IncRateLimited("/echo.v1.EchoService/Stream")
	m.SetBackendHealth("b1", true)
	m.SetBackendHealth("b2", false)
	m.SetCircuitState("b1", 1)
	m.SetRingMembers(3)
	m.SetRingVirtualNodes(600)

	tests := []struct {
		name     string
		c        prometheus.Collector
		metric   string
		expected string
	}{
		{
			name:   "requests",
			c:      m.requests,
			metric: "lb_requests_total",
			expected: `
# HELP lb_requests_total Total gRPC requests handled by the load balancer.
# TYPE lb_requests_total counter
lb_requests_total{code="OK",method="/echo.v1.EchoService/Echo"} 2
lb_requests_total{code="Unavailable",method="/echo.v1.EchoService/Echo"} 1
`,
		},
		{
			name:   "rate limited",
			c:      m.rateLimited,
			metric: "lb_rate_limited_total",
			expected: `
# HELP lb_rate_limited_total Requests rejected by the rate limiter.
# TYPE lb_rate_limited_total counter
lb_rate_limited_total{method="/echo.v1.EchoService/Echo"} 2
lb_rate_limited_total{method="/echo.v1.EchoService/Stream"} 1
`,
		},
		{
			name:   "backend healthy",
			c:      m.backendHealthy,
			metric: "lb_backend_healthy",
			expected: `
# HELP lb_backend_healthy 1 when the backend is healthy, 0 otherwise.
# TYPE lb_backend_healthy gauge
lb_backend_healthy{backend="b1"} 1
lb_backend_healthy{backend="b2"} 0
`,
		},
		{
			name:   "circuit state",
			c:      m.circuitState,
			metric: "lb_circuit_state",
			expected: `
# HELP lb_circuit_state Circuit breaker state: 0=closed, 1=open, 2=half-open.
# TYPE lb_circuit_state gauge
lb_circuit_state{name="b1"} 1
`,
		},
		{
			name:   "ring members",
			c:      m.ringMembers,
			metric: "lb_ring_members",
			expected: `
# HELP lb_ring_members Distinct members on the consistent hash ring.
# TYPE lb_ring_members gauge
lb_ring_members 3
`,
		},
		{
			name:   "ring virtual nodes",
			c:      m.ringVirtualNodes,
			metric: "lb_ring_virtual_nodes",
			expected: `
# HELP lb_ring_virtual_nodes Points on the consistent hash ring.
# TYPE lb_ring_virtual_nodes gauge
lb_ring_virtual_nodes 600
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := testutil.CollectAndCompare(tc.c, strings.NewReader(tc.expected), tc.metric); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestCounterAndGaugeValues(t *testing.T) {
	m := New()

	m.IncInflight(echoMethod)
	m.IncInflight(echoMethod)
	m.DecInflight(echoMethod)
	m.IncFailover(echoMethod, "unavailable")
	m.IncFailover(echoMethod, "unavailable")
	m.IncRejected(echoMethod, "circuit-open")
	m.IncPanic(echoMethod)
	m.SetCircuitState("b1", 2)
	m.SetBackendHealth("b1", true)
	m.SetBackendHealth("b1", false)

	tests := []struct {
		name string
		got  float64
		want float64
	}{
		{"inflight after inc/inc/dec", testutil.ToFloat64(m.inflight.WithLabelValues(echoMethod)), 1},
		{"failovers", testutil.ToFloat64(m.failovers.WithLabelValues(echoMethod, "unavailable")), 2},
		{"rejected", testutil.ToFloat64(m.rejected.WithLabelValues(echoMethod, "circuit-open")), 1},
		{"panics", testutil.ToFloat64(m.panics.WithLabelValues(echoMethod)), 1},
		{"circuit half-open", testutil.ToFloat64(m.circuitState.WithLabelValues("b1")), 2},
		{"health flipped back to 0", testutil.ToFloat64(m.backendHealthy.WithLabelValues("b1")), 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

// The 200 ms SLO is read straight off the le="0.2" bucket; losing that boundary would make
// SLO reporting silently interpolate instead of report.
func TestLatencyBucketsIncludeSLO(t *testing.T) {
	if !slices.Contains(LatencyBuckets, 0.2) {
		t.Fatalf("LatencyBuckets missing the 0.2 SLO boundary: %v", LatencyBuckets)
	}
	if !slices.IsSorted(LatencyBuckets) {
		t.Fatalf("LatencyBuckets must be ascending: %v", LatencyBuckets)
	}

	m := New()
	m.ObserveRequest(echoMethod, "OK", 150*time.Millisecond)
	m.ObserveUpstream("b1", "OK", 150*time.Millisecond)

	for _, name := range []string{"lb_request_duration_seconds", "lb_upstream_duration_seconds"} {
		t.Run(name, func(t *testing.T) {
			got := bucketBounds(t, m.reg, name)
			if !slices.Equal(got, LatencyBuckets) {
				t.Fatalf("%s buckets = %v, want %v", name, got, LatencyBuckets)
			}
			if !slices.Contains(got, 0.2) {
				t.Fatalf("%s is missing the 0.2 SLO boundary", name)
			}
		})
	}
}

func TestHandlerServesPrivateRegistry(t *testing.T) {
	m := New()
	m.ObserveRequest(echoMethod, "OK", time.Millisecond)

	body := scrape(t, m)

	for _, want := range []string{
		"lb_requests_total",
		"lb_request_duration_seconds_bucket",
		`le="0.2"`,
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q", want)
		}
	}

	if got, want := testutil.CollectAndCount(m.requests, "lb_requests_total"), 1; got != want {
		t.Errorf("registry lost the counter: got %d, want %d", got, want)
	}

	// The default registry must stay untouched: nothing here may leak into it.
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather default registry: %v", err)
	}
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), "lb_") {
			t.Errorf("collector %q leaked into the default registry", mf.GetName())
		}
	}
}

func TestHandlerNegotiatesOpenMetrics(t *testing.T) {
	m := New()
	m.ObserveRequest(echoMethod, "OK", time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text; version=1.0.0; charset=utf-8")
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "openmetrics-text") {
		t.Fatalf("Content-Type = %q, want an openmetrics-text negotiation", ct)
	}
	if body := rec.Body.String(); !strings.Contains(body, "# EOF") {
		t.Fatalf("OpenMetrics body is missing its EOF terminator:\n%s", body)
	}
}

func TestConcurrentUse(t *testing.T) {
	m := New()

	const (
		goroutines = 16
		iterations = 200
	)
	backends := []string{"b0", "b1", "b2", "b3"}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			backend := backends[g%len(backends)]
			for i := range iterations {
				m.IncInflight(echoMethod)
				m.ObserveRequest(echoMethod, "OK", time.Duration(i)*time.Microsecond)
				m.ObserveUpstream(backend, "OK", time.Duration(i)*time.Microsecond)
				m.IncFailover(echoMethod, "unavailable")
				m.IncRateLimited(echoMethod)
				m.IncRejected(echoMethod, "circuit-open")
				m.IncPanic(echoMethod)
				m.SetBackendHealth(backend, i%2 == 0)
				m.SetCircuitState(backend, i%3)
				m.SetRingMembers(len(backends))
				m.SetRingVirtualNodes(len(backends) * 200)
				m.ObserveHealthCheck(backend, "serving", time.Millisecond)
				m.DecInflight(echoMethod)
			}
		}()
	}

	// Scrapes and removals race against the writers above; -race must stay clean and the
	// counters that nobody deletes must land on their exact totals.
	wg.Add(2)
	go func() {
		defer wg.Done()
		h := m.Handler()
		for range iterations {
			// Not scrape(): t.Fatalf is illegal off the test goroutine.
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/metrics", nil))
		}
	}()
	go func() {
		defer wg.Done()
		for range iterations {
		}
	}()

	wg.Wait()

	const total = goroutines * iterations
	tests := []struct {
		name string
		got  float64
		want float64
	}{
		{"lb_requests_total", testutil.ToFloat64(m.requests.WithLabelValues(echoMethod, "OK")), total},
		{"lb_rate_limited_total", testutil.ToFloat64(m.rateLimited.WithLabelValues(echoMethod)), total},
		{"lb_failovers_total", testutil.ToFloat64(m.failovers.WithLabelValues(echoMethod, "unavailable")), total},
		{"lb_rejected_total", testutil.ToFloat64(m.rejected.WithLabelValues(echoMethod, "circuit-open")), total},
		{"lb_panics_total", testutil.ToFloat64(m.panics.WithLabelValues(echoMethod)), total},
		{"lb_requests_inflight", testutil.ToFloat64(m.inflight.WithLabelValues(echoMethod)), 0},
		{"lb_ring_members", testutil.ToFloat64(m.ringMembers), float64(len(backends))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func bucketBounds(t *testing.T, reg *prometheus.Registry, name string) []float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if len(mf.GetMetric()) == 0 {
			t.Fatalf("metric family %q has no series", name)
		}
		buckets := mf.GetMetric()[0].GetHistogram().GetBucket()
		bounds := make([]float64, 0, len(buckets))
		for _, b := range buckets {
			bounds = append(bounds, b.GetUpperBound())
		}
		return bounds
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}
