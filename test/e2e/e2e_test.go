// Package e2e_test exercises the assembled load balancer through the generated echov1 client.
//
// Nothing here reaches into the proxy: every assertion is made from the outside, either through an
// ordinary gRPC call or through the metrics handler a Prometheus server would scrape. That is the
// point — the properties under test (transparency, affinity, failover, shedding) are only real if
// they hold for a client that has never heard of this proxy.
package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	echov1 "github.com/hitanshpaliwal/nw-lb-go/gen/echo/v1"
	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/middleware"
)

// TestTransparency proves the proxy is invisible: all four RPC cardinalities round-trip, request
// metadata reaches the backend, and the backend's own header and trailer reach the client.
func TestTransparency(t *testing.T) {
	lb := newTestLB(t)
	ctx := testContext(t)
	payload := []byte("transparent-payload")

	cases := []struct {
		name string
		call func(t *testing.T, ctx context.Context) (backendID string, hdr, tlr metadata.MD)
	}{
		{
			name: "unary",
			call: func(t *testing.T, ctx context.Context) (string, metadata.MD, metadata.MD) {
				t.Helper()
				var hdr, tlr metadata.MD
				resp, err := lb.client.Echo(ctx, &echov1.EchoRequest{Payload: payload},
					grpc.Header(&hdr), grpc.Trailer(&tlr))
				if err != nil {
					t.Fatalf("Echo: %v", err)
				}
				if !bytes.Equal(resp.GetPayload(), payload) {
					t.Errorf("payload = %q, want %q", resp.GetPayload(), payload)
				}
				return resp.GetBackendId(), hdr, tlr
			},
		},
		{
			name: "server_stream",
			call: func(t *testing.T, ctx context.Context) (string, metadata.MD, metadata.MD) {
				t.Helper()
				const want = 5
				st, err := lb.client.ServerStream(ctx, &echov1.EchoRequest{Payload: payload, StreamCount: want})
				if err != nil {
					t.Fatalf("ServerStream: %v", err)
				}
				hdr, err := st.Header()
				if err != nil {
					t.Fatalf("ServerStream header: %v", err)
				}
				var id string
				for i := range want {
					msg, err := st.Recv()
					if err != nil {
						t.Fatalf("ServerStream recv %d: %v", i, err)
					}
					if msg.GetSeq() != uint32(i) {
						t.Errorf("message %d: seq = %d, want %d", i, msg.GetSeq(), i)
					}
					if !bytes.Equal(msg.GetPayload(), payload) {
						t.Errorf("message %d: payload = %q, want %q", i, msg.GetPayload(), payload)
					}
					id = msg.GetBackendId()
				}
				if _, err := st.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("ServerStream did not end cleanly: %v", err)
				}
				return id, hdr, st.Trailer()
			},
		},
		{
			name: "client_stream",
			call: func(t *testing.T, ctx context.Context) (string, metadata.MD, metadata.MD) {
				t.Helper()
				const want = 4
				cs, err := lb.client.ClientStream(ctx)
				if err != nil {
					t.Fatalf("ClientStream: %v", err)
				}
				for i := range want {
					if err := cs.Send(&echov1.EchoRequest{Payload: payload}); err != nil {
						t.Fatalf("ClientStream send %d: %v", i, err)
					}
				}
				resp, err := cs.CloseAndRecv()
				if err != nil {
					t.Fatalf("ClientStream close: %v", err)
				}
				if resp.GetSeq() != want {
					t.Errorf("backend received %d messages, want %d", resp.GetSeq(), want)
				}
				hdr, err := cs.Header()
				if err != nil {
					t.Fatalf("ClientStream header: %v", err)
				}
				return resp.GetBackendId(), hdr, cs.Trailer()
			},
		},
		{
			name: "bidi_stream",
			call: func(t *testing.T, ctx context.Context) (string, metadata.MD, metadata.MD) {
				t.Helper()
				const want = 3
				bs, err := lb.client.BidiStream(ctx)
				if err != nil {
					t.Fatalf("BidiStream: %v", err)
				}
				var id string
				for i := range want {
					if err := bs.Send(&echov1.EchoRequest{Payload: payload}); err != nil {
						t.Fatalf("BidiStream send %d: %v", i, err)
					}
					msg, err := bs.Recv()
					if err != nil {
						t.Fatalf("BidiStream recv %d: %v", i, err)
					}
					if msg.GetSeq() != uint32(i) {
						t.Errorf("message %d: seq = %d, want %d", i, msg.GetSeq(), i)
					}
					id = msg.GetBackendId()
				}
				if err := bs.CloseSend(); err != nil {
					t.Fatalf("BidiStream close send: %v", err)
				}
				if _, err := bs.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("BidiStream did not end cleanly: %v", err)
				}
				hdr, err := bs.Header()
				if err != nil {
					t.Fatalf("BidiStream header: %v", err)
				}
				return id, hdr, bs.Trailer()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			callCtx := metadata.AppendToOutgoingContext(
				withSession(ctx, "transparency-"+tc.name), tokenHeader, tokenValue)
			id, hdr, tlr := tc.call(t, callCtx)

			if !slices.Contains(lb.backendIDs(), id) {
				t.Fatalf("response came from %q, which is not a configured backend", id)
			}
			if got := hdr.Get(backendIDHeader); len(got) != 1 || got[0] != id {
				t.Errorf("response header %s = %v, want [%s]", backendIDHeader, got, id)
			}
			if got := hdr.Get(middleware.RequestIDHeader); len(got) == 0 || got[0] == "" {
				t.Errorf("response header %s missing; the load balancer must return the id it logged",
					middleware.RequestIDHeader)
			}
			if got := tlr.Get(backendTrailerKey); len(got) != 1 || got[0] != id {
				t.Errorf("response trailer %s = %v, want [%s]", backendTrailerKey, got, id)
			}

			md := lb.backend(t, id).metadataSeen()
			if got := md.Get(tokenHeader); len(got) != 1 || got[0] != tokenValue {
				t.Errorf("backend saw %s = %v, want [%s]", tokenHeader, got, tokenValue)
			}
			if got := md.Get(sessionHeader); len(got) == 0 {
				t.Errorf("backend saw no %s; the affinity header must be forwarded too", sessionHeader)
			}
			if got := md.Get(forwardedForKey); len(got) == 0 {
				t.Errorf("backend saw no %s", forwardedForKey)
			}
		})
	}
}

// TestStatusFidelity checks that an upstream client error reaches the caller byte for byte and
// leaves the method circuit breaker closed. A breaker that counted client errors would let one
// caller sending bad requests fail-fast every other caller of the same method.
func TestStatusFidelity(t *testing.T) {
	lb := newTestLB(t)
	ctx := testContext(t)

	// Comfortably above the configured MinRequests, so a breaker that counted these would trip.
	calls := lb.cfg.CircuitBreaker.MinRequests + 10

	cases := []struct {
		name string
		code codes.Code
	}{
		{name: "invalid_argument", code: codes.InvalidArgument},
		{name: "not_found", code: codes.NotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lb.eachBackend(func(b *echoBackend) { b.injectFailure(tc.code, 0) })
			defer lb.eachBackend((*echoBackend).clearFailure)

			for i := range calls {
				_, err := lb.client.Echo(withSession(ctx, fmt.Sprintf("client-error-%d", i)),
					&echov1.EchoRequest{})
				st, ok := status.FromError(err)
				if !ok {
					t.Fatalf("call %d: %v is not a gRPC status", i, err)
				}
				if st.Code() != tc.code {
					t.Fatalf("call %d: code = %s, want %s", i, st.Code(), tc.code)
				}
				if st.Message() != injectedMessage {
					t.Fatalf("call %d: message = %q, want %q", i, st.Message(), injectedMessage)
				}
			}

			br := lb.breakers.Get(echoMethod)
			if got := br.State(); got != circuit.StateClosed {
				t.Errorf("method breaker state = %s, want closed", got)
			}
			if _, failures := br.Counts(); failures != 0 {
				t.Errorf("method breaker recorded %d failures; %s is a client error", failures, tc.code)
			}
			for _, id := range lb.backendIDs() {
				if _, failures := lb.poolBackend(t, id).Breaker().Counts(); failures != 0 {
					t.Errorf("backend %s breaker recorded %d failures for %s", id, failures, tc.code)
				}
			}
		})
	}

	resp, err := lb.client.Echo(ctx, &echov1.EchoRequest{})
	if err != nil {
		t.Fatalf("the load balancer stopped serving after a run of client errors: %v", err)
	}
	if resp.GetBackendId() == "" {
		t.Error("recovery call carried no backend id")
	}
	if n := metricSum(lb.scrape(t), "lb_rejected_total",
		map[string]string{"reason": middleware.RejectReasonCircuitOpen}); n != 0 {
		t.Errorf("lb_rejected_total for circuit_open = %v, want 0", n)
	}
}

// TestAffinity checks the property the consistent hash exists for: one session key, one backend.
func TestAffinity(t *testing.T) {
	const (
		keyCount    = 500
		callsPerKey = 5
	)
	lb := newTestLB(t, withBackends(5))
	ctx := testContext(t)

	owners := make(map[string]string, keyCount)
	perBackend := make(map[string]int, len(lb.backends))

	for _, key := range sessionKeys(keyCount) {
		for i := range callsPerKey {
			id := lb.ownerOf(t, ctx, key)
			if prev, seen := owners[key]; seen && prev != id {
				t.Fatalf("key %q was served by %s and then by %s on call %d", key, prev, id, i)
			}
			owners[key] = id
		}
		perBackend[owners[key]]++
	}

	if len(owners) != keyCount {
		t.Fatalf("observed %d distinct keys, want %d", len(owners), keyCount)
	}
	for _, id := range lb.backendIDs() {
		if perBackend[id] == 0 {
			t.Errorf("backend %s owned no key out of %d; the ring is not spreading load", id, keyCount)
		}
	}
}

// TestRebalancing removes one backend from the healthy set and checks the monotonic remapping
// guarantee: only the keys the removed backend owned move. Anything else would mean a single flap
// reshuffles the whole keyspace and invalidates every warm cache behind the proxy.
func TestRebalancing(t *testing.T) {
	lb := newTestLB(t, withBackends(5))
	ctx := testContext(t)
	keys := sessionKeys(500)

	before := lb.ownerMap(t, ctx, keys)

	victim := lb.backends[2]
	victim.setServing(false)
	vb := lb.poolBackend(t, victim.id)
	waitFor(t, 10*time.Second, "the health checker to drop "+victim.id, func() bool {
		return !vb.Healthy()
	})

	after := lb.ownerMap(t, ctx, keys)

	var reassigned int
	var moved []string
	for _, k := range keys {
		switch {
		case before[k] == victim.id:
			reassigned++
			if after[k] == victim.id {
				t.Fatalf("key %q is still served by the unhealthy backend %s", k, victim.id)
			}
		case after[k] != before[k]:
			moved = append(moved, fmt.Sprintf("%s: %s -> %s", k, before[k], after[k]))
		}
	}

	if reassigned == 0 {
		t.Fatalf("no key was owned by %s, so the test proved nothing", victim.id)
	}
	if len(moved) != 0 {
		t.Fatalf("%d of %d keys owned by surviving backends moved when %s was removed; first few: %v",
			len(moved), len(keys), victim.id, moved[:min(len(moved), 5)])
	}
}

// TestFailover kills a backend under load and checks that calls keep succeeding on the retry path,
// that the health checker eventually removes it, and that a replacement on the same address is
// readmitted and gets its keys back.
func TestFailover(t *testing.T) {
	lb := newTestLB(t, withBackends(4), withConfig(func(c *config.Config) {
		// A slower probe leaves a window in which the balancer still believes the dead backend is
		// healthy, which is exactly the window the data-path retry has to cover.
		c.Health = config.Health{Interval: 300 * time.Millisecond, Timeout: time.Second, Rise: 1, Fall: 2}
		// The per-backend breaker is deliberately kept out of the way: this test is about health
		// and retries, and a tripped breaker would gate the recovery assertion on its own timer.
		c.CircuitBreaker.MinRequests = 100_000
	}))
	ctx := testContext(t)

	keys := sessionKeys(200)
	before := lb.ownerMap(t, ctx, keys)

	victim := lb.backends[1]
	var victimKeys []string
	for _, k := range keys {
		if before[k] == victim.id {
			victimKeys = append(victimKeys, k)
		}
	}
	if len(victimKeys) == 0 {
		t.Fatalf("no key was owned by %s, so the test proved nothing", victim.id)
	}

	failoversBefore := metricSum(lb.scrape(t), "lb_failovers_total", nil)
	victim.stop()

	for _, k := range victimKeys {
		resp, err := lb.client.Echo(withSession(ctx, k), &echov1.EchoRequest{SessionKey: k})
		if err != nil {
			t.Fatalf("call for key %q failed after %s went down: %v", k, victim.id, err)
		}
		if resp.GetBackendId() == victim.id {
			t.Fatalf("key %q was answered by the stopped backend %s", k, victim.id)
		}
	}

	failoversAfter := metricSum(lb.scrape(t), "lb_failovers_total", nil)
	if failoversAfter <= failoversBefore {
		t.Errorf("lb_failovers_total = %v, was %v; the retries were not recorded", failoversAfter, failoversBefore)
	}
	t.Logf("%d of %d keys were owned by %s; lb_failovers_total moved by %v",
		len(victimKeys), len(keys), victim.id, failoversAfter-failoversBefore)

	vb := lb.poolBackend(t, victim.id)
	waitFor(t, 15*time.Second, "the health checker to mark "+victim.id+" unhealthy", func() bool {
		return !vb.Healthy()
	})
	if got := metricValue(t, lb.scrape(t), "lb_backend_healthy",
		map[string]string{"backend": victim.id}); got != 0 {
		t.Errorf("lb_backend_healthy{backend=%q} = %v, want 0", victim.id, got)
	}

	victim.restart(t)
	// gRPC waits out a reconnect backoff before retrying a connection that just failed. The health
	// checker is what this test is asserting on, not that timer.
	vb.Conn().ResetConnectBackoff()
	vb.Conn().Connect()

	waitFor(t, 20*time.Second, "the health checker to readmit "+victim.id, func() bool {
		return vb.Healthy()
	})
	if got := metricValue(t, lb.scrape(t), "lb_backend_healthy",
		map[string]string{"backend": victim.id}); got != 1 {
		t.Errorf("lb_backend_healthy{backend=%q} = %v, want 1", victim.id, got)
	}

	waitFor(t, 10*time.Second, victim.id+" to serve its own keys again", func() bool {
		resp, err := lb.client.Echo(withSession(ctx, victimKeys[0]), &echov1.EchoRequest{})
		return err == nil && resp.GetBackendId() == victim.id
	})

	after := lb.ownerMap(t, ctx, keys)
	for _, k := range keys {
		if after[k] != before[k] {
			t.Fatalf("key %q was owned by %s before the outage and by %s after recovery",
				k, before[k], after[k])
		}
	}
}

// TestRateLimiting checks that the limiter sheds load with the exact status the contract fixes,
// and that the shedding is visible in lb_rate_limited_total.
func TestRateLimiting(t *testing.T) {
	const (
		rps   = 1.0
		burst = 5
		calls = 20
	)
	lb := newTestLB(t, withConfig(func(c *config.Config) {
		c.RateLimit = config.RateLimit{Enabled: true, RPS: rps, Burst: burst}
	}))
	ctx := testContext(t)

	before := metricSum(lb.scrape(t), "lb_rate_limited_total", nil)

	results := make([]error, calls)
	for i := range calls {
		_, results[i] = lb.client.Echo(ctx, &echov1.EchoRequest{})
	}

	// At one token per second the bucket cannot refill meaningfully inside this loop, so the whole
	// burst must have been admitted.
	for i := range burst {
		if results[i] != nil {
			t.Errorf("call %d was within the burst of %d but was rejected: %v", i, burst, results[i])
		}
	}

	var limited int
	for i, err := range results {
		if err == nil {
			continue
		}
		limited++
		st, _ := status.FromError(err)
		if st.Code() != codes.ResourceExhausted {
			t.Fatalf("call %d: code = %s, want ResourceExhausted", i, st.Code())
		}
		if st.Message() != "rate limit exceeded" {
			t.Fatalf("call %d: message = %q, want %q", i, st.Message(), "rate limit exceeded")
		}
	}
	if limited < calls-burst-1 {
		t.Errorf("%d of %d calls were rejected, want at least %d", limited, calls, calls-burst-1)
	}

	after := metricSum(lb.scrape(t), "lb_rate_limited_total", nil)
	if delta := after - before; delta != float64(limited) {
		t.Errorf("lb_rate_limited_total moved by %v, want %d", delta, limited)
	}
}

// TestCircuitBreaking trips the method breaker with a failing upstream and checks that the
// rejection is both correct and cheap — fail-fast is worth nothing if it still pays the upstream
// round-trip — then checks that the breaker closes again once the upstream recovers.
func TestCircuitBreaking(t *testing.T) {
	const (
		upstreamDelay = 40 * time.Millisecond
		openTimeout   = 400 * time.Millisecond
		maxCalls      = 40
	)
	lb := newTestLB(t, withConfig(func(c *config.Config) {
		// One attempt per call keeps the accounting readable: one client call is one upstream call.
		c.Routing.MaxAttempts = 1
		c.CircuitBreaker = config.CircuitBreaker{
			Enabled:      true,
			Window:       5 * time.Second,
			Buckets:      10,
			MinRequests:  5,
			FailureRatio: 0.5,
			OpenTimeout:  openTimeout,
			HalfOpenMax:  1,
		}
	}))
	ctx := testContext(t)

	lb.eachBackend(func(b *echoBackend) { b.injectFailure(codes.Unavailable, upstreamDelay) })

	var (
		upstream []time.Duration
		openTook time.Duration
		opened   bool
	)
	for i := range maxCalls {
		start := time.Now()
		_, err := lb.client.Echo(withSession(ctx, fmt.Sprintf("breaker-%03d", i)), &echov1.EchoRequest{})
		elapsed := time.Since(start)
		if err == nil {
			t.Fatalf("call %d succeeded while every backend was failing", i)
		}
		st, _ := status.FromError(err)
		if st.Code() != codes.Unavailable {
			t.Fatalf("call %d: code = %s, want Unavailable", i, st.Code())
		}
		if st.Message() == "circuit open" {
			openTook, opened = elapsed, true
			break
		}
		upstream = append(upstream, elapsed)
	}

	if !opened {
		t.Fatalf("the method breaker never opened after %d failing calls", len(upstream))
	}
	median := percentile(sortedDurations(upstream), 0.5)
	t.Logf("breaker opened after %d failing calls; rejection took %s against a %s median upstream call",
		len(upstream), openTook, median)
	if openTook >= upstreamDelay {
		t.Errorf("a circuit-open rejection took %s, which is not faster than the %s upstream delay",
			openTook, upstreamDelay)
	}
	if 2*openTook >= median {
		t.Errorf("a circuit-open rejection took %s against a %s median upstream call; that is not fail-fast",
			openTook, median)
	}

	scraped := lb.scrape(t)
	if n := metricSum(scraped, "lb_rejected_total",
		map[string]string{"method": echoMethod, "reason": middleware.RejectReasonCircuitOpen}); n <= 0 {
		t.Errorf("lb_rejected_total for circuit_open = %v, want > 0", n)
	}
	if s := metricValue(t, scraped, "lb_circuit_state", map[string]string{"name": echoMethod}); s < 1 {
		t.Errorf("lb_circuit_state{name=%q} = %v, want open or half-open", echoMethod, s)
	}

	lb.eachBackend((*echoBackend).clearFailure)
	waitFor(t, 15*time.Second, "the breaker to admit traffic once the upstream recovered", func() bool {
		_, err := lb.client.Echo(withSession(ctx, "breaker-recovery"), &echov1.EchoRequest{})
		return err == nil
	})
	waitFor(t, 5*time.Second, "the method breaker to close", func() bool {
		return lb.breakers.Get(echoMethod).State() == circuit.StateClosed
	})
}

// TestMetricsContract scrapes the real handler and checks that the series other tooling depends on
// exist and carry sane values, including the SLO bucket boundary at 200 ms.
func TestMetricsContract(t *testing.T) {
	lb := newTestLB(t)
	ctx := testContext(t)

	for i := range 20 {
		if _, err := lb.client.Echo(withSession(ctx, fmt.Sprintf("metrics-%d", i)),
			&echov1.EchoRequest{}); err != nil {
			t.Fatalf("Echo %d: %v", i, err)
		}
	}
	st, err := lb.client.ServerStream(ctx, &echov1.EchoRequest{StreamCount: 3})
	if err != nil {
		t.Fatalf("ServerStream: %v", err)
	}
	for {
		if _, err := st.Recv(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("ServerStream recv: %v", err)
		}
	}

	m := lb.scrape(t)

	if n := metricValue(t, m, "lb_requests_total",
		map[string]string{"method": echoMethod, "code": codes.OK.String()}); n < 20 {
		t.Errorf("lb_requests_total for %s = %v, want >= 20", echoMethod, n)
	}
	if n := metricValue(t, m, "lb_request_duration_seconds_count",
		map[string]string{"method": echoMethod, "code": codes.OK.String()}); n < 20 {
		t.Errorf("lb_request_duration_seconds_count = %v, want >= 20", n)
	}
	// The SLO is 200 ms, so 0.2 must be an exact bucket boundary rather than something a dashboard
	// has to interpolate towards.
	if n := metricValue(t, m, "lb_request_duration_seconds_bucket",
		map[string]string{"method": echoMethod, "code": codes.OK.String(), "le": "0.2"}); n < 20 {
		t.Errorf("lb_request_duration_seconds_bucket{le=\"0.2\"} = %v, want >= 20", n)
	}
	if n := metricSum(m, "lb_upstream_requests_total", nil); n < 21 {
		t.Errorf("lb_upstream_requests_total = %v, want >= 21", n)
	}
	for _, id := range lb.backendIDs() {
		if n := metricValue(t, m, "lb_backend_healthy", map[string]string{"backend": id}); n != 1 {
			t.Errorf("lb_backend_healthy{backend=%q} = %v, want 1", id, n)
		}
	}
	if n := metricValue(t, m, "lb_ring_members", nil); n != float64(len(lb.backends)) {
		t.Errorf("lb_ring_members = %v, want %d", n, len(lb.backends))
	}
}

// TestLatencySLO is the smoke test for the project's 200 ms p99 target, measured end to end
// through the whole pipeline with no artificial upstream delay.
func TestLatencySLO(t *testing.T) {
	const (
		calls = 2000
		slo   = 200 * time.Millisecond
	)
	lb := newTestLB(t)
	ctx := testContext(t)

	samples := make([]time.Duration, 0, calls)
	for i := range calls {
		start := time.Now()
		if _, err := lb.client.Echo(withSession(ctx, fmt.Sprintf("latency-%d", i%64)),
			&echov1.EchoRequest{}); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		samples = append(samples, time.Since(start))
	}

	sorted := sortedDurations(samples)
	p50, p99, max := percentile(sorted, 0.5), percentile(sorted, 0.99), sorted[len(sorted)-1]
	t.Logf("p50=%s p99=%s max=%s over %d calls", p50, p99, max, calls)
	if p99 >= slo {
		t.Errorf("p99 = %s, want < %s (p50 %s, max %s)", p99, slo, p50, max)
	}
}

// TestGracefulShutdown drains the load balancer while calls are in flight and checks that nothing
// panics and nothing is left running afterwards.
func TestGracefulShutdown(t *testing.T) {
	// goroutineSlack absorbs runtime and test-framework goroutines that come and go independently
	// of the stack under test; a leak of the kind this test looks for is a per-connection or
	// per-request goroutine and is far larger.
	const goroutineSlack = 15

	baseline := runtime.NumGoroutine()
	lb := newTestLB(t)
	ctx := testContext(t)

	var (
		wg        sync.WaitGroup
		completed atomic.Int64
		panicked  atomic.Int64
	)
	problems := make(chan error, 64)
	stop := make(chan struct{})

	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Add(1)
					report(problems, fmt.Errorf("caller %d panicked: %v", i, r))
				}
			}()
			key := fmt.Sprintf("shutdown-%d", i)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := lb.client.Echo(withSession(ctx, key), &echov1.EchoRequest{})
				completed.Add(1)
				switch status.Code(err) {
				case codes.OK, codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
				default:
					report(problems, fmt.Errorf("caller %d: unexpected status: %v", i, err))
					return
				}
			}
		}()
	}

	waitFor(t, 15*time.Second, "traffic to reach the load balancer", func() bool {
		return completed.Load() >= 100
	})

	drained := make(chan struct{})
	go func() {
		lb.gracefulStop()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(30 * time.Second):
		t.Fatal("GracefulStop did not return while calls were in flight")
	}

	close(stop)
	wg.Wait()
	close(problems)
	for err := range problems {
		t.Error(err)
	}
	if n := panicked.Load(); n != 0 {
		t.Errorf("%d caller goroutines panicked during shutdown", n)
	}

	peak := runtime.NumGoroutine()
	lb.shutdown()
	waitFor(t, 30*time.Second, "the stack's goroutines to exit", func() bool {
		runtime.Gosched()
		return runtime.NumGoroutine() <= baseline+goroutineSlack
	})
	t.Logf("goroutines: %d before the stack started, %d while serving, %d after shutdown",
		baseline, peak, runtime.NumGoroutine())
}

// report records a problem seen on a worker goroutine. testing.T is not safe to fail from an
// arbitrary goroutine after the test function returns, so failures travel back over a channel.
func report(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}
