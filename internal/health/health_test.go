package health

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
)

// serving is the empty service name the standard health server answers by default.
const serving = ""

type testServer struct {
	addr   string
	health *grpchealth.Server // nil when the server registers no health service
	stop   func()
}

func (s *testServer) setStatus(st healthpb.HealthCheckResponse_ServingStatus) {
	s.health.SetServingStatus(serving, st)
}

// startServer brings up a real in-process gRPC server. withHealth=false leaves the health service
// unregistered so Check answers Unimplemented, which is what a plain application server does.
func startServer(t *testing.T, withHealth bool) *testServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	ts := &testServer{addr: lis.Addr().String()}
	if withHealth {
		ts.health = grpchealth.NewServer()
		ts.health.SetServingStatus(serving, healthpb.HealthCheckResponse_SERVING)
		healthpb.RegisterHealthServer(srv, ts.health)
	}
	go func() { _ = srv.Serve(lis) }()

	var once sync.Once
	ts.stop = func() { once.Do(srv.Stop) }
	t.Cleanup(ts.stop)
	return ts
}

// deadAddr returns an address nothing listens on: the listener is bound to claim the port and
// closed straight away, so connections are refused rather than hanging.
func deadAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *logCapture) WithGroup(string) slog.Handler { return h }

func (h *logCapture) count(level slog.Level, substr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == level && strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

func (h *logCapture) attr(level slog.Level, substr, key string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level != level || !strings.Contains(r.Message, substr) {
			continue
		}
		var v string
		var found bool
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				v, found = a.Value.String(), true
				return false
			}
			return true
		})
		if found {
			return v, true
		}
	}
	return "", false
}

type fixture struct {
	checker *Checker
	pool    *pool.Pool
	metrics *metrics.Metrics
	logs    *logCapture
}

func (f *fixture) backend(t *testing.T, id string) *pool.Backend {
	t.Helper()
	b, ok := f.pool.Get(id)
	if !ok {
		t.Fatalf("backend %q missing from pool", id)
	}
	return b
}

// newFixture wires a real pool over addrs (ids b1..bN) and a checker over it.
func newFixture(t *testing.T, h config.Health, addrs ...string) *fixture {
	t.Helper()
	cfg := config.Default()
	cfg.Health = h
	for i, a := range addrs {
		cfg.Backends = append(cfg.Backends, config.Backend{
			ID:     fmt.Sprintf("b%d", i+1),
			Addr:   a,
			Weight: config.DefaultWeight,
		})
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config is invalid: %v", err)
	}

	m := metrics.New()
	p, err := pool.New(cfg, m)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	logs := &logCapture{}
	f := &fixture{
		pool:    p,
		metrics: m,
		logs:    logs,
		checker: New(p, cfg.Health, m, slog.New(logs)),
	}
	t.Cleanup(f.checker.Stop)
	return f
}

func health(mutate ...func(*config.Health)) config.Health {
	h := config.Default().Health
	h.Interval = 10 * time.Millisecond
	h.Timeout = 500 * time.Millisecond
	for _, f := range mutate {
		f(&h)
	}
	return h
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(time.Millisecond)
	}
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name, label, value string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// histogramCount sums observations of lb_health_check_duration_seconds for one backend/result pair.
func histogramCount(t *testing.T, reg *prometheus.Registry, backend, result string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "lb_health_check_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["backend"] == backend && labels["result"] == result {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func TestCheckOnce(t *testing.T) {
	tests := []struct {
		name       string
		withHealth bool
		dead       bool
		status     healthpb.HealthCheckResponse_ServingStatus
		service    string
		wantState  pool.State
		wantErr    bool
		wantCode   codes.Code // codes.OK means "do not assert a code"
	}{
		{
			name:       "serving",
			withHealth: true,
			status:     healthpb.HealthCheckResponse_SERVING,
			wantState:  pool.StateHealthy,
		},
		{
			name:       "not serving",
			withHealth: true,
			status:     healthpb.HealthCheckResponse_NOT_SERVING,
			wantState:  pool.StateUnhealthy,
			wantErr:    true,
		},
		{
			name:       "service unknown status",
			withHealth: true,
			status:     healthpb.HealthCheckResponse_SERVICE_UNKNOWN,
			wantState:  pool.StateUnhealthy,
			wantErr:    true,
		},
		{
			name:       "unregistered service name",
			withHealth: true,
			status:     healthpb.HealthCheckResponse_SERVING,
			service:    "does.not.Exist",
			wantState:  pool.StateUnhealthy,
			wantErr:    true,
			wantCode:   codes.NotFound,
		},
		{
			name:      "no health service is healthy",
			wantState: pool.StateHealthy,
			wantErr:   true,
			wantCode:  codes.Unimplemented,
		},
		{
			name:      "dead backend",
			dead:      true,
			wantState: pool.StateUnhealthy,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr := ""
			if tc.dead {
				addr = deadAddr(t)
			} else {
				srv := startServer(t, tc.withHealth)
				if tc.withHealth {
					srv.setStatus(tc.status)
				}
				addr = srv.addr
			}

			f := newFixture(t, health(func(h *config.Health) { h.Service = tc.service }), addr)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			got, err := f.checker.CheckOnce(ctx, f.backend(t, "b1"))
			if got != tc.wantState {
				t.Errorf("CheckOnce state = %v, want %v (err %v)", got, tc.wantState, err)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckOnce err = %v, want error: %v", err, tc.wantErr)
			}
			if tc.wantCode != codes.OK && status.Code(err) != tc.wantCode {
				t.Errorf("CheckOnce code = %v, want %v (err %v)", status.Code(err), tc.wantCode, err)
			}
		})
	}
}

func TestCheckOnceRejectsNilBackend(t *testing.T) {
	f := newFixture(t, health(), startServer(t, true).addr)
	if got, err := f.checker.CheckOnce(context.Background(), nil); got != pool.StateUnhealthy || err == nil {
		t.Errorf("CheckOnce(nil) = (%v, %v), want (%v, non-nil error)", got, err, pool.StateUnhealthy)
	}
}

// TestRiseThreshold pins the exact probe on which a backend becomes healthy: never earlier, and
// never later.
func TestRiseThreshold(t *testing.T) {
	tests := []struct{ rise int }{{1}, {2}, {3}, {5}}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("rise=%d", tc.rise), func(t *testing.T) {
			srv := startServer(t, true)
			f := newFixture(t, health(func(h *config.Health) { h.Rise = tc.rise }), srv.addr)
			b := f.backend(t, "b1")
			st := &counters{}
			ctx := context.Background()

			for i := 1; i <= tc.rise; i++ {
				f.checker.probe(ctx, b, st)
				want := pool.StateUnknown
				if i == tc.rise {
					want = pool.StateHealthy
				}
				if got := b.State(); got != want {
					t.Fatalf("after probe %d/%d: State() = %v, want %v", i, tc.rise, got, want)
				}
			}
		})
	}
}

// TestFallThreshold does the same for the downward transition, starting from a healthy backend.
func TestFallThreshold(t *testing.T) {
	tests := []struct{ fall int }{{1}, {2}, {3}, {5}}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("fall=%d", tc.fall), func(t *testing.T) {
			srv := startServer(t, true)
			f := newFixture(t, health(func(h *config.Health) {
				h.Rise = 1
				h.Fall = tc.fall
			}), srv.addr)
			b := f.backend(t, "b1")
			st := &counters{}
			ctx := context.Background()

			f.checker.probe(ctx, b, st)
			if got := b.State(); got != pool.StateHealthy {
				t.Fatalf("State() = %v after one success with rise=1, want %v", got, pool.StateHealthy)
			}

			srv.setStatus(healthpb.HealthCheckResponse_NOT_SERVING)
			for i := 1; i <= tc.fall; i++ {
				f.checker.probe(ctx, b, st)
				want := pool.StateHealthy
				if i == tc.fall {
					want = pool.StateUnhealthy
				}
				if got := b.State(); got != want {
					t.Fatalf("after failure %d/%d: State() = %v, want %v", i, tc.fall, got, want)
				}
			}
		})
	}
}

// TestIsolatedFailureDoesNotFlip is the reason the damping exists: a blip inside a healthy run
// must be forgotten by the next success rather than accumulating towards Fall.
func TestIsolatedFailureDoesNotFlip(t *testing.T) {
	srv := startServer(t, true)
	f := newFixture(t, health(func(h *config.Health) {
		h.Rise = 1
		h.Fall = 3
	}), srv.addr)
	b := f.backend(t, "b1")
	st := &counters{}
	ctx := context.Background()

	steps := []struct {
		name    string
		status  healthpb.HealthCheckResponse_ServingStatus
		probes  int
		want    pool.State
		wantErr bool
	}{
		{name: "become healthy", status: healthpb.HealthCheckResponse_SERVING, probes: 1, want: pool.StateHealthy},
		{name: "single blip", status: healthpb.HealthCheckResponse_NOT_SERVING, probes: 1, want: pool.StateHealthy},
		{name: "recover", status: healthpb.HealthCheckResponse_SERVING, probes: 1, want: pool.StateHealthy},
		{name: "two failures short of fall", status: healthpb.HealthCheckResponse_NOT_SERVING, probes: 2, want: pool.StateHealthy},
		{name: "recover again resets", status: healthpb.HealthCheckResponse_SERVING, probes: 1, want: pool.StateHealthy},
		{name: "two more failures still short", status: healthpb.HealthCheckResponse_NOT_SERVING, probes: 2, want: pool.StateHealthy},
		{name: "third failure flips", status: healthpb.HealthCheckResponse_NOT_SERVING, probes: 1, want: pool.StateUnhealthy},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			srv.setStatus(step.status)
			for range step.probes {
				f.checker.probe(ctx, b, st)
			}
			if got := b.State(); got != step.want {
				t.Fatalf("State() = %v, want %v", got, step.want)
			}
		})
	}
}

// TestUnimplementedConvergesHealthyAndWarnsOnce covers a backend that never registered the health
// service: it must still receive traffic, and it must not fill the log with the same decision.
func TestUnimplementedConvergesHealthyAndWarnsOnce(t *testing.T) {
	srv := startServer(t, false)
	f := newFixture(t, health(func(h *config.Health) { h.Rise = 2 }), srv.addr)
	b := f.backend(t, "b1")
	st := &counters{}
	ctx := context.Background()

	const probes = 6
	for i := 1; i <= probes; i++ {
		f.checker.probe(ctx, b, st)
		want := pool.StateUnknown
		if i >= 2 {
			want = pool.StateHealthy
		}
		if got := b.State(); got != want {
			t.Fatalf("after probe %d: State() = %v, want %v", i, got, want)
		}
	}

	if got := f.logs.count(slog.LevelWarn, "does not implement"); got != 1 {
		t.Errorf("unimplemented warnings = %d over %d probes, want 1", got, probes)
	}
	if got := histogramCount(t, f.metrics.Registry(), "b1", resultOK); got != probes {
		t.Errorf("lb_health_check_duration_seconds{result=ok} count = %d, want %d", got, probes)
	}
}

func TestDeadBackendGoesUnhealthy(t *testing.T) {
	f := newFixture(t, health(func(h *config.Health) {
		h.Rise = 1
		h.Fall = 2
		h.Timeout = 300 * time.Millisecond
	}), deadAddr(t))
	b := f.backend(t, "b1")
	st := &counters{}
	ctx := context.Background()

	f.checker.probe(ctx, b, st)
	if got := b.State(); got != pool.StateUnknown {
		t.Fatalf("after 1 failure with fall=2: State() = %v, want %v", got, pool.StateUnknown)
	}
	f.checker.probe(ctx, b, st)
	if got := b.State(); got != pool.StateUnhealthy {
		t.Fatalf("after 2 failures with fall=2: State() = %v, want %v", got, pool.StateUnhealthy)
	}
	if got := histogramCount(t, f.metrics.Registry(), "b1", resultError); got != 2 {
		t.Errorf("lb_health_check_duration_seconds{result=error} count = %d, want 2", got)
	}
	if v, ok := gaugeValue(t, f.metrics.Registry(), "lb_backend_healthy", "backend", "b1"); !ok || v != 0 {
		t.Errorf("lb_backend_healthy{backend=b1} = %v (present %v), want 0", v, ok)
	}
}

// TestTransitionSideEffects checks that a real flip — and only a real flip — logs, updates the
// gauge and notifies the pool.
func TestTransitionSideEffects(t *testing.T) {
	srv := startServer(t, true)
	f := newFixture(t, health(func(h *config.Health) {
		h.Rise = 1
		h.Fall = 1
	}), srv.addr)

	var notifications atomic.Int64
	f.pool.OnChange(func() { notifications.Add(1) })

	b := f.backend(t, "b1")
	st := &counters{}
	ctx := context.Background()

	f.checker.probe(ctx, b, st)
	if got := notifications.Load(); got != 1 {
		t.Fatalf("notifications after first transition = %d, want 1", got)
	}
	if v, ok := gaugeValue(t, f.metrics.Registry(), "lb_backend_healthy", "backend", "b1"); !ok || v != 1 {
		t.Errorf("lb_backend_healthy{backend=b1} = %v (present %v), want 1", v, ok)
	}
	if from, ok := f.logs.attr(slog.LevelInfo, "backend health changed", "from"); !ok || from != pool.StateUnknown.String() {
		t.Errorf("transition log from = %q (present %v), want %q", from, ok, pool.StateUnknown.String())
	}

	// Three more successes are not transitions: nothing may be logged or notified again.
	for range 3 {
		f.checker.probe(ctx, b, st)
	}
	if got := notifications.Load(); got != 1 {
		t.Errorf("notifications after repeated successes = %d, want 1", got)
	}
	if got := f.logs.count(slog.LevelInfo, "backend health changed"); got != 1 {
		t.Errorf("transition logs after repeated successes = %d, want 1", got)
	}

	srv.setStatus(healthpb.HealthCheckResponse_NOT_SERVING)
	f.checker.probe(ctx, b, st)
	if got := notifications.Load(); got != 2 {
		t.Errorf("notifications after going unhealthy = %d, want 2", got)
	}
	if got := f.logs.count(slog.LevelInfo, "backend health changed"); got != 2 {
		t.Errorf("transition logs after going unhealthy = %d, want 2", got)
	}
	// The cause of the transition must reach the log, not just its direction.
	if msg, ok := f.logs.attr(slog.LevelInfo, "backend health changed", "error"); !ok || !strings.Contains(msg, "NOT_SERVING") {
		t.Errorf("transition log error = %q (present %v), want it to name NOT_SERVING", msg, ok)
	}
}

// TestStartProbesImmediately uses an interval far longer than the test's patience: the backend can
// only become healthy if Start runs a check before the first tick.
func TestStartProbesImmediately(t *testing.T) {
	srv := startServer(t, true)
	f := newFixture(t, health(func(h *config.Health) {
		h.Interval = time.Hour
		h.Rise = 1
	}), srv.addr)
	b := f.backend(t, "b1")

	f.checker.Start(context.Background())
	waitFor(t, 5*time.Second, b.Healthy, "the immediate first probe to mark b1 healthy")
	f.checker.Stop()
}

func TestStartConvergesAllBackends(t *testing.T) {
	good := startServer(t, true)
	noHealth := startServer(t, false)
	bad := startServer(t, true)
	bad.setStatus(healthpb.HealthCheckResponse_NOT_SERVING)

	f := newFixture(t, health(func(h *config.Health) {
		h.Interval = 5 * time.Millisecond
		h.Rise = 2
		h.Fall = 2
	}), good.addr, noHealth.addr, bad.addr, deadAddr(t))

	f.checker.Start(context.Background())

	want := map[string]pool.State{
		"b1": pool.StateHealthy,   // SERVING
		"b2": pool.StateHealthy,   // no health service at all
		"b3": pool.StateUnhealthy, // NOT_SERVING
		"b4": pool.StateUnhealthy, // nothing listening
	}
	waitFor(t, 10*time.Second, func() bool {
		for id, w := range want {
			b, ok := f.pool.Get(id)
			if !ok || b.State() != w {
				return false
			}
		}
		return true
	}, "every backend to reach its expected state")

	// A live backend flipping to NOT_SERVING must be noticed by the running loop.
	good.setStatus(healthpb.HealthCheckResponse_NOT_SERVING)
	waitFor(t, 10*time.Second, func() bool { return f.backend(t, "b1").State() == pool.StateUnhealthy },
		"b1 to go unhealthy after the server stopped serving")

	good.setStatus(healthpb.HealthCheckResponse_SERVING)
	waitFor(t, 10*time.Second, f.backend(t, "b1").Healthy, "b1 to recover")

	f.checker.Stop()
}

func TestStopIsIdempotentAndLeavesNoGoroutines(t *testing.T) {
	servers := []*testServer{startServer(t, true), startServer(t, true), startServer(t, true)}
	addrs := make([]string, len(servers))
	for i, s := range servers {
		addrs[i] = s.addr
	}
	f := newFixture(t, health(func(h *config.Health) { h.Interval = 5 * time.Millisecond }), addrs...)

	// Warm every connection first so the baseline already includes the gRPC transport goroutines
	// that a health RPC would otherwise create.
	ctx := context.Background()
	for _, b := range f.pool.Backends() {
		if _, err := f.checker.CheckOnce(ctx, b); err != nil {
			t.Fatalf("warmup CheckOnce(%s): %v", b.ID(), err)
		}
	}
	runtime.GC()
	baseline := runtime.NumGoroutine()

	f.checker.Start(ctx)
	// One loop per backend, plus the single shutdown watcher.
	if got, want := f.checker.live.Load(), int64(len(addrs)+1); got != want {
		t.Fatalf("checker goroutines after Start = %d, want %d", got, want)
	}
	waitFor(t, 10*time.Second, func() bool { return len(f.pool.Healthy()) == len(addrs) },
		"all backends to become healthy")

	for i := range 3 {
		f.checker.Stop()
		if got := f.checker.live.Load(); got != 0 {
			t.Fatalf("checker goroutines after Stop #%d = %d, want 0", i+1, got)
		}
	}

	// Nothing the checker started may outlive Stop. The tolerance covers gRPC's own bookkeeping,
	// not our loops, which the live counter already pinned at zero.
	const tolerance = 5
	waitFor(t, 5*time.Second, func() bool { return runtime.NumGoroutine() <= baseline+tolerance },
		fmt.Sprintf("goroutine count to settle at or below %d", baseline+tolerance))
}

func TestStartStopsOnContextCancel(t *testing.T) {
	srv := startServer(t, true)
	f := newFixture(t, health(func(h *config.Health) { h.Interval = 5 * time.Millisecond }), srv.addr)

	ctx, cancel := context.WithCancel(context.Background())
	f.checker.Start(ctx)
	waitFor(t, 10*time.Second, f.backend(t, "b1").Healthy, "b1 to become healthy")

	cancel()
	waitFor(t, 10*time.Second, func() bool { return f.checker.live.Load() == 0 },
		"the checker goroutines to exit on context cancellation")

	// Stop after the context already tore everything down must still return.
	f.checker.Stop()
}

func TestStopBeforeStart(t *testing.T) {
	srv := startServer(t, true)
	f := newFixture(t, health(), srv.addr)

	f.checker.Stop()
	f.checker.Start(context.Background())
	if got := f.checker.live.Load(); got != 0 {
		t.Errorf("checker goroutines after Start following Stop = %d, want 0", got)
	}
	f.checker.Stop()
}

func TestSecondStartDoesNotDoubleProbe(t *testing.T) {
	srv := startServer(t, true)
	f := newFixture(t, health(func(h *config.Health) { h.Interval = time.Hour }), srv.addr)

	f.checker.Start(context.Background())
	f.checker.Start(context.Background())
	if got, want := f.checker.live.Load(), int64(2); got != want {
		t.Errorf("checker goroutines after two Starts = %d, want %d", got, want)
	}
	f.checker.Stop()
}

func TestNextIntervalJitter(t *testing.T) {
	tests := []struct{ interval time.Duration }{
		{10 * time.Millisecond},
		{time.Second},
		{time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.interval.String(), func(t *testing.T) {
			c := New(nil, config.Health{Interval: tc.interval}, nil, nil)
			lo := time.Duration(float64(tc.interval) * (1 - jitterFraction))
			hi := time.Duration(float64(tc.interval) * (1 + jitterFraction))

			var below, above bool
			for range 2000 {
				d := c.nextInterval()
				if d < lo || d > hi {
					t.Fatalf("nextInterval() = %s, want within [%s, %s]", d, lo, hi)
				}
				switch {
				case d < tc.interval:
					below = true
				case d > tc.interval:
					above = true
				}
			}
			if !below || !above {
				t.Errorf("jitter never varied in both directions (below=%v above=%v)", below, above)
			}
		})
	}
}

func TestNewNormalisesDegenerateConfig(t *testing.T) {
	tests := []struct {
		name         string
		in           config.Health
		wantInterval time.Duration
		wantTimeout  time.Duration
		wantRise     int
		wantFall     int
	}{
		{
			name:         "zero value",
			wantInterval: defaultInterval,
			wantTimeout:  defaultTimeout,
			wantRise:     1,
			wantFall:     1,
		},
		{
			name:         "negative durations and counts",
			in:           config.Health{Interval: -time.Second, Timeout: -time.Second, Rise: -3, Fall: 0},
			wantInterval: defaultInterval,
			wantTimeout:  defaultTimeout,
			wantRise:     1,
			wantFall:     1,
		},
		{
			name:         "valid values survive",
			in:           config.Health{Interval: 250 * time.Millisecond, Timeout: 20 * time.Millisecond, Rise: 4, Fall: 7},
			wantInterval: 250 * time.Millisecond,
			wantTimeout:  20 * time.Millisecond,
			wantRise:     4,
			wantFall:     7,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := New(nil, tc.in, nil, nil)
			switch {
			case c.interval != tc.wantInterval:
				t.Errorf("interval = %s, want %s", c.interval, tc.wantInterval)
			case c.timeout != tc.wantTimeout:
				t.Errorf("timeout = %s, want %s", c.timeout, tc.wantTimeout)
			case c.rise != tc.wantRise:
				t.Errorf("rise = %d, want %d", c.rise, tc.wantRise)
			case c.fall != tc.wantFall:
				t.Errorf("fall = %d, want %d", c.fall, tc.wantFall)
			}
		})
	}
}

// TestNilMetricsAndLogger mirrors pool's tolerance: a checker built without observability must
// still work rather than panicking on the first probe.
func TestNilMetricsAndLogger(t *testing.T) {
	srv := startServer(t, true)
	cfg := config.Default()
	cfg.Health = health(func(h *config.Health) { h.Rise = 1 })
	cfg.Backends = []config.Backend{{ID: "b1", Addr: srv.addr, Weight: config.DefaultWeight}}

	p, err := pool.New(cfg, nil)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	c := New(p, cfg.Health, nil, nil)
	t.Cleanup(c.Stop)
	c.Start(context.Background())
	waitFor(t, 10*time.Second, p.Backends()[0].Healthy, "b1 to become healthy without metrics or a logger")
}

// TestConcurrentCheckOnce exercises the shared checker state (the once-per-backend warning map)
// from several goroutines, which is how -race earns its keep here.
func TestConcurrentCheckOnce(t *testing.T) {
	servers := []*testServer{startServer(t, true), startServer(t, false)}
	f := newFixture(t, health(), servers[0].addr, servers[1].addr, deadAddr(t))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f.checker.Start(ctx)

	var wg sync.WaitGroup
	for _, b := range f.pool.Backends() {
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 20 {
					_, _ = f.checker.CheckOnce(ctx, b)
					_ = b.State()
				}
			}()
		}
	}
	wg.Wait()
	f.checker.Stop()

	if got := f.logs.count(slog.LevelWarn, "does not implement"); got != 1 {
		t.Errorf("unimplemented warnings = %d under concurrency, want 1", got)
	}
}
