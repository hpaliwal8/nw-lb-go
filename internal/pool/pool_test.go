package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
)

// startServers brings up n bare gRPC servers on loopback. No service is registered: the pool only
// needs a real listener on the other end of a real connection.
func startServers(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	for i := range n {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		srv := grpc.NewServer()
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
		addrs[i] = lis.Addr().String()
	}
	return addrs
}

// testConfig builds a valid config over the given addresses, with weights that differ per backend
// so accessor plumbing mistakes show up.
func testConfig(addrs []string, mutate ...func(*config.Config)) config.Config {
	c := config.Default()
	c.CircuitBreaker = config.CircuitBreaker{
		Enabled:      true,
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  time.Minute, // long enough that an open breaker stays open for the test
		HalfOpenMax:  1,
	}
	for i, a := range addrs {
		c.Backends = append(c.Backends, config.Backend{
			ID:     fmt.Sprintf("b%d", i+1),
			Addr:   a,
			Weight: (i + 1) * 50,
		})
	}
	for _, f := range mutate {
		f(&c)
	}
	return c
}

func newTestPool(t *testing.T, cfg config.Config, m *metrics.Metrics) *Pool {
	t.Helper()
	p, err := New(cfg, m)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// recordingDial wraps grpc.NewClient, remembering every connection it hands out and failing for
// the targets in fail.
func recordingDial(fail map[string]bool) (dialFunc, func() []*grpc.ClientConn, func() [][]grpc.DialOption) {
	var (
		mu      sync.Mutex
		conns   []*grpc.ClientConn
		optSets [][]grpc.DialOption
	)
	dial := func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		mu.Lock()
		optSets = append(optSets, opts)
		mu.Unlock()
		if fail[target] {
			return nil, errors.New("dial refused")
		}
		cc, err := grpc.NewClient(target, opts...)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		conns = append(conns, cc)
		mu.Unlock()
		return cc, nil
	}
	return dial, func() []*grpc.ClientConn {
			mu.Lock()
			defer mu.Unlock()
			return append([]*grpc.ClientConn(nil), conns...)
		}, func() [][]grpc.DialOption {
			mu.Lock()
			defer mu.Unlock()
			return append([][]grpc.DialOption(nil), optSets...)
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

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateUnknown, "unknown"},
		{StateHealthy, "healthy"},
		{StateUnhealthy, "unhealthy"},
		{State(9), "invalid(9)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.state.String(); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNewBackendsFollowConfigOrder(t *testing.T) {
	addrs := startServers(t, 3)
	cfg := testConfig(addrs)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config is invalid: %v", err)
	}
	p := newTestPool(t, cfg, metrics.New())

	backends := p.Backends()
	if len(backends) != len(cfg.Backends) {
		t.Fatalf("Backends() length = %d, want %d", len(backends), len(cfg.Backends))
	}
	for i, want := range cfg.Backends {
		b := backends[i]
		switch {
		case b.ID() != want.ID:
			t.Errorf("backend %d: ID() = %q, want %q", i, b.ID(), want.ID)
		case b.Addr() != want.Addr:
			t.Errorf("backend %d: Addr() = %q, want %q", i, b.Addr(), want.Addr)
		case b.Weight() != want.Weight:
			t.Errorf("backend %d: Weight() = %d, want %d", i, b.Weight(), want.Weight)
		}
		if b.Conn() == nil {
			t.Errorf("backend %d: Conn() is nil", i)
		}
		if b.Breaker() == nil {
			t.Errorf("backend %d: Breaker() is nil", i)
		}
		if got := b.State(); got != StateUnknown {
			t.Errorf("backend %d: initial State() = %v, want %v", i, got, StateUnknown)
		}
	}

	// Backends returns a copy: a caller reordering it must not disturb the pool.
	backends[0], backends[2] = backends[2], backends[0]
	if got := p.Backends()[0].ID(); got != cfg.Backends[0].ID {
		t.Errorf("Backends() leaked its backing array: first id = %q, want %q", got, cfg.Backends[0].ID)
	}
}

func TestGet(t *testing.T) {
	addrs := startServers(t, 2)
	cfg := testConfig(addrs)
	p := newTestPool(t, cfg, metrics.New())

	tests := []struct {
		id       string
		wantOK   bool
		wantAddr string
	}{
		{id: "b1", wantOK: true, wantAddr: addrs[0]},
		{id: "b2", wantOK: true, wantAddr: addrs[1]},
		{id: "b3", wantOK: false},
		{id: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run("id="+tc.id, func(t *testing.T) {
			b, ok := p.Get(tc.id)
			if ok != tc.wantOK {
				t.Fatalf("Get(%q) ok = %v, want %v", tc.id, ok, tc.wantOK)
			}
			if !ok {
				if b != nil {
					t.Errorf("Get(%q) returned a backend with ok=false", tc.id)
				}
				return
			}
			if b.Addr() != tc.wantAddr {
				t.Errorf("Get(%q).Addr() = %q, want %q", tc.id, b.Addr(), tc.wantAddr)
			}
		})
	}
}

// TestNewIsLazyAndConnUsable pins the two properties grpc.NewClient buys us: startup does not
// block on a connection, and the connection it hands back is a real one that reaches a server.
func TestNewIsLazyAndConnUsable(t *testing.T) {
	addrs := startServers(t, 1)
	p := newTestPool(t, testConfig(addrs), metrics.New())
	b := p.Backends()[0]

	if got := b.Conn().GetState(); got != connectivity.Idle {
		t.Errorf("state right after New = %v, want %v (New must not dial)", got, connectivity.Idle)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b.Conn().Connect()
	for {
		s := b.Conn().GetState()
		if s == connectivity.Ready {
			return
		}
		if !b.Conn().WaitForStateChange(ctx, s) {
			t.Fatalf("connection never became ready, stuck in %v", s)
		}
	}
}

func TestNewUnreachableBackendStillConstructs(t *testing.T) {
	addrs := startServers(t, 1)
	// Port 1 on loopback is reserved and refuses; New must still return a pool.
	cfg := testConfig(append(addrs, "127.0.0.1:1"))
	p := newTestPool(t, cfg, metrics.New())

	if got := len(p.Backends()); got != 2 {
		t.Fatalf("Backends() length = %d, want 2", got)
	}
	if _, ok := p.Get("b2"); !ok {
		t.Error("unreachable backend missing from the pool")
	}
}

func TestNewPassesCallerDialOptionsLast(t *testing.T) {
	addrs := startServers(t, 1)
	dial, _, optSets := recordingDial(nil)

	caller := grpc.EmptyDialOption{}
	p, err := newPool(testConfig(addrs), metrics.New(), dial, caller)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	sets := optSets()
	if len(sets) != 1 {
		t.Fatalf("dial called %d times, want 1", len(sets))
	}
	opts := sets[0]
	// Four defaults (credentials, call options, keepalive, service config) then the caller's.
	if len(opts) != 5 {
		t.Fatalf("dial options = %d, want 5", len(opts))
	}
	if opts[len(opts)-1] != grpc.DialOption(caller) {
		t.Error("caller dial option is not last; it cannot override the defaults")
	}
}

func TestSetStateReportsRealTransitions(t *testing.T) {
	addrs := startServers(t, 1)
	p := newTestPool(t, testConfig(addrs), metrics.New())
	b := p.Backends()[0]

	tests := []struct {
		name        string
		set         State
		wantChanged bool
	}{
		{"unknown to healthy", StateHealthy, true},
		{"healthy repeated", StateHealthy, false},
		{"healthy to unhealthy", StateUnhealthy, true},
		{"unhealthy repeated", StateUnhealthy, false},
		{"unhealthy to unknown", StateUnknown, true},
		{"unknown repeated", StateUnknown, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := b.SetState(tc.set); got != tc.wantChanged {
				t.Errorf("SetState(%v) = %v, want %v", tc.set, got, tc.wantChanged)
			}
			if got := b.State(); got != tc.set {
				t.Errorf("State() = %v, want %v", got, tc.set)
			}
			if got, want := b.Healthy(), tc.set == StateHealthy; got != want {
				t.Errorf("Healthy() = %v, want %v", got, want)
			}
		})
	}
}

func TestHealthyFiltersByState(t *testing.T) {
	addrs := startServers(t, 3)
	p := newTestPool(t, testConfig(addrs), metrics.New())

	tests := []struct {
		name   string
		states []State
		want   []string
	}{
		{"none set", []State{StateUnknown, StateUnknown, StateUnknown}, nil},
		{"middle healthy", []State{StateUnhealthy, StateHealthy, StateUnknown}, []string{"b2"}},
		{"all healthy", []State{StateHealthy, StateHealthy, StateHealthy}, []string{"b1", "b2", "b3"}},
		{"edges healthy", []State{StateHealthy, StateUnhealthy, StateHealthy}, []string{"b1", "b3"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i, s := range tc.states {
				p.Backends()[i].SetState(s)
			}
			got := make([]string, 0, len(tc.want))
			for _, b := range p.Healthy() {
				got = append(got, b.ID())
			}
			if len(got) != len(tc.want) {
				t.Fatalf("Healthy() = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("Healthy() = %v, want %v (order must follow config)", got, tc.want)
				}
			}
		})
	}
}

// trip drives a breaker open using the settings testConfig installs: two failures reach
// MinRequests with a 1.0 failure ratio.
func trip(t *testing.T, b *Backend) {
	t.Helper()
	b.Breaker().Failure()
	b.Breaker().Failure()
	if got := b.Breaker().State(); got != circuit.StateOpen {
		t.Fatalf("breaker state = %v, want %v", got, circuit.StateOpen)
	}
}

func TestAvailableHonoursHealthAndBreaker(t *testing.T) {
	tests := []struct {
		name           string
		breakerEnabled bool
		state          State
		tripBreaker    bool
		want           bool
	}{
		{name: "healthy, breaker closed", breakerEnabled: true, state: StateHealthy, want: true},
		{name: "healthy, breaker open", breakerEnabled: true, state: StateHealthy, tripBreaker: true, want: false},
		{name: "unhealthy, breaker closed", breakerEnabled: true, state: StateUnhealthy},
		{name: "unknown, breaker closed", breakerEnabled: true, state: StateUnknown},
		{name: "unhealthy, breaker open", breakerEnabled: true, state: StateUnhealthy, tripBreaker: true},
		{name: "breaker disabled, open, healthy", state: StateHealthy, tripBreaker: true, want: true},
		{name: "breaker disabled, open, unhealthy", state: StateUnhealthy, tripBreaker: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addrs := startServers(t, 1)
			cfg := testConfig(addrs, func(c *config.Config) {
				c.CircuitBreaker.Enabled = tc.breakerEnabled
			})
			p := newTestPool(t, cfg, metrics.New())
			b := p.Backends()[0]

			b.SetState(tc.state)
			if tc.tripBreaker {
				trip(t, b)
			}
			if got := b.Available(); got != tc.want {
				t.Errorf("Available() = %v, want %v (state %v, breaker %v, enabled %v)",
					got, tc.want, b.State(), b.Breaker().State(), tc.breakerEnabled)
			}
		})
	}
}

// A disabled breaker must still count and report: only the Available gate is turned off, so
// operators can watch what the breaker would have done before enabling it.
func TestDisabledBreakerStillRecordsAndReports(t *testing.T) {
	addrs := startServers(t, 1)
	cfg := testConfig(addrs, func(c *config.Config) { c.CircuitBreaker.Enabled = false })
	m := metrics.New()
	p := newTestPool(t, cfg, m)
	b := p.Backends()[0]

	if v, ok := gaugeValue(t, m.Registry(), "lb_circuit_state", "name", "b1"); !ok || v != float64(circuit.StateClosed) {
		t.Errorf("initial lb_circuit_state{name=b1} = %v (present %v), want %v", v, ok, float64(circuit.StateClosed))
	}

	trip(t, b)

	if req, fail := b.Breaker().Counts(); req != 2 || fail != 2 {
		t.Errorf("Counts() = (%d, %d), want (2, 2)", req, fail)
	}
	if v, ok := gaugeValue(t, m.Registry(), "lb_circuit_state", "name", "b1"); !ok || v != float64(circuit.StateOpen) {
		t.Errorf("lb_circuit_state{name=b1} = %v (present %v), want %v", v, ok, float64(circuit.StateOpen))
	}
}

func TestBreakerStateChangeUpdatesMetrics(t *testing.T) {
	addrs := startServers(t, 2)
	m := metrics.New()
	p := newTestPool(t, testConfig(addrs), m)

	trip(t, p.Backends()[0])

	tests := []struct {
		backend string
		want    float64
	}{
		{"b1", float64(circuit.StateOpen)},
		{"b2", float64(circuit.StateClosed)},
	}
	for _, tc := range tests {
		t.Run(tc.backend, func(t *testing.T) {
			v, ok := gaugeValue(t, m.Registry(), "lb_circuit_state", "name", tc.backend)
			if !ok {
				t.Fatalf("lb_circuit_state{name=%s} not exported", tc.backend)
			}
			if v != tc.want {
				t.Errorf("lb_circuit_state{name=%s} = %v, want %v", tc.backend, v, tc.want)
			}
		})
	}
}

func TestNilMetricsIsTolerated(t *testing.T) {
	addrs := startServers(t, 1)
	p, err := New(testConfig(addrs), nil)
	if err != nil {
		t.Fatalf("New with nil metrics: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	b := p.Backends()[0]
	b.SetState(StateHealthy)
	trip(t, b)
	if b.Available() {
		t.Error("Available() = true with an open breaker")
	}
}

func TestOnChangeAndNotifyChange(t *testing.T) {
	addrs := startServers(t, 1)
	p := newTestPool(t, testConfig(addrs), metrics.New())

	var first, second, nested atomic.Int64
	p.OnChange(func() { first.Add(1) })
	p.OnChange(func() {
		second.Add(1)
		// A callback that registers another callback must not deadlock: NotifyChange copies the
		// list before invoking anything.
		if second.Load() == 1 {
			p.OnChange(func() { nested.Add(1) })
		}
	})
	p.OnChange(nil) // ignored, must not panic on the next notify

	p.NotifyChange()
	if first.Load() != 1 || second.Load() != 1 || nested.Load() != 0 {
		t.Fatalf("after first notify: first=%d second=%d nested=%d, want 1/1/0",
			first.Load(), second.Load(), nested.Load())
	}

	p.NotifyChange()
	if first.Load() != 2 || second.Load() != 2 || nested.Load() != 1 {
		t.Fatalf("after second notify: first=%d second=%d nested=%d, want 2/2/1",
			first.Load(), second.Load(), nested.Load())
	}
}

func TestNotifyChangeWithoutCallbacks(t *testing.T) {
	addrs := startServers(t, 1)
	p := newTestPool(t, testConfig(addrs), metrics.New())
	p.NotifyChange()
}

func TestCloseIsIdempotent(t *testing.T) {
	addrs := startServers(t, 2)
	p, err := New(testConfig(addrs), metrics.New())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i := range 3 {
		if err := p.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
	for _, b := range p.Backends() {
		if got := b.Conn().GetState(); got != connectivity.Shutdown {
			t.Errorf("backend %s: conn state after Close = %v, want %v", b.ID(), got, connectivity.Shutdown)
		}
	}
}

func TestNewClosesDialedConnsOnFailure(t *testing.T) {
	addrs := startServers(t, 3)

	tests := []struct {
		name      string
		cfg       func([]string) config.Config
		fail      func([]string) map[string]bool
		wantConns int
	}{
		{
			name:      "dial fails on the third backend",
			cfg:       func(a []string) config.Config { return testConfig(a) },
			fail:      func(a []string) map[string]bool { return map[string]bool{a[2]: true} },
			wantConns: 2,
		},
		{
			name: "duplicate backend id",
			cfg: func(a []string) config.Config {
				return testConfig(a, func(c *config.Config) { c.Backends[2].ID = c.Backends[0].ID })
			},
			wantConns: 2,
		},
		{
			name: "empty backend addr",
			cfg: func(a []string) config.Config {
				return testConfig(a, func(c *config.Config) { c.Backends[1].Addr = "" })
			},
			wantConns: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var fail map[string]bool
			if tc.fail != nil {
				fail = tc.fail(addrs)
			}
			dial, conns, _ := recordingDial(fail)

			p, err := newPool(tc.cfg(addrs), metrics.New(), dial)
			if err == nil {
				_ = p.Close()
				t.Fatal("newPool succeeded, want an error")
			}
			if p != nil {
				t.Error("newPool returned a pool alongside an error")
			}

			created := conns()
			if len(created) != tc.wantConns {
				t.Fatalf("dialed %d connections, want %d", len(created), tc.wantConns)
			}
			for i, cc := range created {
				if got := cc.GetState(); got != connectivity.Shutdown {
					cc.Close()
					t.Errorf("conn %d leaked: state = %v, want %v", i, got, connectivity.Shutdown)
				}
			}
		})
	}
}

func TestZeroWeightFallsBackToDefault(t *testing.T) {
	addrs := startServers(t, 1)
	cfg := testConfig(addrs, func(c *config.Config) { c.Backends[0].Weight = 0 })
	p := newTestPool(t, cfg, metrics.New())

	if got := p.Backends()[0].Weight(); got != config.DefaultWeight {
		t.Errorf("Weight() = %d, want %d", got, config.DefaultWeight)
	}
}

func TestConcurrentStateAndReads(t *testing.T) {
	addrs := startServers(t, 4)
	p := newTestPool(t, testConfig(addrs), metrics.New())

	var notified atomic.Int64
	p.OnChange(func() { notified.Add(1) })

	const iterations = 500
	var wg sync.WaitGroup
	for i, b := range p.Backends() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range iterations {
				s := StateHealthy
				if (n+i)%3 == 0 {
					s = StateUnhealthy
				}
				if b.SetState(s) {
					p.NotifyChange()
				}
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				_ = p.Healthy()
				for _, b := range p.Backends() {
					_ = b.Available()
					_ = b.State().String()
				}
				if b, ok := p.Get("b1"); ok {
					if probe, admitted := b.Reserve(); admitted {
						probe.Success()
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			p.OnChange(func() {})
		}
	}()
	wg.Wait()

	if notified.Load() == 0 {
		t.Error("no change notifications fired")
	}
}
