// Package pool owns backend identity: the dialed client connection, the health state and the
// per-backend circuit breaker. Everything a request path touches (State, Healthy, Available) is
// lock-free; the mutex here guards only the change-callback registry, which is a control-plane
// concern.
package pool

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
)

type State int32

const (
	StateUnknown State = iota
	StateHealthy
	StateUnhealthy
)

func (s State) String() string {
	switch s {
	case StateUnknown:
		return "unknown"
	case StateHealthy:
		return "healthy"
	case StateUnhealthy:
		return "unhealthy"
	default:
		return fmt.Sprintf("invalid(%d)", int32(s))
	}
}

// Keepalive settings are deliberately not configurable: they must stay well above the 5 minute
// minimum most servers enforce, and pinging more often than every 30s risks ENHANCE_YOUR_CALM.
const (
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second
)

// roundRobinServiceConfig spreads load across every address a single target resolves to, which
// matters when one configured backend is a DNS name in front of several pods.
const roundRobinServiceConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

// Backend is immutable except for its health state, which is an atomic so the request path never
// blocks behind the health checker.
type Backend struct {
	id     string
	addr   string
	weight int
	conn   *grpc.ClientConn

	breaker *circuit.Breaker
	// breakerEnabled is fixed at construction. The breaker exists either way so its metrics and
	// counters stay live; when disabled it simply does not gate Available.
	breakerEnabled bool

	state atomic.Int32
}

func (b *Backend) ID() string { return b.id }

func (b *Backend) Addr() string { return b.addr }

func (b *Backend) Weight() int { return b.weight }

func (b *Backend) Conn() *grpc.ClientConn { return b.conn }

func (b *Backend) State() State { return State(b.state.Load()) }

// SetState stores s and reports whether it differed from the previous value, so callers can log
// and notify on real transitions only.
func (b *Backend) SetState(s State) bool {
	return State(b.state.Swap(int32(s))) != s
}

func (b *Backend) Breaker() *circuit.Breaker { return b.breaker }

func (b *Backend) Healthy() bool { return b.State() == StateHealthy }

// Available reports whether a request may be sent to this backend now. It reserves nothing, so it
// is safe to call while filtering candidates that may then be discarded.
func (b *Backend) Available() bool {
	if !b.Healthy() {
		return false
	}
	if !b.breakerEnabled {
		return true
	}
	return b.breaker.Ready()
}

// Reserve claims capacity for an attempt the caller is committing to, consuming a half-open probe
// slot. It must be called at most once per attempt, and the returned Probe MUST be resolved with
// Success or Failure — otherwise the slot is never released and the breaker wedges half-open.
//
// The Probe is always usable even when the bool is false, so a caller that presses on regardless
// (the last-resort attempt when every candidate is filtered out) still books its outcome.
func (b *Backend) Reserve() (circuit.Probe, bool) {
	if !b.Healthy() {
		return b.breaker.Unattributed(), false
	}
	if !b.breakerEnabled {
		return b.breaker.Unattributed(), true
	}
	return b.breaker.Admit()
}

type Pool struct {
	backends []*Backend
	byID     map[string]*Backend
	breakers *circuit.Registry

	mu        sync.Mutex
	callbacks []func()

	closeOnce sync.Once
	closeErr  error
}

// dialFunc is the seam that lets tests inject a failing dial; production always uses
// grpc.NewClient.
type dialFunc func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

// New creates one lazily-connected client per configured backend. grpc.NewClient does not block on
// the connection, so an unreachable backend cannot stall startup — it simply stays unhealthy until
// the health checker says otherwise.
func New(cfg config.Config, m *metrics.Metrics, dialOpts ...grpc.DialOption) (*Pool, error) {
	return newPool(cfg, m, grpc.NewClient, dialOpts...)
}

func newPool(cfg config.Config, m *metrics.Metrics, dial dialFunc, dialOpts ...grpc.DialOption) (*Pool, error) {
	p := &Pool{
		backends: make([]*Backend, 0, len(cfg.Backends)),
		byID:     make(map[string]*Backend, len(cfg.Backends)),
		breakers: circuit.NewRegistry(breakerSettings(cfg.CircuitBreaker, m)),
	}

	opts := dialOptions(cfg.Proxy, dialOpts)
	for i, bc := range cfg.Backends {
		b, err := p.newBackend(i, bc, cfg.CircuitBreaker.Enabled, dial, opts, m)
		if err != nil {
			// Everything dialled so far belongs to a pool nobody will ever see; drop it now rather
			// than leaking a connection per failed start.
			p.closeConns()
			return nil, err
		}
		p.backends = append(p.backends, b)
		p.byID[b.id] = b
	}
	return p, nil
}

func (p *Pool) newBackend(i int, bc config.Backend, breakerEnabled bool, dial dialFunc, opts []grpc.DialOption, m *metrics.Metrics) (*Backend, error) {
	switch {
	case bc.ID == "":
		return nil, fmt.Errorf("pool: backends[%d]: empty id", i)
	case bc.Addr == "":
		return nil, fmt.Errorf("pool: backend %q: empty addr", bc.ID)
	}
	if _, dup := p.byID[bc.ID]; dup {
		return nil, fmt.Errorf("pool: duplicate backend id %q", bc.ID)
	}

	conn, err := dial(bc.Addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("pool: backend %q (%s): %w", bc.ID, bc.Addr, err)
	}

	weight := bc.Weight
	if weight <= 0 {
		weight = config.DefaultWeight
	}
	b := &Backend{
		id:             bc.ID,
		addr:           bc.Addr,
		weight:         weight,
		conn:           conn,
		breaker:        p.breakers.Get(bc.ID),
		breakerEnabled: breakerEnabled,
	}
	b.state.Store(int32(StateUnknown))

	if m != nil {
		// Publish both series up front so a backend that never flips is still visible on a
		// dashboard instead of missing entirely.
		m.SetCircuitState(b.id, int(b.breaker.State()))
		m.SetBackendHealth(b.id, false)
	}
	return b, nil
}

func breakerSettings(cb config.CircuitBreaker, m *metrics.Metrics) circuit.Settings {
	s := circuit.Settings{
		Window:       cb.Window,
		Buckets:      cb.Buckets,
		MinRequests:  cb.MinRequests,
		FailureRatio: cb.FailureRatio,
		OpenTimeout:  cb.OpenTimeout,
		HalfOpenMax:  cb.HalfOpenMax,
	}
	if m != nil {
		s.OnStateChange = func(name string, _, to circuit.State) {
			m.SetCircuitState(name, int(to))
		}
	}
	return s
}

// dialOptions puts the caller's options last so a test (or cmd/lb) can override any default.
func dialOptions(px config.Proxy, extra []grpc.DialOption) []grpc.DialOption {
	recv, send := px.MaxRecvMsgSize, px.MaxSendMsgSize
	if recv <= 0 {
		recv = config.Default().Proxy.MaxRecvMsgSize
	}
	if send <= 0 {
		send = config.Default().Proxy.MaxSendMsgSize
	}
	opts := make([]grpc.DialOption, 0, 4+len(extra))
	opts = append(opts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(recv),
			grpc.MaxCallSendMsgSize(send),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultServiceConfig(roundRobinServiceConfig),
	)
	return append(opts, extra...)
}

// Backends returns the configured backends in config order. The slice is a copy; the Backend
// pointers are shared and live for the lifetime of the pool.
func (p *Pool) Backends() []*Backend { return slices.Clone(p.backends) }

func (p *Pool) Get(id string) (*Backend, bool) {
	b, ok := p.byID[id]
	return b, ok
}

func (p *Pool) Healthy() []*Backend {
	out := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		if b.Healthy() {
			out = append(out, b)
		}
	}
	return out
}

// OnChange registers f to run on every NotifyChange. Callbacks are never unregistered: the pool
// outlives the components that subscribe to it.
func (p *Pool) OnChange(f func()) {
	if f == nil {
		return
	}
	p.mu.Lock()
	p.callbacks = append(p.callbacks, f)
	p.mu.Unlock()
}

// NotifyChange runs the registered callbacks synchronously. The mutex is released first so a
// callback may itself call OnChange or NotifyChange without deadlocking.
func (p *Pool) NotifyChange() {
	p.mu.Lock()
	cbs := slices.Clone(p.callbacks)
	p.mu.Unlock()
	for _, f := range cbs {
		f()
	}
}

// Close closes every connection and is safe to call more than once; repeat calls return the same
// result as the first.
func (p *Pool) Close() error {
	p.closeOnce.Do(func() { p.closeErr = p.closeConns() })
	return p.closeErr
}

func (p *Pool) closeConns() error {
	var errs []error
	for _, b := range p.backends {
		if err := b.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("pool: close %s (%s): %w", b.id, b.addr, err))
		}
	}
	return errors.Join(errs...)
}
