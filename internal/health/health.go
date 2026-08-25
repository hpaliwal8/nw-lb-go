// Package health probes every backend in the pool over the standard gRPC health protocol and
// damps the result before it reaches the request path.
//
// The damping is the point of this package. A single slow or lost probe must not move traffic, so
// a backend only flips after Rise consecutive successes or Fall consecutive failures. The
// hysteresis counters live here, not on pool.Backend, because they are private control-plane
// state: nothing on the request path should be able to observe or perturb them.
package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
)

// Fallbacks for a zero-valued config.Health. config.Validate rejects these values in production,
// but a Checker built by hand in a test must not spin at zero interval.
const (
	defaultInterval = time.Second
	defaultTimeout  = 500 * time.Millisecond
)

// jitterFraction spreads each backend's tick over ±10% of Interval. Without it a fleet that
// starts together probes together forever, turning routine health traffic into a synchronised
// burst that every backend sees at the same instant.
const jitterFraction = 0.1

// Result labels for lb_health_check_duration_seconds.
const (
	resultOK    = "ok"
	resultError = "error"
)

type Checker struct {
	p   *pool.Pool
	cfg config.Health
	m   *metrics.Metrics
	log *slog.Logger

	interval time.Duration
	timeout  time.Duration
	rise     int
	fall     int

	stop      chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once
	wg        sync.WaitGroup
	// live counts the goroutines this checker owns, so tests can assert Stop leaves nothing behind
	// without guessing at unrelated runtime goroutines.
	live atomic.Int64

	// unimplemented remembers which backends have already been reported as lacking a health
	// service, so that decision is logged once instead of on every tick.
	unimplemented sync.Map
}

// New builds a checker over p. A nil logger discards; a nil metrics handle is tolerated so a
// checker can be exercised without a registry.
func New(p *pool.Pool, cfg config.Health, m *metrics.Metrics, log *slog.Logger) *Checker {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c := &Checker{
		p:        p,
		cfg:      cfg,
		m:        m,
		log:      log,
		interval: cfg.Interval,
		timeout:  cfg.Timeout,
		rise:     cfg.Rise,
		fall:     cfg.Fall,
		stop:     make(chan struct{}),
	}
	if c.interval <= 0 {
		c.interval = defaultInterval
	}
	if c.timeout <= 0 {
		c.timeout = defaultTimeout
	}
	if c.rise < 1 {
		c.rise = 1
	}
	if c.fall < 1 {
		c.fall = 1
	}
	return c
}

// Start launches one goroutine per backend and returns immediately. Checking stops when ctx is
// cancelled or Stop is called, whichever comes first. Repeat calls are no-ops: a second Start
// would double every backend's probe rate.
func (c *Checker) Start(ctx context.Context) {
	c.startOnce.Do(func() {
		if c.p == nil {
			return
		}
		select {
		case <-c.stop:
			// Stop already ran; spawning now would leak past the Wait that already returned.
			return
		default:
		}

		ctx, cancel := context.WithCancel(ctx)
		// One watcher folds both shutdown signals into the single cancellation the per-backend
		// loops select on, which keeps the loops free of a second channel case.
		c.spawn(func() {
			defer cancel()
			select {
			case <-ctx.Done():
			case <-c.stop:
			}
		})
		for _, b := range c.p.Backends() {
			c.spawn(func() { c.run(ctx, b) })
		}
	})
}

func (c *Checker) spawn(f func()) {
	c.wg.Add(1)
	c.live.Add(1)
	go func() {
		defer c.live.Add(-1)
		defer c.wg.Done()
		f()
	}()
}

// Stop signals every goroutine and waits for them. It is safe to call more than once and from
// several goroutines; every caller waits for the same shutdown.
func (c *Checker) Stop() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.wg.Wait()
}

// CheckOnce issues a single health RPC over the backend's existing connection — it never dials.
// It returns both the mapped state and the underlying error so callers can log the cause of a
// transition rather than just its direction.
//
// A backend that answers Unimplemented is treated as HEALTHY: plenty of real servers never
// register grpc.health.v1.Health, and refusing to route to them would be a worse failure than
// routing blind.
func (c *Checker) CheckOnce(ctx context.Context, b *pool.Backend) (pool.State, error) {
	if b == nil {
		return pool.StateUnhealthy, errors.New("health: nil backend")
	}
	conn := b.Conn()
	if conn == nil {
		return pool.StateUnhealthy, fmt.Errorf("health: backend %s has no connection", b.ID())
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{
		Service: c.cfg.Service,
	})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			c.warnUnimplemented(b)
			return pool.StateHealthy, err
		}
		return pool.StateUnhealthy, err
	}
	if s := resp.GetStatus(); s != grpc_health_v1.HealthCheckResponse_SERVING {
		return pool.StateUnhealthy, fmt.Errorf("health: backend %s reports %s", b.ID(), s)
	}
	return pool.StateHealthy, nil
}

func (c *Checker) warnUnimplemented(b *pool.Backend) {
	if _, seen := c.unimplemented.LoadOrStore(b.ID(), struct{}{}); seen {
		return
	}
	c.log.Warn("backend does not implement the gRPC health service, treating it as healthy",
		"backend", b.ID(), "addr", b.Addr(), "service", c.cfg.Service)
}

// counters is one backend's hysteresis state, owned exclusively by that backend's goroutine.
type counters struct {
	success int
	failure int
}

func (c *Checker) run(ctx context.Context, b *pool.Backend) {
	st := &counters{}
	// The first probe runs immediately: waiting a full interval before learning anything would
	// leave every backend in StateUnknown — and therefore unroutable — for that long at startup.
	c.probe(ctx, b, st)

	t := time.NewTimer(c.nextInterval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.probe(ctx, b, st)
			t.Reset(c.nextInterval())
		}
	}
}

// nextInterval recomputes the jitter every tick rather than picking one offset per backend: a
// fixed offset drifts back into lockstep whenever probes are delayed by the same upstream stall.
func (c *Checker) nextInterval() time.Duration {
	f := 1 + jitterFraction*(2*rand.Float64()-1)
	d := time.Duration(float64(c.interval) * f)
	if d <= 0 {
		d = time.Nanosecond
	}
	return d
}

func (c *Checker) probe(ctx context.Context, b *pool.Backend, st *counters) {
	start := time.Now()
	state, err := c.CheckOnce(ctx, b)
	elapsed := time.Since(start)

	if c.m != nil {
		result := resultError
		if state == pool.StateHealthy {
			result = resultOK
		}
		c.m.ObserveHealthCheck(b.ID(), result, elapsed)
	}
	c.record(b, st, state, err)
}

// record applies the Rise/Fall hysteresis and publishes only real transitions.
//
// The counters start at zero and the first probe runs immediately, so a backend that is already
// serving when the process starts converges on exactly the Rise'th probe. The first success is
// deliberately not special-cased into an immediate flip: a backend that happens to answer one
// probe while flapping must still earn Rise in a row before it receives traffic.
func (c *Checker) record(b *pool.Backend, st *counters, s pool.State, err error) {
	var target pool.State
	if s == pool.StateHealthy {
		st.failure = 0
		// Clamped at the threshold: these loops run for the lifetime of the process, and an
		// unbounded counter would eventually wrap.
		if st.success < c.rise {
			st.success++
		}
		if st.success < c.rise {
			return
		}
		target = pool.StateHealthy
	} else {
		st.success = 0
		if st.failure < c.fall {
			st.failure++
		}
		if st.failure < c.fall {
			return
		}
		target = pool.StateUnhealthy
	}

	old := b.State()
	if !b.SetState(target) {
		return
	}

	attrs := []any{
		"backend", b.ID(),
		"addr", b.Addr(),
		"from", old.String(),
		"to", target.String(),
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	c.log.Info("backend health changed", attrs...)

	if c.m != nil {
		c.m.SetBackendHealth(b.ID(), target == pool.StateHealthy)
	}
	if c.p != nil {
		c.p.NotifyChange()
	}
}
