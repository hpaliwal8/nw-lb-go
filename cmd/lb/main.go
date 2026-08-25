// Command lb is the gRPC load balancer: a transparent L7 proxy with consistent-hash affinity,
// active health checking, per-backend and per-method circuit breakers, and rate limiting.
//
// Configuration is layered: built-in defaults, then an optional YAML file (-config), then LB_*
// environment variables, and finally the command line flags, which win over everything.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/hitanshpaliwal/nw-lb-go/internal/balancer"
	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/health"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/middleware"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
	"github.com/hitanshpaliwal/nw-lb-go/internal/proxy"
	"github.com/hitanshpaliwal/nw-lb-go/internal/ratelimit"
)

// Server-side keepalive. These are deliberately fixed rather than configurable: they have to stay
// consistent with the client-side policy internal/pool dials with, and a mismatch between the two
// (a client pinging more often than the server permits) ends in GOAWAY ENHANCE_YOUR_CALM.
const (
	maxConnectionIdle     = 15 * time.Minute
	serverKeepaliveTime   = 30 * time.Second
	keepaliveTimeout      = 10 * time.Second
	minClientPingInterval = 10 * time.Second
)

// adminReadHeaderTimeout bounds a slow-loris on the admin port. The admin server has no other
// timeout because /debug/pprof/profile legitimately holds a request open for its whole duration.
const adminReadHeaderTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "lb:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	return runWith(ctx, args, stdout, nil)
}

// runWith is run plus a hook that reports the bound addresses. Tests need it because the addresses
// are only known after binding when the configuration asks for port 0.
func runWith(ctx context.Context, args []string, stdout io.Writer, ready func(grpcAddr, adminAddr net.Addr)) error {
	cfg, err := resolveConfig(args, stdout)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	log := newLogger(stdout, cfg.Logging)
	log.Info("starting load balancer",
		"listen", cfg.Listen,
		"admin_listen", cfg.AdminListen,
		"backends", len(cfg.Backends),
		"hash_header", cfg.Routing.HashHeader,
		"max_attempts", cfg.Routing.MaxAttempts,
		"rate_limit", cfg.RateLimit.Enabled,
		"circuit_breaker", cfg.CircuitBreaker.Enabled,
	)

	m := metrics.New()

	backends, err := pool.New(cfg, m)
	if err != nil {
		return err
	}
	defer backends.Close()

	checker := health.New(backends, cfg.Health, m, log)
	checker.Start(ctx)
	defer checker.Stop()

	bal := balancer.New(backends, cfg.Routing, m)
	defer bal.Close()

	px := proxy.New(bal, cfg, m, log)

	limiter := ratelimit.New(ratelimit.Config{
		Enabled:        cfg.RateLimit.Enabled,
		RPS:            cfg.RateLimit.RPS,
		Burst:          cfg.RateLimit.Burst,
		PerClient:      cfg.RateLimit.PerClient,
		PerClientRPS:   cfg.RateLimit.PerClientRPS,
		PerClientBurst: cfg.RateLimit.PerClientBurst,
	})
	defer limiter.Close()

	chain := middleware.Chain(
		middleware.Recovery(log, m),
		middleware.Context(cfg.Routing.HashHeader),
		middleware.Logging(log, cfg.Logging),
		middleware.Metrics(m),
		middleware.RateLimit(limiter, m),
		middleware.CircuitBreak(methodBreakers(cfg.CircuitBreaker, m), m),
	)

	grpcSrv := grpc.NewServer(serverOptions(cfg, px, chain)...)

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	adminLn, err := net.Listen("tcp", cfg.AdminListen)
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("listen %s: %w", cfg.AdminListen, err)
	}

	// Flipped once the proxy port is bound: /healthz answers for the process, not for the upstreams,
	// so it must not report ok while the port is still closed.
	var serving atomic.Bool
	serving.Store(true)

	adminSrv := &http.Server{
		Handler:           newAdminMux(m, backends, &serving),
		ReadHeaderTimeout: adminReadHeaderTimeout,
	}

	// Buffered so neither goroutine outlives the RPC of shutting down: after the select below
	// nobody reads these channels again.
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcSrv.Serve(ln) }()
	adminErr := make(chan error, 1)
	go func() { adminErr <- adminSrv.Serve(adminLn) }()

	log.Info("serving", "grpc", ln.Addr().String(), "admin", adminLn.Addr().String())
	if ready != nil {
		ready(ln.Addr(), adminLn.Addr())
	}

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("signal received, draining")
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			runErr = fmt.Errorf("grpc serve: %w", err)
		}
	case err := <-adminErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("admin serve: %w", err)
		}
	}

	lb := &instance{
		log:      log,
		grace:    cfg.Proxy.ShutdownGrace,
		checker:  checker,
		grpcSrv:  grpcSrv,
		adminSrv: adminSrv,
		bal:      bal,
		pool:     backends,
		limiter:  limiter,
	}
	lb.shutdown()
	return runErr
}

// instance groups the pieces shutdown has to take down, in order.
type instance struct {
	log      *slog.Logger
	grace    time.Duration
	checker  *health.Checker
	grpcSrv  *grpc.Server
	adminSrv *http.Server
	bal      *balancer.Balancer
	pool     *pool.Pool
	limiter  *ratelimit.Limiter
}

// shutdown drains in dependency order: stop probing first so a backend cannot flip while traffic is
// draining, then let in-flight RPCs finish, then take the admin port down, and only then close the
// connections those RPCs were using. Every step is idempotent, so the deferred cleanups in run stay
// safe.
func (in *instance) shutdown() {
	in.log.Info("shutting down", "grace", in.grace)

	in.checker.Stop()
	in.log.Info("health checker stopped")

	ctx, cancel := context.WithTimeout(context.Background(), in.grace)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		in.grpcSrv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		in.log.Info("grpc server stopped gracefully")
	case <-ctx.Done():
		in.log.Warn("graceful stop exceeded the shutdown grace, closing in-flight streams")
		in.grpcSrv.Stop()
		<-stopped
		in.log.Info("grpc server force-stopped")
	}

	// The same deadline on purpose: the grace is a budget for the whole shutdown, not per stage.
	if err := in.adminSrv.Shutdown(ctx); err != nil {
		in.log.Warn("admin server did not shut down cleanly, closing it", "error", err)
		_ = in.adminSrv.Close()
	}
	in.log.Info("admin server stopped")

	in.bal.Close()
	if err := in.pool.Close(); err != nil {
		in.log.Warn("closing backend connections", "error", err)
	}
	in.limiter.Close()
	in.log.Info("shutdown complete")
}

// resolveConfig layers defaults, the optional YAML file, LB_* environment variables and the flags,
// in that order. Flags are applied only when actually present on the command line, so a flag left
// at its zero value never silently overwrites a configured address.
func resolveConfig(args []string, out io.Writer) (config.Config, error) {
	fs := flag.NewFlagSet("lb", flag.ContinueOnError)
	fs.SetOutput(out)
	configPath := fs.String("config", "", "path to a YAML config file (optional)")
	listen := fs.String("listen", "", "gRPC listen address, overrides the config file")
	adminListen := fs.String("admin-listen", "", "admin HTTP listen address, overrides the config file")
	logLevel := fs.String("log-level", "", "log level: debug, info, warn or error")

	if err := fs.Parse(args); err != nil {
		return config.Config{}, err
	}
	if fs.NArg() > 0 {
		return config.Config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	cfg := config.Default()
	if strings.TrimSpace(*configPath) != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			return config.Config{}, err
		}
		cfg = loaded
	} else if err := cfg.ApplyEnv(os.LookupEnv); err != nil {
		// config.Load applies the environment itself; this is the no-file path, where LB_BACKENDS is
		// the only way to name a backend at all.
		return config.Config{}, fmt.Errorf("config: environment: %w", err)
	}

	set := make(map[string]bool, 4)
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["listen"] {
		cfg.Listen = *listen
	}
	if set["admin-listen"] {
		cfg.AdminListen = *adminListen
	}
	if set["log-level"] {
		cfg.Logging.Level = strings.ToLower(strings.TrimSpace(*logLevel))
	}

	if err := cfg.Validate(); err != nil {
		return config.Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

func newLogger(w io.Writer, cfg config.Logging) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.SlogLevel()}
	if strings.EqualFold(strings.TrimSpace(cfg.Format), "text") {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

// methodBreakers builds the registry backing the per-method breaker. It returns nil when breaking is
// disabled, which middleware.CircuitBreak turns into a pass-through rather than a per-request branch.
// The per-backend breakers are a separate registry owned by internal/pool; these two are keyed
// differently ("/pkg.Svc/Method" versus a backend id) so they share the lb_circuit_state gauge
// without colliding.
func methodBreakers(cb config.CircuitBreaker, m *metrics.Metrics) *circuit.Registry {
	if !cb.Enabled {
		return nil
	}
	return circuit.NewRegistry(circuit.Settings{
		Window:       cb.Window,
		Buckets:      cb.Buckets,
		MinRequests:  cb.MinRequests,
		FailureRatio: cb.FailureRatio,
		OpenTimeout:  cb.OpenTimeout,
		HalfOpenMax:  cb.HalfOpenMax,
		OnStateChange: func(name string, _, to circuit.State) {
			m.SetCircuitState(name, int(to))
		},
	})
}

func serverOptions(cfg config.Config, px *proxy.Proxy, chain grpc.StreamServerInterceptor) []grpc.ServerOption {
	opts := px.ServerOptions()
	return append(opts,
		// chain is already the composed pipeline; ChainStreamInterceptor is used here purely as the
		// way to install one interceptor, not to compose a second time.
		grpc.ChainStreamInterceptor(chain),
		grpc.MaxRecvMsgSize(cfg.Proxy.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.Proxy.MaxSendMsgSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: maxConnectionIdle,
			Time:              serverKeepaliveTime,
			Timeout:           keepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             minClientPingInterval,
			PermitWithoutStream: true,
		}),
		// Stream workers cap the number of goroutines gRPC spawns for incoming streams, which keeps
		// tail latency flat under a burst instead of paying scheduler cost per stream.
		grpc.NumStreamWorkers(uint32(runtime.NumCPU())),
	)
}

type healthResponse struct {
	Status string `json:"status"`
}

type readyResponse struct {
	Status          string `json:"status"`
	HealthyBackends int    `json:"healthy_backends"`
	Backends        int    `json:"backends"`
}

type backendStatus struct {
	ID           string `json:"id"`
	Addr         string `json:"addr"`
	State        string `json:"state"`
	BreakerState string `json:"breaker_state"`
	Weight       int    `json:"weight"`
	Healthy      bool   `json:"healthy"`
}

// newAdminMux builds the admin surface on its own mux. Nothing is registered on
// http.DefaultServeMux: net/http/pprof's init already populates that one, and serving it would
// expose profiling on any other listener the process ever opens.
func newAdminMux(m *metrics.Metrics, p *pool.Pool, serving *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())

	// Liveness: is this process able to serve at all. Deliberately independent of backend health —
	// an orchestrator must not restart a healthy proxy because its upstreams are down.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !serving.Load() {
			writeJSON(w, http.StatusServiceUnavailable, healthResponse{Status: "starting"})
			return
		}
		writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
	})

	// Readiness: would a request routed here reach a backend. This is what a load balancer in front
	// of the proxy should poll.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		all := p.Backends()
		healthy := len(p.Healthy())
		if healthy == 0 {
			writeJSON(w, http.StatusServiceUnavailable, readyResponse{
				Status: "no healthy backends", HealthyBackends: 0, Backends: len(all),
			})
			return
		}
		writeJSON(w, http.StatusOK, readyResponse{
			Status: "ok", HealthyBackends: healthy, Backends: len(all),
		})
	})

	mux.HandleFunc("/backends", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, backendStatuses(p))
	})

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}

func backendStatuses(p *pool.Pool) []backendStatus {
	backends := p.Backends()
	out := make([]backendStatus, 0, len(backends))
	for _, b := range backends {
		st := b.State()
		out = append(out, backendStatus{
			ID:           b.ID(),
			Addr:         b.Addr(),
			State:        st.String(),
			BreakerState: b.Breaker().State().String(),
			Weight:       b.Weight(),
			Healthy:      st == pool.StateHealthy,
		})
	}
	return out
}

// writeJSON marshals before touching the ResponseWriter so a failure cannot leave a half-written
// body under an already-sent 200.
func writeJSON(w http.ResponseWriter, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"status":"internal error"}`, http.StatusInternalServerError)
		return
	}
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}
