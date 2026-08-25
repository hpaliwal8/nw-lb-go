package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpchealth "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
)

// clearEnv neutralises any LB_* variable in the developer's shell: config resolution reads the real
// environment, and a stray LB_BACKENDS would otherwise change what these tests assert on.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		config.EnvListen, config.EnvAdminListen, config.EnvBackends,
		config.EnvLogLevel, config.EnvRateLimitRPS, config.EnvHashHeader, config.EnvMaxAttempts,
	} {
		t.Setenv(k, "")
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lb.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const baseConfig = `
listen: ":18080"
admin_listen: ":19090"
backends:
  - id: a
    addr: "127.0.0.1:50051"
  - id: b
    addr: "127.0.0.1:50052"
    weight: 50
routing:
  hash_header: "x-session-id"
  max_attempts: 2
logging:
  level: "warn"
  format: "text"
`

func TestResolveConfig(t *testing.T) {
	path := writeConfig(t, baseConfig)

	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, cfg config.Config)
	}{
		{
			name: "file only",
			args: []string{"-config", path},
			check: func(t *testing.T, cfg config.Config) {
				if cfg.Listen != ":18080" || cfg.AdminListen != ":19090" {
					t.Errorf("addresses = %q/%q", cfg.Listen, cfg.AdminListen)
				}
				if len(cfg.Backends) != 2 {
					t.Fatalf("backends = %d, want 2", len(cfg.Backends))
				}
				if cfg.Backends[0].Weight != config.DefaultWeight || cfg.Backends[1].Weight != 50 {
					t.Errorf("weights = %d/%d", cfg.Backends[0].Weight, cfg.Backends[1].Weight)
				}
				if cfg.Routing.MaxAttempts != 2 {
					t.Errorf("max_attempts = %d, want 2", cfg.Routing.MaxAttempts)
				}
			},
		},
		{
			name: "flags override the file",
			args: []string{"-config", path, "-listen", "127.0.0.1:1234", "-admin-listen", "127.0.0.1:5678", "-log-level", "DEBUG"},
			check: func(t *testing.T, cfg config.Config) {
				if cfg.Listen != "127.0.0.1:1234" {
					t.Errorf("listen = %q", cfg.Listen)
				}
				if cfg.AdminListen != "127.0.0.1:5678" {
					t.Errorf("admin_listen = %q", cfg.AdminListen)
				}
				if cfg.Logging.SlogLevel() != slog.LevelDebug {
					t.Errorf("level = %v", cfg.Logging.SlogLevel())
				}
			},
		},
		{
			// A flag that is present but empty is still an override, and must fail validation rather
			// than silently falling back to the file.
			name:    "explicitly empty listen",
			args:    []string{"-config", path, "-listen", ""},
			wantErr: "listen: must not be empty",
		},
		{
			name: "environment supplies backends without a file",
			env:  map[string]string{config.EnvBackends: "x=127.0.0.1:1,y=127.0.0.1:2", config.EnvMaxAttempts: "5"},
			check: func(t *testing.T, cfg config.Config) {
				if len(cfg.Backends) != 2 || cfg.Backends[0].ID != "x" || cfg.Backends[1].Addr != "127.0.0.1:2" {
					t.Errorf("backends = %+v", cfg.Backends)
				}
				if cfg.Routing.MaxAttempts != 5 {
					t.Errorf("max_attempts = %d, want 5", cfg.Routing.MaxAttempts)
				}
				if cfg.Listen != config.Default().Listen {
					t.Errorf("listen = %q, want the default", cfg.Listen)
				}
			},
		},
		{
			name:    "no backends anywhere",
			wantErr: "at least one backend is required",
		},
		{
			name:    "missing file",
			args:    []string{"-config", filepath.Join(t.TempDir(), "absent.yaml")},
			wantErr: "no such file",
		},
		{
			name:    "unknown flag",
			args:    []string{"-nope"},
			wantErr: "flag provided but not defined",
		},
		{
			name:    "positional arguments",
			args:    []string{"-config", path, "extra"},
			wantErr: "unexpected arguments: extra",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			cfg, err := resolveConfig(tt.args, io.Discard)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("resolveConfig succeeded, want error containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveConfig: %v", err)
			}
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestResolveConfigFlagsBeatEnvironment(t *testing.T) {
	clearEnv(t)
	t.Setenv(config.EnvBackends, "127.0.0.1:1")
	t.Setenv(config.EnvListen, "127.0.0.1:1111")

	cfg, err := resolveConfig([]string{"-listen", "127.0.0.1:9999"}, io.Discard)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.Listen != "127.0.0.1:9999" {
		t.Errorf("listen = %q, want the flag value", cfg.Listen)
	}
}

func TestRunHelp(t *testing.T) {
	clearEnv(t)
	var out strings.Builder
	if err := run(t.Context(), []string{"-h"}, &out); err != nil {
		t.Fatalf("run(-h) = %v, want nil", err)
	}
	for _, want := range []string{"-config", "-listen", "-admin-listen", "-log-level"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("usage does not mention %s:\n%s", want, out.String())
		}
	}
}

// TestShippedConfigs guards the files the compose topology and the local benchmark actually start
// with: a typo in either is only discovered at startup otherwise, and config.Load rejects unknown
// keys, so a renamed field would take the whole deployment down.
func TestShippedConfigs(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantAddrs  []string
		wantSample float64
	}{
		{
			name:       "compose",
			path:       filepath.Join("..", "..", "config", "lb.yaml"),
			wantAddrs:  []string{"backend-1:50051", "backend-2:50051", "backend-3:50051"},
			wantSample: 0.01,
		},
		{
			name:       "local",
			path:       filepath.Join("..", "..", "config", "lb.local.yaml"),
			wantAddrs:  []string{"127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"},
			wantSample: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			cfg, err := config.Load(tt.path)
			if err != nil {
				t.Fatalf("config.Load(%s): %v", tt.path, err)
			}
			if cfg.Listen != ":8080" || cfg.AdminListen != ":9090" {
				t.Errorf("addresses = %q/%q, want :8080/:9090", cfg.Listen, cfg.AdminListen)
			}
			if len(cfg.Backends) != len(tt.wantAddrs) {
				t.Fatalf("backends = %d, want %d", len(cfg.Backends), len(tt.wantAddrs))
			}
			for i, want := range tt.wantAddrs {
				if cfg.Backends[i].ID != fmt.Sprintf("backend-%d", i+1) || cfg.Backends[i].Addr != want {
					t.Errorf("backend %d = %+v, want id backend-%d at %s", i, cfg.Backends[i], i+1, want)
				}
				if cfg.Backends[i].Weight != config.DefaultWeight {
					t.Errorf("backend %d weight = %d", i, cfg.Backends[i].Weight)
				}
			}
			if cfg.Routing.HashHeader != "x-session-id" || cfg.Routing.VirtualNodes != 200 || cfg.Routing.MaxAttempts != 3 {
				t.Errorf("routing = %+v", cfg.Routing)
			}
			wantHealth := config.Health{Interval: time.Second, Timeout: 500 * time.Millisecond, Rise: 2, Fall: 3}
			if cfg.Health != wantHealth {
				t.Errorf("health = %+v, want %+v", cfg.Health, wantHealth)
			}
			if !cfg.RateLimit.Enabled || cfg.RateLimit.RPS != 50000 || cfg.RateLimit.Burst != 100000 {
				t.Errorf("rate_limit = %+v", cfg.RateLimit)
			}
			if cfg.CircuitBreaker != (config.CircuitBreaker{
				Enabled: true, Window: 10 * time.Second, Buckets: 10, MinRequests: 20,
				FailureRatio: 0.5, OpenTimeout: 5 * time.Second, HalfOpenMax: 5,
			}) {
				t.Errorf("circuit_breaker = %+v", cfg.CircuitBreaker)
			}
			if cfg.Logging.Format != "json" || cfg.Logging.Level != "info" || cfg.Logging.SampleRate != tt.wantSample {
				t.Errorf("logging = %+v, want sample_rate %v", cfg.Logging, tt.wantSample)
			}
			if cfg.Proxy != config.Default().Proxy {
				t.Errorf("proxy = %+v, want the defaults %+v", cfg.Proxy, config.Default().Proxy)
			}
		})
	}
}

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.Logging
		emit    func(l *slog.Logger)
		want    []string
		notWant []string
	}{
		{
			name: "json at info",
			cfg:  config.Logging{Level: "info", Format: "json"},
			emit: func(l *slog.Logger) { l.Info("hello", "k", "v") },
			want: []string{`"msg":"hello"`, `"k":"v"`},
		},
		{
			name: "text at info",
			cfg:  config.Logging{Level: "info", Format: "text"},
			emit: func(l *slog.Logger) { l.Info("hello", "k", "v") },
			want: []string{"msg=hello", "k=v"},
		},
		{
			name:    "warn level drops info",
			cfg:     config.Logging{Level: "warn", Format: "json"},
			emit:    func(l *slog.Logger) { l.Info("dropped") },
			notWant: []string{"dropped"},
		},
		{
			name: "debug level keeps debug",
			cfg:  config.Logging{Level: "debug", Format: "json"},
			emit: func(l *slog.Logger) { l.Debug("kept") },
			want: []string{"kept"},
		},
		{
			name: "unknown format falls back to json",
			cfg:  config.Logging{Level: "info", Format: ""},
			emit: func(l *slog.Logger) { l.Info("hello") },
			want: []string{`"msg":"hello"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf strings.Builder
			tt.emit(newLogger(&buf, tt.cfg))
			for _, w := range tt.want {
				if !strings.Contains(buf.String(), w) {
					t.Errorf("output %q does not contain %q", buf.String(), w)
				}
			}
			for _, w := range tt.notWant {
				if strings.Contains(buf.String(), w) {
					t.Errorf("output %q unexpectedly contains %q", buf.String(), w)
				}
			}
		})
	}
}

// newTestPool builds a pool over addresses nobody listens on. grpc.NewClient never dials eagerly,
// so this stays offline and deterministic. The metrics handle is returned as well because the admin
// mux must serve the same registry the pool publishes to.
func newTestPool(t *testing.T) (*pool.Pool, *metrics.Metrics) {
	t.Helper()
	cfg := config.Default()
	cfg.Backends = []config.Backend{
		{ID: "b1", Addr: "127.0.0.1:59001", Weight: 100},
		{ID: "b2", Addr: "127.0.0.1:59002", Weight: 50},
	}
	m := metrics.New()
	p, err := pool.New(cfg, m)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, m
}

func TestAdminMux(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		serving  bool
		healthy  []string
		wantCode int
		wantBody []string
	}{
		{
			name:     "healthz before the listener is up",
			path:     "/healthz",
			wantCode: http.StatusServiceUnavailable,
			wantBody: []string{`"status":"starting"`},
		},
		{
			name:     "healthz once serving",
			path:     "/healthz",
			serving:  true,
			wantCode: http.StatusOK,
			wantBody: []string{`"status":"ok"`},
		},
		{
			name:     "readyz with no healthy backend",
			path:     "/readyz",
			serving:  true,
			wantCode: http.StatusServiceUnavailable,
			wantBody: []string{`"healthy_backends":0`, `"backends":2`},
		},
		{
			name:     "readyz with one healthy backend",
			path:     "/readyz",
			serving:  true,
			healthy:  []string{"b2"},
			wantCode: http.StatusOK,
			wantBody: []string{`"status":"ok"`, `"healthy_backends":1`},
		},
		{
			name:     "metrics",
			path:     "/metrics",
			serving:  true,
			wantCode: http.StatusOK,
			wantBody: []string{"lb_backend_healthy"},
		},
		{
			name:     "pprof index is on this mux",
			path:     "/debug/pprof/",
			serving:  true,
			wantCode: http.StatusOK,
			wantBody: []string{"goroutine"},
		},
		{
			name:     "pprof cmdline is on this mux",
			path:     "/debug/pprof/cmdline",
			serving:  true,
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown path",
			path:     "/nope",
			serving:  true,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, m := newTestPool(t)
			for _, id := range tt.healthy {
				b, ok := p.Get(id)
				if !ok {
					t.Fatalf("backend %q not in the pool", id)
				}
				b.SetState(pool.StateHealthy)
			}
			var serving atomic.Bool
			serving.Store(tt.serving)

			rec := httptest.NewRecorder()
			newAdminMux(m, p, &serving).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tt.wantCode, rec.Body.String())
			}
			for _, w := range tt.wantBody {
				if !strings.Contains(rec.Body.String(), w) {
					t.Errorf("body %q does not contain %q", rec.Body.String(), w)
				}
			}
		})
	}
}

func TestAdminBackends(t *testing.T) {
	p, m := newTestPool(t)
	b, _ := p.Get("b1")
	b.SetState(pool.StateHealthy)

	var serving atomic.Bool
	serving.Store(true)
	rec := httptest.NewRecorder()
	newAdminMux(m, p, &serving).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/backends", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got []backendStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	want := []backendStatus{
		{ID: "b1", Addr: "127.0.0.1:59001", State: "healthy", BreakerState: "closed", Weight: 100, Healthy: true},
		{ID: "b2", Addr: "127.0.0.1:59002", State: "unknown", BreakerState: "closed", Weight: 50, Healthy: false},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d backends, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("backend %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// startHealthBackend runs a gRPC server that serves only the standard health service. That is
// enough to drive both the load balancer's own probing and one proxied RPC, without pulling a demo
// service into this test.
func startHealthBackend(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, grpchealth.NewServer())
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

func pollUntil(t *testing.T, within time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s did not happen within %s", what, within)
}

func TestRunServesAndShutsDown(t *testing.T) {
	clearEnv(t)
	backendAddr := startHealthBackend(t)
	path := writeConfig(t, fmt.Sprintf(`
listen: "127.0.0.1:0"
admin_listen: "127.0.0.1:0"
backends:
  - id: b1
    addr: %q
routing:
  hash_header: "x-session-id"
  max_attempts: 2
health:
  interval: 20ms
  timeout: 2s
  rise: 1
  fall: 1
rate_limit:
  enabled: true
  rps: 100000
  burst: 100000
circuit_breaker:
  enabled: true
logging:
  level: "error"
  format: "json"
  sample_rate: 0
proxy:
  shutdown_grace: 5s
`, backendAddr))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	type addrs struct{ grpcAddr, adminAddr string }
	ready := make(chan addrs, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWith(ctx, []string{"-config", path}, io.Discard, func(g, a net.Addr) {
			ready <- addrs{grpcAddr: g.String(), adminAddr: a.String()}
		})
	}()

	var bound addrs
	select {
	case bound = <-ready:
	case err := <-done:
		t.Fatalf("run returned before serving: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("run never reported its listen addresses")
	}

	admin := "http://" + bound.adminAddr
	pollUntil(t, 10*time.Second, "readyz reporting a healthy backend", func() bool {
		return getStatus(t, admin+"/readyz") == http.StatusOK
	})
	if code := getStatus(t, admin+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", code)
	}

	body := getBody(t, admin+"/backends")
	var backends []backendStatus
	if err := json.Unmarshal([]byte(body), &backends); err != nil {
		t.Fatalf("decode /backends %q: %v", body, err)
	}
	if len(backends) != 1 || !backends[0].Healthy || backends[0].Addr != backendAddr {
		t.Fatalf("/backends = %+v", backends)
	}

	// One real RPC through the proxy: the load balancer registers no services, so this reaches the
	// unknown-service handler and is forwarded to the backend verbatim.
	conn, err := grpc.NewClient(bound.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial lb: %v", err)
	}
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(rpcCtx, &grpc_health_v1.HealthCheckRequest{})
	rpcCancel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("proxied health check: %v", err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("proxied status = %v, want SERVING", resp.GetStatus())
	}
	if got := getBody(t, admin+"/metrics"); !strings.Contains(got, "lb_requests_total") {
		t.Error("/metrics does not report lb_requests_total after a proxied request")
	}
	_ = conn.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return after its context was cancelled")
	}

	// The admin listener must be gone once run returns, otherwise a restart would fail to bind.
	if resp, err := http.Get(admin + "/healthz"); err == nil {
		resp.Body.Close()
		t.Error("admin server still answering after shutdown")
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	clearEnv(t)
	err := run(t.Context(), []string{"-config", writeConfig(t, "listen: \":0\"\n")}, io.Discard)
	if err == nil {
		t.Fatal("run accepted a config with no backends")
	}
	if !strings.Contains(err.Error(), "backend") {
		t.Errorf("error = %v, want it to mention backends", err)
	}
}

func TestRunReportsListenFailure(t *testing.T) {
	clearEnv(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	path := writeConfig(t, fmt.Sprintf(`
listen: %q
admin_listen: "127.0.0.1:0"
backends:
  - id: b1
    addr: "127.0.0.1:59001"
logging:
  level: "error"
`, ln.Addr().String()))

	err = run(t.Context(), []string{"-config", path}, io.Discard)
	if err == nil {
		t.Fatal("run bound an address that was already taken")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Errorf("error = %v, want a net.OpError", err)
	}
}

func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func getBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(body)
}
