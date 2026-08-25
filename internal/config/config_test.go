package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// allEnvVars is every variable ApplyEnv reads. Tests that call Load must blank
// them all, otherwise a developer's shell leaks into the assertions.
var allEnvVars = []string{
	EnvListen, EnvAdminListen, EnvBackends, EnvLogLevel,
	EnvRateLimitRPS, EnvHashHeader, EnvMaxAttempts,
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allEnvVars {
		t.Setenv(k, "")
	}
}

func lookupFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lb.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func withBackends(c Config, backends ...Backend) Config {
	c.Backends = backends
	return c
}

func TestDefault(t *testing.T) {
	got := Default()

	want := Config{
		Listen:      ":8080",
		AdminListen: ":9090",
		Routing: Routing{
			HashHeader:   "x-session-id",
			VirtualNodes: 200,
			MaxAttempts:  3,
			RetryPolicy:  "connect-failure",
		},
		Health: Health{
			Interval: time.Second,
			Timeout:  500 * time.Millisecond,
			Rise:     2,
			Fall:     3,
		},
		RateLimit: RateLimit{
			RPS:            50000,
			Burst:          100000,
			PerClientRPS:   5000,
			PerClientBurst: 10000,
		},
		CircuitBreaker: CircuitBreaker{
			Window:       10 * time.Second,
			Buckets:      10,
			MinRequests:  20,
			FailureRatio: 0.5,
			OpenTimeout:  5 * time.Second,
			HalfOpenMax:  5,
		},
		Logging: Logging{Level: "info", Format: "json", SampleRate: 1},
		Proxy: Proxy{
			MaxRecvMsgSize: 16 << 20,
			MaxSendMsgSize: 16 << 20,
			ShutdownGrace:  10 * time.Second,
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Default() =\n%+v\nwant\n%+v", got, want)
	}
	if len(got.Backends) != 0 {
		t.Errorf("Default().Backends = %v, want none", got.Backends)
	}
	// Defaults minus backends is the only thing a config file has to fix.
	if err := withBackends(got, Backend{ID: "a", Addr: "h:1", Weight: 100}).Validate(); err != nil {
		t.Errorf("Default()+one backend must validate, got %v", err)
	}
}

func TestLoadFullYAML(t *testing.T) {
	clearEnv(t)

	path := writeYAML(t, `
listen: ":7000"
admin_listen: ":7001"
backends:
  - id: a
    addr: 10.0.0.1:50051
    weight: 50
  - id: b
    addr: 10.0.0.2:50051
routing:
  hash_header: x-tenant
  virtual_nodes: 64
  max_attempts: 2
  retry_policy: unavailable
health:
  interval: 2s
  timeout: 250ms
  rise: 5
  fall: 7
  service: echo.v1.EchoService
rate_limit:
  enabled: true
  rps: 1200.5
  burst: 900
  per_client: true
  per_client_rps: 30
  per_client_burst: 60
circuit_breaker:
  enabled: true
  window: 1m30s
  buckets: 6
  min_requests: 12
  failure_ratio: 0.25
  open_timeout: 3s
  half_open_max: 2
logging:
  level: debug
  format: text
  sample_rate: 0.1
proxy:
  max_recv_msg_size: 1024
  max_send_msg_size: 2048
  shutdown_grace: 30s
`)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := Config{
		Listen:      ":7000",
		AdminListen: ":7001",
		Backends: []Backend{
			{ID: "a", Addr: "10.0.0.1:50051", Weight: 50},
			{ID: "b", Addr: "10.0.0.2:50051", Weight: 100},
		},
		Routing: Routing{HashHeader: "x-tenant", VirtualNodes: 64, MaxAttempts: 2, RetryPolicy: "unavailable"},
		Health: Health{
			Interval: 2 * time.Second,
			Timeout:  250 * time.Millisecond,
			Rise:     5,
			Fall:     7,
			Service:  "echo.v1.EchoService",
		},
		RateLimit: RateLimit{
			Enabled: true, RPS: 1200.5, Burst: 900,
			PerClient: true, PerClientRPS: 30, PerClientBurst: 60,
		},
		CircuitBreaker: CircuitBreaker{
			Enabled: true, Window: 90 * time.Second, Buckets: 6, MinRequests: 12,
			FailureRatio: 0.25, OpenTimeout: 3 * time.Second, HalfOpenMax: 2,
		},
		Logging: Logging{Level: "debug", Format: "text", SampleRate: 0.1},
		Proxy: Proxy{
			MaxRecvMsgSize: 1024,
			MaxSendMsgSize: 2048,
			ShutdownGrace:  30 * time.Second,
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadPartialYAMLKeepsDefaults(t *testing.T) {
	clearEnv(t)

	path := writeYAML(t, `
listen: ":9999"
backends:
  - id: only
    addr: 127.0.0.1:50051
health:
  timeout: 900ms
`)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := withBackends(Default(), Backend{ID: "only", Addr: "127.0.0.1:50051", Weight: 100})
	want.Listen = ":9999"
	want.Health.Timeout = 900 * time.Millisecond

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLoadErrors(t *testing.T) {
	clearEnv(t)

	const backends = "backends:\n  - {id: a, addr: \"h:1\"}\n"

	tests := []struct {
		name     string
		body     string
		path     string // overrides the temp file when set
		wantSubs []string
	}{
		{
			name:     "unknown top level field",
			body:     backends + "listne: \":1\"\n",
			wantSubs: []string{"listne", "not found"},
		},
		{
			name:     "unknown nested field",
			body:     backends + "health:\n  intervel: 1s\n",
			wantSubs: []string{"intervel", "not found"},
		},
		{
			name:     "unknown backend field",
			body:     "backends:\n  - {id: a, addr: \"h:1\", wieght: 3}\n",
			wantSubs: []string{"wieght", "not found"},
		},
		{
			name:     "bad duration names the field",
			body:     backends + "health:\n  interval: 1 fortnight\n",
			wantSubs: []string{"health.interval", "1 fortnight"},
		},
		{
			name:     "bad nested duration names the field",
			body:     backends + "proxy:\n  shutdown_grace: soon\n",
			wantSubs: []string{"proxy.shutdown_grace", "soon"},
		},
		{
			name:     "invalid yaml",
			body:     "backends: [\n",
			wantSubs: []string{"parse"},
		},
		{
			name:     "no backends",
			body:     "listen: \":1\"\n",
			wantSubs: []string{"backends", "at least one"},
		},
		{
			name:     "empty file has no backends",
			body:     "",
			wantSubs: []string{"backends"},
		},
		{
			name:     "validation errors are aggregated",
			body:     "backends:\n  - {id: a, addr: \"h:1\"}\n  - {id: a, addr: \"h:1\"}\nrouting:\n  max_attempts: 0\n",
			wantSubs: []string{"duplicate id", "duplicate addr", "routing.max_attempts"},
		},
		{
			name:     "missing file",
			path:     filepath.Join(t.TempDir(), "absent.yaml"),
			wantSubs: []string{"open", "absent.yaml"},
		},
		{
			name:     "empty path",
			path:     "   ",
			wantSubs: []string{"Default()"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.path
			if path == "" {
				path = writeYAML(t, tc.body)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatalf("Load(%q) succeeded, want error", path)
			}
			for _, sub := range tc.wantSubs {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q does not mention %q", err, sub)
				}
			}
		})
	}
}

func TestLoadBurstDerivation(t *testing.T) {
	clearEnv(t)

	tests := []struct {
		name               string
		body               string
		wantBurst          int
		wantPerClientBurst int
	}{
		{
			name:               "no rate_limit section uses 2x default rps",
			body:               "",
			wantBurst:          100000,
			wantPerClientBurst: 10000,
		},
		{
			name:               "rps without burst derives burst",
			body:               "rate_limit:\n  rps: 100\n",
			wantBurst:          200,
			wantPerClientBurst: 10000,
		},
		{
			name:               "fractional rps rounds up",
			body:               "rate_limit:\n  rps: 0.5\n  per_client_rps: 1.2\n",
			wantBurst:          1,
			wantPerClientBurst: 3,
		},
		{
			name:               "explicit burst wins",
			body:               "rate_limit:\n  rps: 100\n  burst: 7\n  per_client_rps: 100\n  per_client_burst: 9\n",
			wantBurst:          7,
			wantPerClientBurst: 9,
		},
		{
			name:               "explicit zero burst is rederived",
			body:               "rate_limit:\n  rps: 10\n  burst: 0\n",
			wantBurst:          20,
			wantPerClientBurst: 10000,
		},
		{
			name:               "zero rps still yields a usable burst",
			body:               "rate_limit:\n  rps: 0\n  per_client_rps: 0\n",
			wantBurst:          1,
			wantPerClientBurst: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeYAML(t, "backends:\n  - {id: a, addr: \"h:1\"}\n"+tc.body)
			got, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got.RateLimit.Burst != tc.wantBurst {
				t.Errorf("Burst = %d, want %d", got.RateLimit.Burst, tc.wantBurst)
			}
			if got.RateLimit.PerClientBurst != tc.wantPerClientBurst {
				t.Errorf("PerClientBurst = %d, want %d", got.RateLimit.PerClientBurst, tc.wantPerClientBurst)
			}
		})
	}
}

func TestLoadEnvOverridesYAML(t *testing.T) {
	clearEnv(t)

	path := writeYAML(t, `
listen: ":7000"
backends:
  - {id: fromfile, addr: "10.0.0.1:50051"}
routing:
  max_attempts: 2
`)

	t.Setenv(EnvListen, ":6000")
	t.Setenv(EnvBackends, "env-a=10.1.1.1:1,10.1.1.2:2")
	t.Setenv(EnvMaxAttempts, "5")

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Listen != ":6000" {
		t.Errorf("Listen = %q, want :6000", got.Listen)
	}
	if got.Routing.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", got.Routing.MaxAttempts)
	}
	want := []Backend{
		{ID: "env-a", Addr: "10.1.1.1:1", Weight: 100},
		{ID: "backend-2", Addr: "10.1.1.2:2", Weight: 100},
	}
	if !reflect.DeepEqual(got.Backends, want) {
		t.Errorf("Backends = %+v, want %+v (env replaces, never merges)", got.Backends, want)
	}
}

func TestApplyEnv(t *testing.T) {
	base := func() Config {
		return withBackends(Default(), Backend{ID: "file", Addr: "127.0.0.1:1", Weight: 100})
	}

	tests := []struct {
		name  string
		env   map[string]string
		check func(t *testing.T, c Config)
	}{
		{
			name:  "no variables leaves the config alone",
			env:   map[string]string{},
			check: func(t *testing.T, c Config) { mustEqual(t, c, base()) },
		},
		{
			name: "listen",
			env:  map[string]string{EnvListen: ":1234"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Listen = ":1234"
				mustEqual(t, c, want)
			},
		},
		{
			name: "admin listen",
			env:  map[string]string{EnvAdminListen: "127.0.0.1:9999"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.AdminListen = "127.0.0.1:9999"
				mustEqual(t, c, want)
			},
		},
		{
			name: "hash header",
			env:  map[string]string{EnvHashHeader: "x-tenant-id"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Routing.HashHeader = "x-tenant-id"
				mustEqual(t, c, want)
			},
		},
		{
			name: "log level is lowercased",
			env:  map[string]string{EnvLogLevel: "DEBUG"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Logging.Level = "debug"
				mustEqual(t, c, want)
			},
		},
		{
			name: "rate limit rps",
			env:  map[string]string{EnvRateLimitRPS: "250.5"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.RateLimit.RPS = 250.5
				mustEqual(t, c, want)
			},
		},
		{
			name: "max attempts",
			env:  map[string]string{EnvMaxAttempts: "9"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Routing.MaxAttempts = 9
				mustEqual(t, c, want)
			},
		},
		{
			name: "backends in id=addr form",
			env:  map[string]string{EnvBackends: "a=10.0.0.1:50051,b=10.0.0.2:50051"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Backends = []Backend{
					{ID: "a", Addr: "10.0.0.1:50051", Weight: 100},
					{ID: "b", Addr: "10.0.0.2:50051", Weight: 100},
				}
				mustEqual(t, c, want)
			},
		},
		{
			name: "backends in bare addr form get positional ids",
			env:  map[string]string{EnvBackends: "10.0.0.1:50051,10.0.0.2:50051,10.0.0.3:50051"},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Backends = []Backend{
					{ID: "backend-1", Addr: "10.0.0.1:50051", Weight: 100},
					{ID: "backend-2", Addr: "10.0.0.2:50051", Weight: 100},
					{ID: "backend-3", Addr: "10.0.0.3:50051", Weight: 100},
				}
				mustEqual(t, c, want)
			},
		},
		{
			name: "backends mixed forms keep positional ids",
			env:  map[string]string{EnvBackends: " 10.0.0.1:1 , named=10.0.0.2:2 ,10.0.0.3:3 "},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Backends = []Backend{
					{ID: "backend-1", Addr: "10.0.0.1:1", Weight: 100},
					{ID: "named", Addr: "10.0.0.2:2", Weight: 100},
					{ID: "backend-3", Addr: "10.0.0.3:3", Weight: 100},
				}
				mustEqual(t, c, want)
			},
		},
		{
			name: "blank values are treated as unset",
			env: map[string]string{
				EnvListen:   "  ",
				EnvBackends: "",
				EnvLogLevel: "",
			},
			check: func(t *testing.T, c Config) { mustEqual(t, c, base()) },
		},
		{
			name: "every variable at once",
			env: map[string]string{
				EnvListen:       ":1",
				EnvAdminListen:  ":2",
				EnvBackends:     "x=h:3",
				EnvLogLevel:     "warn",
				EnvRateLimitRPS: "10",
				EnvHashHeader:   "x-k",
				EnvMaxAttempts:  "1",
			},
			check: func(t *testing.T, c Config) {
				want := base()
				want.Listen = ":1"
				want.AdminListen = ":2"
				want.Backends = []Backend{{ID: "x", Addr: "h:3", Weight: 100}}
				want.Logging.Level = "warn"
				want.RateLimit.RPS = 10
				want.Routing.HashHeader = "x-k"
				want.Routing.MaxAttempts = 1
				mustEqual(t, c, want)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := base()
			if err := got.ApplyEnv(lookupFrom(tc.env)); err != nil {
				t.Fatalf("ApplyEnv: %v", err)
			}
			tc.check(t, got)
		})
	}
}

func TestApplyEnvErrors(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		wantSubs []string
	}{
		{
			name:     "rps is not a number",
			env:      map[string]string{EnvRateLimitRPS: "fast"},
			wantSubs: []string{EnvRateLimitRPS, "fast"},
		},
		{
			name:     "max attempts is not an integer",
			env:      map[string]string{EnvMaxAttempts: "3.5"},
			wantSubs: []string{EnvMaxAttempts, "3.5"},
		},
		{
			name:     "backend entry without an id",
			env:      map[string]string{EnvBackends: "=10.0.0.1:1"},
			wantSubs: []string{EnvBackends, "empty id"},
		},
		{
			name:     "backend entry without an addr",
			env:      map[string]string{EnvBackends: "a="},
			wantSubs: []string{EnvBackends, "empty addr"},
		},
		{
			name: "all failures are reported together",
			env: map[string]string{
				EnvRateLimitRPS: "fast",
				EnvMaxAttempts:  "many",
				EnvBackends:     "=h:1",
			},
			wantSubs: []string{EnvRateLimitRPS, EnvMaxAttempts, EnvBackends},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			err := c.ApplyEnv(lookupFrom(tc.env))
			if err == nil {
				t.Fatal("ApplyEnv succeeded, want error")
			}
			for _, sub := range tc.wantSubs {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q does not mention %q", err, sub)
				}
			}
		})
	}

	t.Run("bad value leaves the field untouched", func(t *testing.T) {
		c := Default()
		if err := c.ApplyEnv(lookupFrom(map[string]string{EnvRateLimitRPS: "fast"})); err == nil {
			t.Fatal("want error")
		}
		if c.RateLimit.RPS != Default().RateLimit.RPS {
			t.Errorf("RPS = %v, want the default preserved", c.RateLimit.RPS)
		}
	})

	t.Run("nil lookup", func(t *testing.T) {
		c := Default()
		if err := c.ApplyEnv(nil); err == nil {
			t.Fatal("ApplyEnv(nil) succeeded, want error")
		}
	})
}

func TestValidate(t *testing.T) {
	valid := func() Config {
		return withBackends(Default(),
			Backend{ID: "a", Addr: "10.0.0.1:50051", Weight: 100},
			Backend{ID: "b", Addr: "10.0.0.2:50051", Weight: 25},
		)
	}

	tests := []struct {
		name     string
		mutate   func(c *Config)
		wantSubs []string // empty means the config must be valid
	}{
		{name: "valid", mutate: func(*Config) {}},
		{
			name:     "empty listen",
			mutate:   func(c *Config) { c.Listen = "" },
			wantSubs: []string{"listen"},
		},
		{
			name:     "blank listen",
			mutate:   func(c *Config) { c.Listen = "   " },
			wantSubs: []string{"listen"},
		},
		{
			name:     "empty admin listen",
			mutate:   func(c *Config) { c.AdminListen = "" },
			wantSubs: []string{"admin_listen"},
		},
		{
			name:     "no backends",
			mutate:   func(c *Config) { c.Backends = nil },
			wantSubs: []string{"backends", "at least one"},
		},
		{
			name:     "duplicate ids",
			mutate:   func(c *Config) { c.Backends[1].ID = "a" },
			wantSubs: []string{"backends[1].id", "duplicate id", `"a"`},
		},
		{
			name:     "duplicate addrs",
			mutate:   func(c *Config) { c.Backends[1].Addr = "10.0.0.1:50051" },
			wantSubs: []string{"backends[1].addr", "duplicate addr"},
		},
		{
			name:     "empty backend id",
			mutate:   func(c *Config) { c.Backends[0].ID = "" },
			wantSubs: []string{"backends[0].id"},
		},
		{
			name:     "empty backend addr",
			mutate:   func(c *Config) { c.Backends[0].Addr = "" },
			wantSubs: []string{"backends[0].addr"},
		},
		{
			name:     "zero weight",
			mutate:   func(c *Config) { c.Backends[1].Weight = 0 },
			wantSubs: []string{"backends[1].weight", "> 0", "0"},
		},
		{
			name:     "negative weight",
			mutate:   func(c *Config) { c.Backends[0].Weight = -5 },
			wantSubs: []string{"backends[0].weight", "-5"},
		},
		{
			name:     "zero virtual nodes",
			mutate:   func(c *Config) { c.Routing.VirtualNodes = 0 },
			wantSubs: []string{"routing.virtual_nodes", "> 0"},
		},
		{
			name:     "negative virtual nodes",
			mutate:   func(c *Config) { c.Routing.VirtualNodes = -1 },
			wantSubs: []string{"routing.virtual_nodes", "-1"},
		},
		{
			name:     "max attempts zero",
			mutate:   func(c *Config) { c.Routing.MaxAttempts = 0 },
			wantSubs: []string{"routing.max_attempts", ">= 1"},
		},
		{
			name:     "unknown retry policy",
			mutate:   func(c *Config) { c.Routing.RetryPolicy = "sticky" },
			wantSubs: []string{"routing.retry_policy", "sticky"},
		},
		{name: "retry policy unavailable", mutate: func(c *Config) { c.Routing.RetryPolicy = RetryUnavailable }},
		{name: "retry policy none", mutate: func(c *Config) { c.Routing.RetryPolicy = RetryNone }},
		{
			name:     "rise zero",
			mutate:   func(c *Config) { c.Health.Rise = 0 },
			wantSubs: []string{"health.rise", ">= 1"},
		},
		{
			name:     "fall zero",
			mutate:   func(c *Config) { c.Health.Fall = 0 },
			wantSubs: []string{"health.fall", ">= 1"},
		},
		{
			name:     "health interval zero",
			mutate:   func(c *Config) { c.Health.Interval = 0 },
			wantSubs: []string{"health.interval", "> 0"},
		},
		{
			name:     "health timeout zero",
			mutate:   func(c *Config) { c.Health.Timeout = 0 },
			wantSubs: []string{"health.timeout", "> 0"},
		},
		{
			name:     "failure ratio zero",
			mutate:   func(c *Config) { c.CircuitBreaker.FailureRatio = 0 },
			wantSubs: []string{"circuit_breaker.failure_ratio", "(0,1]"},
		},
		{
			name:     "failure ratio above one",
			mutate:   func(c *Config) { c.CircuitBreaker.FailureRatio = 1.5 },
			wantSubs: []string{"circuit_breaker.failure_ratio", "1.5"},
		},
		{name: "failure ratio exactly one", mutate: func(c *Config) { c.CircuitBreaker.FailureRatio = 1 }},
		{
			name:     "buckets zero",
			mutate:   func(c *Config) { c.CircuitBreaker.Buckets = 0 },
			wantSubs: []string{"circuit_breaker.buckets", ">= 1"},
		},
		{
			name:     "min requests zero",
			mutate:   func(c *Config) { c.CircuitBreaker.MinRequests = 0 },
			wantSubs: []string{"circuit_breaker.min_requests"},
		},
		{
			name:     "half open max zero",
			mutate:   func(c *Config) { c.CircuitBreaker.HalfOpenMax = 0 },
			wantSubs: []string{"circuit_breaker.half_open_max"},
		},
		{
			name:     "circuit window zero",
			mutate:   func(c *Config) { c.CircuitBreaker.Window = 0 },
			wantSubs: []string{"circuit_breaker.window"},
		},
		{
			name:     "open timeout zero",
			mutate:   func(c *Config) { c.CircuitBreaker.OpenTimeout = 0 },
			wantSubs: []string{"circuit_breaker.open_timeout"},
		},
		{
			name:     "rate limit enabled without rps",
			mutate:   func(c *Config) { c.RateLimit.Enabled = true; c.RateLimit.RPS = 0 },
			wantSubs: []string{"rate_limit.rps"},
		},
		{
			name:     "rate limit enabled without burst",
			mutate:   func(c *Config) { c.RateLimit.Enabled = true; c.RateLimit.Burst = 0 },
			wantSubs: []string{"rate_limit.burst"},
		},
		{
			name: "per client enabled without per client rps",
			mutate: func(c *Config) {
				c.RateLimit.Enabled = true
				c.RateLimit.PerClient = true
				c.RateLimit.PerClientRPS = 0
			},
			wantSubs: []string{"rate_limit.per_client_rps"},
		},
		{
			name:   "rate limit disabled ignores its numbers",
			mutate: func(c *Config) { c.RateLimit.RPS = 0; c.RateLimit.Burst = 0 },
		},
		{
			name:     "unknown log level",
			mutate:   func(c *Config) { c.Logging.Level = "trace" },
			wantSubs: []string{"logging.level", "trace"},
		},
		{
			name:     "unknown log format",
			mutate:   func(c *Config) { c.Logging.Format = "logfmt" },
			wantSubs: []string{"logging.format", "logfmt"},
		},
		{
			name:     "sample rate above one",
			mutate:   func(c *Config) { c.Logging.SampleRate = 1.5 },
			wantSubs: []string{"logging.sample_rate", "1.5"},
		},
		{
			name:     "sample rate negative",
			mutate:   func(c *Config) { c.Logging.SampleRate = -0.1 },
			wantSubs: []string{"logging.sample_rate"},
		},
		{name: "sample rate zero", mutate: func(c *Config) { c.Logging.SampleRate = 0 }},
		{
			name:     "max recv msg size zero",
			mutate:   func(c *Config) { c.Proxy.MaxRecvMsgSize = 0 },
			wantSubs: []string{"proxy.max_recv_msg_size"},
		},
		{
			name:     "max send msg size negative",
			mutate:   func(c *Config) { c.Proxy.MaxSendMsgSize = -1 },
			wantSubs: []string{"proxy.max_send_msg_size"},
		},
		{
			name:     "shutdown grace zero",
			mutate:   func(c *Config) { c.Proxy.ShutdownGrace = 0 },
			wantSubs: []string{"proxy.shutdown_grace"},
		},
		{
			name: "several problems are reported at once",
			mutate: func(c *Config) {
				c.Listen = ""
				c.Backends[1].ID = "a"
				c.Routing.MaxAttempts = 0
				c.Health.Rise = 0
				c.CircuitBreaker.FailureRatio = 0
			},
			wantSubs: []string{
				"listen", "duplicate id", "routing.max_attempts",
				"health.rise", "circuit_breaker.failure_ratio",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := valid()
			tc.mutate(&c)
			err := c.Validate()

			if len(tc.wantSubs) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			for _, sub := range tc.wantSubs {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q does not mention %q", err, sub)
				}
			}
		})
	}
}

func TestValidateAggregatesWithErrorsJoin(t *testing.T) {
	c := Default() // no backends, plus two more broken fields
	c.Listen = ""
	c.Routing.VirtualNodes = 0

	err := c.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error")
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("Validate() returned %T, want an errors.Join aggregate", err)
	}
	if got := len(joined.Unwrap()); got != 3 {
		t.Errorf("aggregate holds %d errors, want 3: %v", got, err)
	}
}

func TestSlogLevel(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{" Debug ", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"", slog.LevelInfo},
		{"nonsense", slog.LevelInfo},
	}

	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			if got := (Logging{Level: tc.level}).SlogLevel(); got != tc.want {
				t.Errorf("Logging{Level: %q}.SlogLevel() = %v, want %v", tc.level, got, tc.want)
			}
		})
	}
}

func TestParseBackends(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []Backend
		wantErr bool
	}{
		{
			name: "id=addr",
			in:   "a=h1:1,b=h2:2",
			want: []Backend{{ID: "a", Addr: "h1:1", Weight: 100}, {ID: "b", Addr: "h2:2", Weight: 100}},
		},
		{
			name: "bare addrs",
			in:   "h1:1,h2:2",
			want: []Backend{{ID: "backend-1", Addr: "h1:1", Weight: 100}, {ID: "backend-2", Addr: "h2:2", Weight: 100}},
		},
		{
			name: "ipv6 addr keeps its colons",
			in:   "v6=[::1]:50051",
			want: []Backend{{ID: "v6", Addr: "[::1]:50051", Weight: 100}},
		},
		{
			name: "trailing and empty entries are skipped but do not shift ids",
			in:   "h1:1,,h3:3,",
			want: []Backend{{ID: "backend-1", Addr: "h1:1", Weight: 100}, {ID: "backend-3", Addr: "h3:3", Weight: 100}},
		},
		{
			name: "only separators yields nothing",
			in:   ",,",
			want: []Backend{},
		},
		{name: "empty id", in: "=h:1", wantErr: true},
		{name: "empty addr", in: "a=", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseBackends(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseBackends(%q) = %+v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBackends(%q): %v", tc.in, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ParseBackends(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

func mustEqual(t *testing.T, got, want Config) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("config =\n%+v\nwant\n%+v", got, want)
	}
}
