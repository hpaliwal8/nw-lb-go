// Package config resolves the load balancer's configuration from three layers,
// applied in order: built-in defaults, an optional YAML file, and environment
// overrides. Every layer replaces values wholesale — nothing is merged.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Environment variables recognised by ApplyEnv. A variable that is unset, or set
// to a blank string, leaves the underlying value untouched; container runtimes
// routinely inject empty variables and those must not wipe a valid config.
const (
	EnvListen       = "LB_LISTEN"
	EnvAdminListen  = "LB_ADMIN_LISTEN"
	EnvBackends     = "LB_BACKENDS"
	EnvLogLevel     = "LB_LOG_LEVEL"
	EnvRateLimitRPS = "LB_RATE_LIMIT_RPS"
	EnvHashHeader   = "LB_HASH_HEADER"
	EnvMaxAttempts  = "LB_MAX_ATTEMPTS"
)

// Routing.RetryPolicy values: which upstream failures may be replayed on another backend.
//
// A gRPC status alone cannot distinguish "the backend never received this" from "the backend
// processed it and then died", so replaying anything observed after the request was sent turns the
// proxy into at-least-once delivery. RetryConnectFailure is the default because it is the only
// setting that is safe for a method the caller did not declare idempotent.
const (
	// RetryConnectFailure replays only failures that happened while establishing the upstream
	// stream, where the request provably never reached a backend that could act on it.
	RetryConnectFailure = "connect-failure"
	// RetryUnavailable additionally replays Unavailable, ResourceExhausted and DeadlineExceeded
	// reported after the request was sent. Higher availability, at-least-once delivery: only
	// choose it when every method behind this balancer is idempotent.
	RetryUnavailable = "unavailable"
	// RetryNone never replays.
	RetryNone = "none"
)

// DefaultWeight is applied to any backend that omits an explicit weight.
const DefaultWeight = 100

type Config struct {
	Listen         string         `yaml:"listen"`
	AdminListen    string         `yaml:"admin_listen"`
	Backends       []Backend      `yaml:"backends"`
	Routing        Routing        `yaml:"routing"`
	Health         Health         `yaml:"health"`
	RateLimit      RateLimit      `yaml:"rate_limit"`
	CircuitBreaker CircuitBreaker `yaml:"circuit_breaker"`
	Logging        Logging        `yaml:"logging"`
	Proxy          Proxy          `yaml:"proxy"`
}

type Backend struct {
	ID     string `yaml:"id"`
	Addr   string `yaml:"addr"`
	Weight int    `yaml:"weight"`
}

type Routing struct {
	HashHeader   string `yaml:"hash_header"`
	VirtualNodes int    `yaml:"virtual_nodes"`
	MaxAttempts  int    `yaml:"max_attempts"`
	RetryPolicy  string `yaml:"retry_policy"`
}

type Health struct {
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
	Rise     int           `yaml:"rise"`
	Fall     int           `yaml:"fall"`
	Service  string        `yaml:"service"`
}

type RateLimit struct {
	Enabled        bool    `yaml:"enabled"`
	RPS            float64 `yaml:"rps"`
	Burst          int     `yaml:"burst"`
	PerClient      bool    `yaml:"per_client"`
	PerClientRPS   float64 `yaml:"per_client_rps"`
	PerClientBurst int     `yaml:"per_client_burst"`
}

type CircuitBreaker struct {
	Enabled      bool          `yaml:"enabled"`
	Window       time.Duration `yaml:"window"`
	Buckets      int           `yaml:"buckets"`
	MinRequests  int           `yaml:"min_requests"`
	FailureRatio float64       `yaml:"failure_ratio"`
	OpenTimeout  time.Duration `yaml:"open_timeout"`
	HalfOpenMax  int           `yaml:"half_open_max"`
}

type Logging struct {
	Level      string  `yaml:"level"`
	Format     string  `yaml:"format"`
	SampleRate float64 `yaml:"sample_rate"`
}

type Proxy struct {
	MaxRecvMsgSize int           `yaml:"max_recv_msg_size"`
	MaxSendMsgSize int           `yaml:"max_send_msg_size"`
	ShutdownGrace  time.Duration `yaml:"shutdown_grace"`
}

// Default returns a fully populated configuration with no backends. It is the
// base every other layer is applied on top of, and the only sane starting point
// for callers that have no config file.
func Default() Config {
	return Config{
		Listen:      ":8080",
		AdminListen: ":9090",
		Routing: Routing{
			HashHeader:   "x-session-id",
			VirtualNodes: 200,
			MaxAttempts:  3,
			RetryPolicy:  RetryConnectFailure,
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
		Logging: Logging{
			Level:      "info",
			Format:     "json",
			SampleRate: 1,
		},
		Proxy: Proxy{
			MaxRecvMsgSize: 16 << 20,
			MaxSendMsgSize: 16 << 20,
			ShutdownGrace:  10 * time.Second,
		},
	}
}

// Load reads path as YAML on top of Default(), applies environment overrides and
// validates the result. An empty path is a programming error: callers with no
// config file must use Default() directly.
func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, errors.New("config: Load needs a path; use Default() when there is no config file")
	}

	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	raw := newRawConfig(Default())
	dec := yaml.NewDecoder(f)
	// Typos in a config file are the most common way to silently run with the
	// wrong settings, so unknown keys are hard errors.
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
	}

	c, err := raw.config()
	if err != nil {
		return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := c.ApplyEnv(os.LookupEnv); err != nil {
		return Config{}, fmt.Errorf("config: environment: %w", err)
	}
	c.applyDerivedDefaults()
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("config: %s: %w", path, err)
	}
	return c, nil
}

// ApplyEnv overlays the LB_* environment variables using the supplied lookup
// (os.LookupEnv in production, a stub in tests). Overrides replace values; they
// never merge with what is already there.
func (c *Config) ApplyEnv(lookup func(string) (string, bool)) error {
	if lookup == nil {
		return errors.New("config: ApplyEnv needs a lookup function")
	}

	var errs []error
	if v, ok := envValue(lookup, EnvListen); ok {
		c.Listen = v
	}
	if v, ok := envValue(lookup, EnvAdminListen); ok {
		c.AdminListen = v
	}
	if v, ok := envValue(lookup, EnvHashHeader); ok {
		c.Routing.HashHeader = v
	}
	if v, ok := envValue(lookup, EnvLogLevel); ok {
		c.Logging.Level = strings.ToLower(v)
	}
	if v, ok := envValue(lookup, EnvBackends); ok {
		backends, err := ParseBackends(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s=%q: %w", EnvBackends, v, err))
		} else {
			c.Backends = backends
		}
	}
	if v, ok := envValue(lookup, EnvRateLimitRPS); ok {
		rps, err := strconv.ParseFloat(v, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s=%q: not a number: %w", EnvRateLimitRPS, v, err))
		} else {
			c.RateLimit.RPS = rps
		}
	}
	if v, ok := envValue(lookup, EnvMaxAttempts); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s=%q: not an integer: %w", EnvMaxAttempts, v, err))
		} else {
			c.Routing.MaxAttempts = n
		}
	}
	return errors.Join(errs...)
}

// Validate reports every problem it finds at once: a half-fixed config that
// fails again on the next start wastes an operator's time.
func (c Config) Validate() error {
	var errs []error

	if strings.TrimSpace(c.Listen) == "" {
		errs = append(errs, errors.New("listen: must not be empty"))
	}
	if strings.TrimSpace(c.AdminListen) == "" {
		errs = append(errs, errors.New("admin_listen: must not be empty"))
	}

	if len(c.Backends) == 0 {
		errs = append(errs, errors.New("backends: at least one backend is required"))
	}
	ids := make(map[string]int, len(c.Backends))
	addrs := make(map[string]int, len(c.Backends))
	for i, b := range c.Backends {
		switch {
		case b.ID == "":
			errs = append(errs, fmt.Errorf("backends[%d].id: must not be empty", i))
		default:
			if first, dup := ids[b.ID]; dup {
				errs = append(errs, fmt.Errorf("backends[%d].id: duplicate id %q, already used by backends[%d]", i, b.ID, first))
			} else {
				ids[b.ID] = i
			}
		}
		switch {
		case b.Addr == "":
			errs = append(errs, fmt.Errorf("backends[%d].addr: must not be empty", i))
		default:
			if first, dup := addrs[b.Addr]; dup {
				errs = append(errs, fmt.Errorf("backends[%d].addr: duplicate addr %q, already used by backends[%d]", i, b.Addr, first))
			} else {
				addrs[b.Addr] = i
			}
		}
		if b.Weight <= 0 {
			errs = append(errs, fmt.Errorf("backends[%d].weight: must be > 0, got %d", i, b.Weight))
		}
	}

	switch c.Routing.RetryPolicy {
	case RetryConnectFailure, RetryUnavailable, RetryNone:
	default:
		errs = append(errs, fmt.Errorf("routing.retry_policy: must be one of %q, %q, %q, got %q",
			RetryConnectFailure, RetryUnavailable, RetryNone, c.Routing.RetryPolicy))
	}
	if c.Routing.VirtualNodes <= 0 {
		errs = append(errs, fmt.Errorf("routing.virtual_nodes: must be > 0, got %d", c.Routing.VirtualNodes))
	}
	if c.Routing.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("routing.max_attempts: must be >= 1, got %d", c.Routing.MaxAttempts))
	}

	if c.Health.Interval <= 0 {
		errs = append(errs, fmt.Errorf("health.interval: must be > 0, got %s", c.Health.Interval))
	}
	if c.Health.Timeout <= 0 {
		errs = append(errs, fmt.Errorf("health.timeout: must be > 0, got %s", c.Health.Timeout))
	}
	if c.Health.Rise < 1 {
		errs = append(errs, fmt.Errorf("health.rise: must be >= 1, got %d", c.Health.Rise))
	}
	if c.Health.Fall < 1 {
		errs = append(errs, fmt.Errorf("health.fall: must be >= 1, got %d", c.Health.Fall))
	}

	if c.RateLimit.Enabled {
		if !(c.RateLimit.RPS > 0) {
			errs = append(errs, fmt.Errorf("rate_limit.rps: must be > 0 when enabled, got %v", c.RateLimit.RPS))
		}
		if c.RateLimit.Burst < 1 {
			errs = append(errs, fmt.Errorf("rate_limit.burst: must be >= 1 when enabled, got %d", c.RateLimit.Burst))
		}
		if c.RateLimit.PerClient {
			if !(c.RateLimit.PerClientRPS > 0) {
				errs = append(errs, fmt.Errorf("rate_limit.per_client_rps: must be > 0 when per_client is set, got %v", c.RateLimit.PerClientRPS))
			}
			if c.RateLimit.PerClientBurst < 1 {
				errs = append(errs, fmt.Errorf("rate_limit.per_client_burst: must be >= 1 when per_client is set, got %d", c.RateLimit.PerClientBurst))
			}
		}
	}

	if c.CircuitBreaker.Window <= 0 {
		errs = append(errs, fmt.Errorf("circuit_breaker.window: must be > 0, got %s", c.CircuitBreaker.Window))
	}
	if c.CircuitBreaker.Buckets < 1 {
		errs = append(errs, fmt.Errorf("circuit_breaker.buckets: must be >= 1, got %d", c.CircuitBreaker.Buckets))
	}
	if c.CircuitBreaker.MinRequests < 1 {
		errs = append(errs, fmt.Errorf("circuit_breaker.min_requests: must be >= 1, got %d", c.CircuitBreaker.MinRequests))
	}
	if !(c.CircuitBreaker.FailureRatio > 0 && c.CircuitBreaker.FailureRatio <= 1) {
		errs = append(errs, fmt.Errorf("circuit_breaker.failure_ratio: must be in (0,1], got %v", c.CircuitBreaker.FailureRatio))
	}
	if c.CircuitBreaker.OpenTimeout <= 0 {
		errs = append(errs, fmt.Errorf("circuit_breaker.open_timeout: must be > 0, got %s", c.CircuitBreaker.OpenTimeout))
	}
	if c.CircuitBreaker.HalfOpenMax < 1 {
		errs = append(errs, fmt.Errorf("circuit_breaker.half_open_max: must be >= 1, got %d", c.CircuitBreaker.HalfOpenMax))
	}

	switch strings.ToLower(c.Logging.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("logging.level: must be one of debug, info, warn, error, got %q", c.Logging.Level))
	}
	switch strings.ToLower(c.Logging.Format) {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("logging.format: must be json or text, got %q", c.Logging.Format))
	}
	if c.Logging.SampleRate < 0 || c.Logging.SampleRate > 1 {
		errs = append(errs, fmt.Errorf("logging.sample_rate: must be in [0,1], got %v", c.Logging.SampleRate))
	}

	if c.Proxy.MaxRecvMsgSize <= 0 {
		errs = append(errs, fmt.Errorf("proxy.max_recv_msg_size: must be > 0, got %d", c.Proxy.MaxRecvMsgSize))
	}
	if c.Proxy.MaxSendMsgSize <= 0 {
		errs = append(errs, fmt.Errorf("proxy.max_send_msg_size: must be > 0, got %d", c.Proxy.MaxSendMsgSize))
	}
	if c.Proxy.ShutdownGrace <= 0 {
		errs = append(errs, fmt.Errorf("proxy.shutdown_grace: must be > 0, got %s", c.Proxy.ShutdownGrace))
	}

	return errors.Join(errs...)
}

// SlogLevel maps the configured level name onto slog. Unrecognised names fall
// back to info; Validate is what rejects them.
func (c Logging) SlogLevel() slog.Level {
	switch strings.ToLower(strings.TrimSpace(c.Level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ParseBackends reads the LB_BACKENDS syntax: comma-separated entries that are
// either "id=addr" or a bare "addr", the latter taking the positional id
// "backend-N". Weights are not expressible here and default to DefaultWeight.
func ParseBackends(s string) ([]Backend, error) {
	fields := strings.Split(s, ",")
	backends := make([]Backend, 0, len(fields))
	var errs []error
	for i, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		id, addr, hasID := strings.Cut(f, "=")
		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if !hasID {
			addr = id
			id = fmt.Sprintf("backend-%d", i+1)
		}
		if id == "" {
			errs = append(errs, fmt.Errorf("entry %d (%q): empty id", i+1, f))
			continue
		}
		if addr == "" {
			errs = append(errs, fmt.Errorf("entry %d (%q): empty addr", i+1, f))
			continue
		}
		backends = append(backends, Backend{ID: id, Addr: addr, Weight: DefaultWeight})
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return backends, nil
}

// applyDerivedDefaults fills in values that depend on other values, so a file
// that sets rps but not burst still ends up with a usable burst.
func (c *Config) applyDerivedDefaults() {
	for i := range c.Backends {
		c.Backends[i].ID = strings.TrimSpace(c.Backends[i].ID)
		c.Backends[i].Addr = strings.TrimSpace(c.Backends[i].Addr)
		if c.Backends[i].Weight == 0 {
			c.Backends[i].Weight = DefaultWeight
		}
	}
	if c.RateLimit.Burst == 0 {
		c.RateLimit.Burst = burstFor(c.RateLimit.RPS)
	}
	if c.RateLimit.PerClientBurst == 0 {
		c.RateLimit.PerClientBurst = burstFor(c.RateLimit.PerClientRPS)
	}
}

// burstFor gives a bucket two seconds of headroom, clamped so an absurd rps
// cannot overflow the int the limiter takes.
func burstFor(rps float64) int {
	v := math.Ceil(2 * rps)
	switch {
	case math.IsNaN(v) || v < 1:
		return 1
	case v > math.MaxInt32:
		return math.MaxInt32
	default:
		return int(v)
	}
}

func envValue(lookup func(string) (string, bool), key string) (string, bool) {
	v, ok := lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

// The raw* types exist because yaml.v3 decodes "1s" as a string, not as a
// time.Duration. Decoding the whole document into one shadow tree — rather than
// giving each duration-bearing struct an UnmarshalYAML — keeps the single
// KnownFields(true) decoder in charge of every nested mapping; a per-struct
// yaml.Node.Decode would silently drop that check.
type rawConfig struct {
	Listen         string       `yaml:"listen"`
	AdminListen    string       `yaml:"admin_listen"`
	Backends       []Backend    `yaml:"backends"`
	Routing        Routing      `yaml:"routing"`
	Health         rawHealth    `yaml:"health"`
	RateLimit      rawRateLimit `yaml:"rate_limit"`
	CircuitBreaker rawCircuit   `yaml:"circuit_breaker"`
	Logging        Logging      `yaml:"logging"`
	Proxy          rawProxy     `yaml:"proxy"`
}

type rawHealth struct {
	Interval string `yaml:"interval"`
	Timeout  string `yaml:"timeout"`
	Rise     int    `yaml:"rise"`
	Fall     int    `yaml:"fall"`
	Service  string `yaml:"service"`
}

type rawRateLimit struct {
	Enabled        bool    `yaml:"enabled"`
	RPS            float64 `yaml:"rps"`
	Burst          int     `yaml:"burst"`
	PerClient      bool    `yaml:"per_client"`
	PerClientRPS   float64 `yaml:"per_client_rps"`
	PerClientBurst int     `yaml:"per_client_burst"`
}

type rawCircuit struct {
	Enabled      bool    `yaml:"enabled"`
	Window       string  `yaml:"window"`
	Buckets      int     `yaml:"buckets"`
	MinRequests  int     `yaml:"min_requests"`
	FailureRatio float64 `yaml:"failure_ratio"`
	OpenTimeout  string  `yaml:"open_timeout"`
	HalfOpenMax  int     `yaml:"half_open_max"`
}

type rawProxy struct {
	MaxRecvMsgSize int    `yaml:"max_recv_msg_size"`
	MaxSendMsgSize int    `yaml:"max_send_msg_size"`
	ShutdownGrace  string `yaml:"shutdown_grace"`
}

// newRawConfig seeds the shadow tree from base so that keys absent from the YAML
// keep their default. Burst fields are deliberately left at zero: zero is how
// applyDerivedDefaults recognises "not specified" and recomputes them from rps.
func newRawConfig(base Config) rawConfig {
	return rawConfig{
		Listen:      base.Listen,
		AdminListen: base.AdminListen,
		Backends:    base.Backends,
		Routing:     base.Routing,
		Health: rawHealth{
			Interval: base.Health.Interval.String(),
			Timeout:  base.Health.Timeout.String(),
			Rise:     base.Health.Rise,
			Fall:     base.Health.Fall,
			Service:  base.Health.Service,
		},
		RateLimit: rawRateLimit{
			Enabled:      base.RateLimit.Enabled,
			RPS:          base.RateLimit.RPS,
			PerClient:    base.RateLimit.PerClient,
			PerClientRPS: base.RateLimit.PerClientRPS,
		},
		CircuitBreaker: rawCircuit{
			Enabled:      base.CircuitBreaker.Enabled,
			Window:       base.CircuitBreaker.Window.String(),
			Buckets:      base.CircuitBreaker.Buckets,
			MinRequests:  base.CircuitBreaker.MinRequests,
			FailureRatio: base.CircuitBreaker.FailureRatio,
			OpenTimeout:  base.CircuitBreaker.OpenTimeout.String(),
			HalfOpenMax:  base.CircuitBreaker.HalfOpenMax,
		},
		Logging: base.Logging,
		Proxy: rawProxy{
			MaxRecvMsgSize: base.Proxy.MaxRecvMsgSize,
			MaxSendMsgSize: base.Proxy.MaxSendMsgSize,
			ShutdownGrace:  base.Proxy.ShutdownGrace.String(),
		},
	}
}

func (r rawConfig) config() (Config, error) {
	c := Config{
		Listen:      r.Listen,
		AdminListen: r.AdminListen,
		Backends:    r.Backends,
		Routing:     r.Routing,
		Health: Health{
			Rise:    r.Health.Rise,
			Fall:    r.Health.Fall,
			Service: r.Health.Service,
		},
		RateLimit: RateLimit{
			Enabled:        r.RateLimit.Enabled,
			RPS:            r.RateLimit.RPS,
			Burst:          r.RateLimit.Burst,
			PerClient:      r.RateLimit.PerClient,
			PerClientRPS:   r.RateLimit.PerClientRPS,
			PerClientBurst: r.RateLimit.PerClientBurst,
		},
		CircuitBreaker: CircuitBreaker{
			Enabled:      r.CircuitBreaker.Enabled,
			Buckets:      r.CircuitBreaker.Buckets,
			MinRequests:  r.CircuitBreaker.MinRequests,
			FailureRatio: r.CircuitBreaker.FailureRatio,
			HalfOpenMax:  r.CircuitBreaker.HalfOpenMax,
		},
		Logging: r.Logging,
		Proxy: Proxy{
			MaxRecvMsgSize: r.Proxy.MaxRecvMsgSize,
			MaxSendMsgSize: r.Proxy.MaxSendMsgSize,
		},
	}

	durations := []struct {
		field string
		src   string
		dst   *time.Duration
	}{
		{"health.interval", r.Health.Interval, &c.Health.Interval},
		{"health.timeout", r.Health.Timeout, &c.Health.Timeout},
		{"circuit_breaker.window", r.CircuitBreaker.Window, &c.CircuitBreaker.Window},
		{"circuit_breaker.open_timeout", r.CircuitBreaker.OpenTimeout, &c.CircuitBreaker.OpenTimeout},
		{"proxy.shutdown_grace", r.Proxy.ShutdownGrace, &c.Proxy.ShutdownGrace},
	}
	var errs []error
	for _, d := range durations {
		v, err := time.ParseDuration(strings.TrimSpace(d.src))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: invalid duration %q (want a value like \"1s\" or \"500ms\")", d.field, d.src))
			continue
		}
		*d.dst = v
	}
	return c, errors.Join(errs...)
}
