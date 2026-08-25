# Package contract (single source of truth)

Module: `github.com/hitanshpaliwal/nw-lb-go`. Go 1.25, grpc-go **v1.83.1**, protobuf v1.36.12,
prometheus/client_golang v1.24.1, `golang.org/x/time/rate`, `gopkg.in/yaml.v3`,
`github.com/cespare/xxhash/v2`. Logging is stdlib `log/slog`. **No other dependencies.**

Every package below MUST export exactly these identifiers with exactly these signatures.
Implementations may add unexported helpers and extra exported helpers, but must not change or
drop anything listed here — other packages are written in parallel against this file.

Generated protobuf lives in `gen/echo/v1` (package `echov1`) and already exists. Do not regenerate.

## Dependency waves (a package may only import internal packages from an earlier wave)

1. `internal/config`, `internal/hashring`, `internal/circuit`, `internal/ratelimit`, `internal/metrics`
2. `internal/pool`, `internal/middleware`
3. `internal/health`, `internal/balancer`
4. `internal/proxy`
5. `cmd/backend`, `cmd/lb`, `cmd/loadgen`

---

## internal/config

```go
package config

type Config struct {
	Listen         string          `yaml:"listen"`          // default ":8080"
	AdminListen    string          `yaml:"admin_listen"`    // default ":9090"
	Backends       []Backend       `yaml:"backends"`
	Routing        Routing         `yaml:"routing"`
	Health         Health          `yaml:"health"`
	RateLimit      RateLimit       `yaml:"rate_limit"`
	CircuitBreaker CircuitBreaker  `yaml:"circuit_breaker"`
	Logging        Logging         `yaml:"logging"`
	Proxy          Proxy           `yaml:"proxy"`
}

type Backend struct {
	ID     string `yaml:"id"`
	Addr   string `yaml:"addr"`
	Weight int    `yaml:"weight"` // default 100, must be > 0
}

type Routing struct {
	HashHeader   string `yaml:"hash_header"`   // metadata key, default "x-session-id"
	VirtualNodes int    `yaml:"virtual_nodes"` // default 200
	MaxAttempts  int    `yaml:"max_attempts"`  // default 3, >= 1
	RetryPolicy  string `yaml:"retry_policy"`  // "connect-failure" (default) | "unavailable" | "none"
}

type Health struct {
	Interval time.Duration `yaml:"interval"` // default 1s
	Timeout  time.Duration `yaml:"timeout"`  // default 500ms
	Rise     int           `yaml:"rise"`     // default 2
	Fall     int           `yaml:"fall"`     // default 3
	Service  string        `yaml:"service"`  // grpc health service name, default ""
}

type RateLimit struct {
	Enabled        bool    `yaml:"enabled"`
	RPS            float64 `yaml:"rps"`              // global, default 50000
	Burst          int     `yaml:"burst"`            // default 2x RPS
	PerClient      bool    `yaml:"per_client"`
	PerClientRPS   float64 `yaml:"per_client_rps"`   // default 5000
	PerClientBurst int     `yaml:"per_client_burst"` // default 10000
}

type CircuitBreaker struct {
	Enabled      bool          `yaml:"enabled"`
	Window       time.Duration `yaml:"window"`        // default 10s
	Buckets      int           `yaml:"buckets"`       // default 10
	MinRequests  int           `yaml:"min_requests"`  // default 20
	FailureRatio float64       `yaml:"failure_ratio"` // default 0.5
	OpenTimeout  time.Duration `yaml:"open_timeout"`  // default 5s
	HalfOpenMax  int           `yaml:"half_open_max"` // default 5
}

type Logging struct {
	Level      string  `yaml:"level"`       // debug|info|warn|error, default info
	Format     string  `yaml:"format"`      // json|text, default json
	SampleRate float64 `yaml:"sample_rate"` // 0..1 fraction of OK RPCs logged; errors always logged. default 1
}

type Proxy struct {
	MaxRecvMsgSize int           `yaml:"max_recv_msg_size"` // default 16<<20
	MaxSendMsgSize int           `yaml:"max_send_msg_size"` // default 16<<20
	ShutdownGrace  time.Duration `yaml:"shutdown_grace"`    // default 10s
}

func Default() Config                       // fully populated, zero backends
func Load(path string) (Config, error)      // YAML over Default(), then ApplyEnv, then Validate
func (c *Config) ApplyEnv(lookup func(string) (string, bool)) error // LB_LISTEN, LB_ADMIN_LISTEN,
    // LB_BACKENDS ("id=addr,id=addr" or "addr,addr"), LB_LOG_LEVEL, LB_RATE_LIMIT_RPS,
    // LB_HASH_HEADER, LB_MAX_ATTEMPTS
func (c Config) Validate() error            // non-empty listen, >=1 backend, unique ids+addrs,
                                            // weight>0, virtual_nodes>0, max_attempts>=1,
                                            // rise/fall>=1, failure_ratio in (0,1], buckets>=1
func (c Logging) SlogLevel() slog.Level
```

## internal/hashring

Consistent hash ring with weighted virtual nodes. Reads must be lock-free: keep an immutable
snapshot behind an `atomic.Pointer`. Hash: `xxhash.Sum64String`. Virtual node key format is
`fmt.Sprintf("%s#%d", memberID, i)`.

```go
package hashring

type Member struct {
	ID     string
	Weight int // virtual nodes = VirtualNodes * Weight / 100, min 1
}

type Ring struct{ /* atomic snapshot */ }

func New(virtualNodes int, members ...Member) *Ring
func (r *Ring) Set(members []Member)          // atomic replace of the whole membership
func (r *Ring) Add(m Member)
func (r *Ring) Remove(id string)
func (r *Ring) Members() []Member             // sorted by ID
func (r *Ring) Len() int                      // number of distinct members
func (r *Ring) VirtualNodes() int             // number of points on the ring
func (r *Ring) Get(key string) (string, bool) // primary owner
// GetN walks the ring clockwise from key's position and returns up to n DISTINCT member ids
// in preference order. Fewer than n only when the ring holds fewer members.
func (r *Ring) GetN(key string, n int) []string
```

Tests must cover: determinism, distinct-ids from GetN, monotonic remapping (removing one member
only moves keys that were owned by it), distribution balance (max/mean load < 1.5 over 100k keys
with 10 members), and concurrent Get/Set under `-race`.

## internal/circuit

Rolling-window breaker, 3 states. Time is injectable for tests.

```go
package circuit

type State int32
const (StateClosed State = iota; StateOpen; StateHalfOpen)
func (s State) String() string

type Settings struct {
	Window       time.Duration
	Buckets      int
	MinRequests  int
	FailureRatio float64
	OpenTimeout  time.Duration
	HalfOpenMax  int
	MaxEntries   int              // Registry cap; 0 => DefaultMaxEntries (1024)
	Now          func() time.Time // nil => time.Now
	OnStateChange func(name string, from, to State) // may be nil
}

type Breaker struct{ /* mutex-guarded rolling buckets */ }

func NewBreaker(name string, s Settings) *Breaker
func (b *Breaker) Name() string
func (b *Breaker) State() State  // recomputes open->half-open on OpenTimeout expiry
func (b *Breaker) Ready() bool          // NON-CONSUMING predicate: "would a request be admitted?"
func (b *Breaker) Admit() (Probe, bool) // RESERVES a half-open probe; the Probe MUST be resolved
func (b *Breaker) Unattributed() Probe  // books outcomes without ever resolving a probe
func (b *Breaker) Success()             // UNATTRIBUTED outcome: window only, never resolves a probe
func (b *Breaker) Failure()

// Probe identifies one admitted request so its outcome is attributed to the half-open episode that
// admitted it. The zero Probe is safe and does nothing.
type Probe struct{}
func (p Probe) IsProbe() bool
func (p Probe) Success()
func (p Probe) Failure()
func (p Probe) Release() // admission abandoned without contacting the backend; window untouched
func (b *Breaker) Counts() (requests, failures int)
func (b *Breaker) Reset()

type Registry struct{}
func NewRegistry(s Settings) *Registry
func (r *Registry) Get(name string) *Breaker // create-on-miss, safe for concurrent use; returns the
                                             // shared OverflowName breaker once at MaxEntries

const DefaultMaxEntries = 1024
const OverflowName = "__overflow__"
```

**Ready vs Admit.** `Admit` has a side effect: in half-open it reserves one of `HalfOpenMax` probe
slots, and that slot only comes back through the returned `Probe`. Anything that merely *considers*
a backend — candidate filtering, a staleness re-check — must call `Ready`, never `Admit`. Using an
admitting call as a predicate strands probes that nothing resolves, and once `HalfOpenMax` of them
accumulate the breaker never admits another request: a healthy backend is removed from routing
permanently. Every `Admit()` that returns true must be resolved exactly once on every exit path —
`Success`, `Failure`, or `Release` — panics and early returns included.

**Attribution.** Only a probe from the half-open episode still running may resolve a slot or decide
the state. This proxy forwards long-lived streams, so outcomes routinely arrive from requests
admitted while the breaker was still closed, long before it tripped. Such an outcome is counted in
the rolling window but gets no say in whether the backend is judged recovered; letting it close the
breaker would restore full traffic on evidence that predates the trip, and clearing the window on
the way out would stop the genuine probe's failure from re-tripping it. `Breaker.Success`/`Failure`
are the unattributed form and can never resolve a probe.

`Settings.MaxEntries` bounds `Registry`. This proxy forwards whatever method name arrives on the
wire, so an unauthenticated client could otherwise mint unbounded breakers with `/x/1`, `/x/2`, ...

Semantics: closed → open when `requests >= MinRequests && failures/requests >= FailureRatio`
within the window. Open → half-open after `OpenTimeout`. Half-open: `Allow()` returns true for at
most `HalfOpenMax` concurrent probes; one `Failure()` reopens (resetting the timer), and
`HalfOpenMax` successes close it and clear the window.

## internal/ratelimit

```go
package ratelimit

type Config struct {
	Enabled        bool
	RPS            float64
	Burst          int
	PerClient      bool
	PerClientRPS   float64
	PerClientBurst int
	TTL            time.Duration // idle per-client entry eviction, default 5m
}

type Limiter struct{}
func New(c Config) *Limiter
func (l *Limiter) Allow(clientKey string) bool // global bucket first, then per-client if enabled
func (l *Limiter) Clients() int                // live per-client buckets (for tests/metrics)
func (l *Limiter) Close()                      // stops the janitor goroutine; idempotent
```

Per-client buckets live in a sharded map (>=16 shards, `xxhash` of the key picks the shard) so
hot-path contention stays low. A janitor evicts entries idle for TTL.

## internal/metrics

All metrics registered on a private `*prometheus.Registry` (never the default one).

```go
package metrics

type Metrics struct{}
func New() *Metrics
func (m *Metrics) Registry() *prometheus.Registry
func (m *Metrics) Handler() http.Handler

// RED, request side
func (m *Metrics) ObserveRequest(method, code string, d time.Duration) // counter + histogram
func (m *Metrics) IncInflight(method string)
func (m *Metrics) DecInflight(method string)
// Upstream side
func (m *Metrics) ObserveUpstream(backend, code string, d time.Duration)
func (m *Metrics) IncFailover(method, reason string)
func (m *Metrics) IncRateLimited(method string)
func (m *Metrics) IncPanic(method string)
func (m *Metrics) IncRejected(method, reason string) // circuit-open etc.
// Gauges
func (m *Metrics) SetBackendHealth(backend string, healthy bool)
func (m *Metrics) SetCircuitState(name string, state int)
func (m *Metrics) SetRingMembers(n int)
func (m *Metrics) SetRingVirtualNodes(n int)
func (m *Metrics) ObserveHealthCheck(backend, result string, d time.Duration)
```

Metric names — use exactly these:

| name | type | labels |
|---|---|---|
| `lb_requests_total` | counter | `method`,`code` |
| `lb_request_duration_seconds` | histogram | `method`,`code` |
| `lb_requests_inflight` | gauge | `method` |
| `lb_upstream_requests_total` | counter | `backend`,`code` |
| `lb_upstream_duration_seconds` | histogram | `backend` |
| `lb_failovers_total` | counter | `method`,`reason` |
| `lb_rate_limited_total` | counter | `method` |
| `lb_rejected_total` | counter | `method`,`reason` |
| `lb_panics_total` | counter | `method` |
| `lb_backend_healthy` | gauge | `backend` |
| `lb_circuit_state` | gauge | `name` |
| `lb_ring_members` | gauge | — |
| `lb_ring_virtual_nodes` | gauge | — |
| `lb_health_check_duration_seconds` | histogram | `backend`,`result` |

`lb_request_duration_seconds` buckets (SLO is 200 ms, so resolve finely below it):
`0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1, 2.5, 5`.
`lb_upstream_duration_seconds` uses the same buckets.

## internal/pool

Owns backend identity, the dialed `*grpc.ClientConn`, health state and the per-backend breaker.

```go
package pool

type State int32
const (StateUnknown State = iota; StateHealthy; StateUnhealthy)
func (s State) String() string

type Backend struct{ /* atomics for state; conn is immutable after New */ }

func (b *Backend) ID() string
func (b *Backend) Addr() string
func (b *Backend) Weight() int
func (b *Backend) Conn() *grpc.ClientConn
func (b *Backend) State() State
func (b *Backend) SetState(s State) bool       // reports whether the state changed
func (b *Backend) Breaker() *circuit.Breaker
func (b *Backend) Available() bool                     // Healthy() && Breaker().Ready() — reserves nothing
func (b *Backend) Reserve() (circuit.Probe, bool)      // commits to an attempt; the Probe MUST be
                                                       // resolved (Success/Failure/Release)
func (b *Backend) Healthy() bool               // State()==StateHealthy

type Pool struct{}

// New dials every backend lazily (grpc.NewClient — no blocking dial) with the shared dial options.
func New(cfg config.Config, m *metrics.Metrics, dialOpts ...grpc.DialOption) (*Pool, error)
func (p *Pool) Backends() []*Backend            // stable order, by config order
func (p *Pool) Get(id string) (*Backend, bool)
func (p *Pool) Healthy() []*Backend
func (p *Pool) Close() error                    // closes all conns
func (p *Pool) OnChange(f func())               // registered callbacks fire when any state flips
func (p *Pool) NotifyChange()                   // invoked by health checker after SetState
```

`New` must set, on every conn: `grpc.WithTransportCredentials(insecure.NewCredentials())`,
default call options with the configured max message sizes, keepalive
(`keepalive.ClientParameters{Time: 30s, Timeout: 10s, PermitWithoutStream: true}`),
and `grpc.WithDefaultServiceConfig` selecting `round_robin` so a single backend DNS name that
resolves to several addresses still spreads load.

## internal/health

```go
package health

type Checker struct{}
func New(p *pool.Pool, cfg config.Health, m *metrics.Metrics, log *slog.Logger) *Checker
func (c *Checker) Start(ctx context.Context)  // one goroutine per backend; returns immediately
func (c *Checker) Stop()                      // idempotent, waits for goroutines
func (c *Checker) CheckOnce(ctx context.Context, b *pool.Backend) (pool.State, error) // exported for tests
```

Uses `grpc_health_v1.NewHealthClient(b.Conn()).Check(...)` with `cfg.Timeout`. Flap damping:
`Rise` consecutive `SERVING` responses to become healthy, `Fall` consecutive failures/non-SERVING
to become unhealthy. First check runs immediately, then every `Interval` with ±10% jitter so
backends are not probed in lockstep. On every transition: log at info, update
`SetBackendHealth`, and call `p.NotifyChange()`.

## internal/balancer

```go
package balancer

type Balancer struct{}
func New(p *pool.Pool, cfg config.Routing, m *metrics.Metrics) *Balancer
// Pick returns up to maxAttempts candidate backends in preference order for the given hash key.
// The first is the consistent-hash owner; the rest are the next distinct ring members, used only
// for failover. Backends that are unhealthy or whose breaker is open are skipped, EXCEPT that if
// every backend is filtered out Pick falls back to all healthy backends, and if none are healthy
// it returns every backend (fail-open: a stale health signal must not black-hole traffic).
func (b *Balancer) Pick(key string, maxAttempts int) []*pool.Backend
func (b *Balancer) Ring() *hashring.Ring
func (b *Balancer) Rebuild()      // re-sync ring membership from the pool; wired to pool.OnChange
func (b *Balancer) Close()
```

The ring holds **all configured** backends (membership must not change with health, or every flap
would reshuffle every key); health/breaker state is applied as a filter over the ordered candidate
list. `New` must register `Rebuild` via `p.OnChange` and populate ring gauges.

## internal/middleware

```go
package middleware

type RequestInfo struct {
	ID        string
	Method    string
	StartedAt time.Time
	HashKey   string
	// mutated by the proxy; guarded internally
}
func (r *RequestInfo) SetBackend(id string)
func (r *RequestInfo) Backend() string
func (r *RequestInfo) AddAttempt()
func (r *RequestInfo) Attempts() int
func (r *RequestInfo) SetHashKey(k string)

func NewContext(ctx context.Context, info *RequestInfo) context.Context
func FromContext(ctx context.Context) (*RequestInfo, bool)

// WrapServerStream returns a grpc.ServerStream whose Context() is ctx.
func WrapServerStream(ss grpc.ServerStream, ctx context.Context) grpc.ServerStream

// Chain composes interceptors; the first argument is the OUTERMOST.
func Chain(is ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor

func Recovery(log *slog.Logger, m *metrics.Metrics) grpc.StreamServerInterceptor
func Context(hashHeader string) grpc.StreamServerInterceptor // builds RequestInfo, request id, hash key
func Logging(log *slog.Logger, cfg config.Logging) grpc.StreamServerInterceptor
func Metrics(m *metrics.Metrics) grpc.StreamServerInterceptor
func RateLimit(l *ratelimit.Limiter, m *metrics.Metrics) grpc.StreamServerInterceptor
func CircuitBreak(reg *circuit.Registry, m *metrics.Metrics) grpc.StreamServerInterceptor

// ClientKey resolves the rate-limit key: metadata "x-client-id" if present, else peer IP.
func ClientKey(ctx context.Context) string
// HashKey resolves the affinity key: metadata[hashHeader] if present, else peer IP.
func HashKey(ctx context.Context, hashHeader string) string
```

Pipeline order fixed by `cmd/lb` (outermost first):
`Recovery → Context → Logging → Metrics → RateLimit → CircuitBreak → proxy handler`.

- `Recovery` converts a panic into `codes.Internal`, logs it with the stack, increments `IncPanic`.
- `Context` generates a 16-hex-char request id (or reuses incoming `x-request-id`), resolves the
  hash key, stores `RequestInfo`, and sets `x-request-id` on the response header.
- `Logging` emits one record per RPC: `request_id, method, code, duration_ms, backend, attempts,
  hash_key, peer`. Non-OK always logged (warn for client errors, error for server errors); OK
  logged with probability `SampleRate`.
- `Metrics` wraps `IncInflight/DecInflight` and `ObserveRequest` with the final status code
  (`status.Code(err).String()`).
- `RateLimit` rejects with `codes.ResourceExhausted` and message `"rate limit exceeded"`.
- `CircuitBreak` keys a breaker on the **method** and rejects with `codes.Unavailable` message
  `"circuit open"` when `Allow()` is false; records success/failure from the handler's error
  (only `Unavailable`, `DeadlineExceeded`, `Internal`, `ResourceExhausted`, `Unknown` count as
  failures — `InvalidArgument` and `NotFound` are client errors and must not trip the breaker).

## internal/proxy

Transparent L7 gRPC proxy. Registered with `grpc.UnknownServiceHandler` so it forwards **any**
service/method without knowing its schema.

```go
package proxy

// Codec is the passthrough codec. It MUST report Name() == "proto" and MUST NOT be registered
// globally with encoding.RegisterCodecV2 (that would break every other client in the process).
// It is applied per-server via grpc.ForceServerCodecV2 and per-call via grpc.ForceCodecV2.
type Codec struct{}
func (Codec) Marshal(v any) (mem.BufferSlice, error)
func (Codec) Unmarshal(data mem.BufferSlice, v any) error
func (Codec) Name() string

type Proxy struct{}
func New(b *balancer.Balancer, cfg config.Config, m *metrics.Metrics, log *slog.Logger) *Proxy
func (p *Proxy) Handler() grpc.StreamHandler // pass to grpc.UnknownServiceHandler
func (p *Proxy) ServerOptions() []grpc.ServerOption // ForceServerCodecV2 + UnknownServiceHandler
```

Forwarding rules:

1. Method from `grpc.MethodFromServerStream(stream)`; `codes.Internal` if absent.
2. Build the outgoing context from `metadata.FromIncomingContext`, copied and stripped of
   hop-by-hop keys (`connection`, `te`, `trailer`, `transfer-encoding`, `upgrade`,
   `keep-alive`, `proxy-authorization`, `:authority` and any other reserved `:`-prefixed key),
   with `x-forwarded-for` appended from the peer and `x-request-id` propagated.
3. Pick candidates via `balancer.Pick(hashKey, cfg.Routing.MaxAttempts)`.
4. Read the **first** client message into a `mem.BufferSlice` and retain it (`Ref()`), then try
   candidate 1. On failure, retry the next candidate **only if it is still safe**: no response
   message and no header has been forwarded downstream, and the client has not sent a second
   message yet. Retryable status codes: `Unavailable`, `DeadlineExceeded` (only if the parent
   context still has budget), `ResourceExhausted`, `Internal` when it came from the transport,
   plus any error from `NewClientStream`/first `SendMsg`. Every retry calls `m.IncFailover` and
   `info.AddAttempt()`. Free the buffered slice exactly once when the RPC ends.
5. Per attempt: `grpc.NewClientStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
   b.Conn(), method, grpc.ForceCodecV2(Codec{}), grpc.WaitForReady(false))`.
6. Pump both directions concurrently: client→upstream (`stream.RecvMsg` → `cs.SendMsg`, then
   `cs.CloseSend` on `io.EOF`) and upstream→client (`cs.RecvMsg` → `stream.SendMsg`). Forward the
   upstream header via `cs.Header()` + `stream.SendHeader` before the first response message, and
   `cs.Trailer()` via `stream.SetTrailer` at the end — on both the success and error paths.
7. Record the outcome on the backend's breaker (`Success`/`Failure`, same code classification as
   `CircuitBreak`), `m.ObserveUpstream(backendID, code, elapsed)`, and `info.SetBackend(id)`.
8. Free every `mem.BufferSlice` received from `RecvMsg` after it has been sent onward — leaking
   pooled buffers is a real bug here, and double-freeing panics. Buffers handed to `SendMsg` are
   freed by gRPC, so `Ref()` before reusing the buffered first message on a retry.

Latency discipline: no allocation of per-message goroutines, no `time.Sleep` on the hot path, and
no logging above debug level per message.

## cmd/backend

Demo upstream. Flags: `-listen` (default `:50051`), `-id` (default hostname), `-delay` (base
latency added to every reply, default 0), `-fail-ratio` (0..1, random `Unavailable`),
`-health` (`serving|not_serving`, default serving). Env fallbacks `BACKEND_LISTEN`, `BACKEND_ID`,
`BACKEND_DELAY`, `BACKEND_FAIL_RATIO`. Registers `EchoService`, the standard
`grpc_health_v1` health server, and reflection. Honours `EchoRequest.delay_ms`, `fail_code` and
`stream_count`. Exposes `/healthz` and `/metrics` on `-admin-listen` (default `:9101`).
Graceful shutdown on SIGINT/SIGTERM: flip health to NOT_SERVING, wait 250 ms, then `GracefulStop`.

## cmd/lb

Flags: `-config` (path, optional), `-listen`, `-admin-listen`, `-log-level`. Wires:
config → metrics → pool → health checker → balancer → proxy → middleware chain → grpc.Server with
`proxy.ServerOptions()` plus `grpc.ChainStreamInterceptor(chain)`, `grpc.MaxRecvMsgSize`,
`grpc.MaxSendMsgSize`, `grpc.KeepaliveParams`, `grpc.NumStreamWorkers(uint32(runtime.NumCPU()))`.
Admin HTTP server on `AdminListen`: `/metrics`, `/healthz` (always 200 once serving),
`/readyz` (200 only when >=1 backend is healthy), `/backends` (JSON: id, addr, state, breaker
state, weight), and `net/http/pprof`. Graceful shutdown on SIGINT/SIGTERM within `ShutdownGrace`.

## cmd/loadgen

The measurement tool. It must produce defensible numbers, which means avoiding coordinated
omission: in `-mode open` it schedules each request at a fixed offset `start + i/rps` and reports
**both** service time (send→response) and response time (scheduled→response).

Flags: `-target` (host:port), `-rps`, `-duration`, `-concurrency`, `-mode` (`open|closed`),
`-warmup`, `-payload` (bytes), `-delay-ms` (server-side think time), `-keys` (distinct session
keys, 0 = no affinity header), `-conns` (client connections), `-method`
(`unary|server_stream`), `-out` (JSON path), `-label`, `-slo` (default 200ms).

Latency is computed over **successful RPCs only**, with failures summarised separately in
`error_latency`. Pooling failures into the latency distribution inverts the SLO: a load balancer
that rejects everything in 50us would report a sub-millisecond p99 and "PASS". The verdict
therefore also requires `ok > 0` and an error ratio within `-error-budget` (default 0.001), and a
run cut short by SIGINT is marked `interrupted` with its `scheduled` count and cannot pass.

Output: total, ok, error counts by gRPC code, achieved RPS, and p50/p90/p95/p99/p99.9/max for both
timers, plus `slo_violations` and the fraction under the SLO, and the observed backend
distribution (from `EchoResponse.backend_id`) with a stickiness assertion when `-keys > 0`
(each key must map to exactly one backend id — report the number of keys that saw more than one).
Percentiles are computed from the full sorted sample (nearest-rank), never from a summary.
Writes JSON to `-out` and a human table to stdout.
