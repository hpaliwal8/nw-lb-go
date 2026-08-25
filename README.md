# nw-lb-go

A layer-7 gRPC load balancer in Go. It terminates gRPC, picks an upstream with a
weighted consistent hash ring, and forwards the stream verbatim — it never needs
to know the schema of the service it is proxying, because it is registered as a
`grpc.UnknownServiceHandler` with a passthrough codec.

What is in the box:

- **Session affinity** via a consistent hash ring (xxhash, weighted virtual
  nodes). Removing a backend only remaps the keys that backend owned.
- **Health checking** over `grpc.health.v1.Health/Check`, with rise/fall flap
  damping and jittered probes.
- **Retry and failover** onto the next ring member, but only while a retry is
  still observably safe (no header and no response message forwarded yet).
- **Circuit breaking** on a rolling window, per method at the middleware layer
  and per backend in the connection pool.
- **Rate limiting**, global and optionally per client, from sharded token buckets.
- **RED metrics** on a private Prometheus registry, with a 200 ms latency SLO
  that is an exact histogram bucket boundary.
- **A load generator** that schedules on an open loop and reports response time
  as well as service time, so the numbers survive scrutiny.

`docs/CONTRACT.md` is the authoritative description of every package's API.

---

## Architecture

```
                                                    +-----------------------------+
                                                    |  health.Checker             |
                                                    |  grpc.health.v1/Check       |
                                                    |  every 1s +/-10% jitter     |
                                                    |  rise 2 / fall 3            |
                                                    +--------------+--------------+
                                                                   | SetState / NotifyChange
                                                                   v
  client                          cmd/lb                     +-----------+
    |                                                        | pool.Pool |
    |  gRPC                                                  |  ClientConn per backend
    |  :8080     +----------------------------------------+  |  health state
    +----------->| grpc.Server                            |  |  per-backend breaker
                 |   ForceServerCodecV2(proxy.Codec)      |  +-----+-----+
                 |   UnknownServiceHandler(proxy.Handler) |        |
                 |                                        |        |
                 | middleware chain, OUTERMOST first:     |        |
                 |   1  Recovery      panic  -> Internal  |        |
                 |   2  Context       request id, hash key|        |
                 |   3  Logging       one record per RPC  |        |
                 |   4  Metrics       inflight + RED      |--------+-------> metrics.Metrics
                 |   5  RateLimit     -> ResourceExhausted|        |         private registry
                 |   6  CircuitBreak  -> Unavailable      |        |              |
                 +-------------------+--------------------+        |              | /metrics
                                     |                             |              v
                                     v                             |        admin HTTP :9090
                 +----------------------------------------+        |        /healthz /readyz
                 | proxy.Handler                          |        |        /backends /debug/pprof
                 |   buffers the first client message     |        |
                 |   pumps both directions concurrently   |        |
                 |   retries while it is still safe       |        |
                 +-------------------+--------------------+        |
                                     |  Pick(hashKey, max_attempts)|
                                     v                             |
                 +----------------------------------------+        |
                 | balancer.Balancer                      |        |
                 |   ordered candidates from the ring,    |<-------+ Rebuild on health change
                 |   then filter unhealthy / breaker-open |
                 |   fail open if that leaves nothing     |
                 +-------------------+--------------------+
                                     |
                 +----------------------------------------+
                 | hashring.Ring   (immutable snapshot    |
                 |   behind an atomic.Pointer, so reads   |
                 |   are lock-free)                       |
                 |   xxhash, 200 vnodes per weight 100    |
                 |   membership = ALL configured backends |
                 +-------------------+--------------------+
                                     |
        +----------------------------+----------------------------+
        |                            |                            |
        v                            v                            v
 +--------------+            +--------------+            +--------------+
 |  backend-1   |            |  backend-2   |            |  backend-3   |
 |  gRPC :50051 |            |  gRPC :50052 |            |  gRPC :50053 |
 |  admin :9101 |            |  admin :9102 |            |  admin :9103 |
 +--------------+            +--------------+            +--------------+
```

Two design decisions are worth calling out because they are easy to get wrong:

**The ring holds every configured backend, healthy or not.** Health is applied
afterwards, as a filter over the ordered candidate list. If membership tracked
health instead, every flap would reshuffle every key and session affinity would
be worthless exactly when the system is under stress.

**The balancer fails open.** If health and breaker filtering removes every
candidate, `Pick` falls back to all healthy backends, and if none are healthy it
returns all of them. A stale health signal must not black-hole traffic.

---

## Request lifecycle

1. A client opens a stream to `:8080`. gRPC hands it to the proxy handler,
   because no concrete service is registered — only `UnknownServiceHandler`.
2. **Recovery** installs a deferred recover. A panic anywhere below becomes
   `codes.Internal` and increments `lb_panics_total`; the process survives.
3. **Context** mints a 16-hex-character request id (or reuses an inbound
   `x-request-id`), resolves the hash key from metadata `x-session-id` — falling
   back to the peer IP — and stores a `RequestInfo` on the context. The request
   id goes back out on the response header.
4. **Logging** starts the clock for the single access-log record this RPC will
   produce on the way out.
5. **Metrics** increments `lb_requests_inflight` and arranges to observe
   `lb_requests_total` and `lb_request_duration_seconds` with the final status.
6. **RateLimit** takes a token from the global bucket, then from the client's
   bucket if per-client limiting is on. No token means `ResourceExhausted` and
   `lb_rate_limited_total`.
7. **CircuitBreak** checks the breaker keyed on the *method*. Open means
   `Unavailable` with `lb_rejected_total{reason="circuit_open"}`, without ever
   touching a backend.
8. The **proxy handler** resolves the method name, copies the inbound metadata
   while stripping hop-by-hop keys and reserved `:`-prefixed keys, appends the
   peer to `x-forwarded-for`, and propagates `x-request-id`.
9. **`balancer.Pick(hashKey, max_attempts)`** returns up to `max_attempts`
   distinct backends: the ring's owner of the key first, then the next distinct
   members clockwise as failover candidates.
10. The proxy reads the **first** client message into a `mem.BufferSlice` and
    retains it, then opens a client stream to candidate 1 with
    `WaitForReady(false)`.
11. Both directions are pumped concurrently: client to upstream until `io.EOF`
    then `CloseSend`, and upstream to client, forwarding the upstream header
    before the first response message and the trailer at the end — on the error
    path too.
12. If the attempt fails and nothing observable has been forwarded yet (no
    header, no response message, no second client message), the retained first
    message is re-sent to the next candidate. Each retry counts
    `lb_failovers_total{reason=<code>}` and bumps the attempt counter.
13. The outcome is recorded on the backend's breaker and as
    `lb_upstream_requests_total` / `lb_upstream_duration_seconds`. Only
    `Unavailable`, `DeadlineExceeded`, `Internal`, `ResourceExhausted` and
    `Unknown` count as failures; `InvalidArgument` and `NotFound` are the
    caller's fault and must not trip a breaker.
14. Unwinding the chain: `DecInflight`, the RED observation with
    `status.Code(err).String()`, and one log record — always for a non-OK
    status, with probability `logging.sample_rate` for an OK one.
15. Every pooled buffer is freed exactly once. Leaking them is a slow death and
    double-freeing panics.

---

## Configuration

Precedence, lowest to highest: **built-in defaults → YAML file → `LB_*`
environment → command-line flags**. Flags only take effect when actually present
on the command line, so an unset flag never overwrites a configured value.

Unknown YAML keys are a **hard error**. A typo fails the process at startup
instead of silently reverting to a default.

Two config files ship with the repo:

| file | backends point at | used by |
|---|---|---|
| `config/lb.yaml` | compose service names (`backend-1:50051`) | the `lb` container |
| `config/lb.local.yaml` | `127.0.0.1:50051-50053` | `make run-lb` |

### Options

| key | type | default | meaning |
|---|---|---|---|
| `listen` | string | `:8080` | gRPC data-plane address |
| `admin_listen` | string | `:9090` | admin HTTP address |
| `backends[].id` | string | required | stable identity; also the ring member key and the per-backend breaker name |
| `backends[].addr` | string | required | `host:port` to dial |
| `backends[].weight` | int | `100` | virtual nodes = `virtual_nodes * weight / 100`, minimum 1 |
| `routing.hash_header` | string | `x-session-id` | metadata key carrying the affinity key |
| `routing.virtual_nodes` | int | `200` | ring points per weight-100 member; higher is smoother and slower to rebuild |
| `routing.max_attempts` | int | `3` | candidates returned by `Pick`; 1 disables failover |
| `routing.retry_policy` | enum | `connect-failure` | which failures may be replayed: `connect-failure` (at-most-once), `unavailable` (at-least-once), `none` |
| `health.interval` | duration | `1s` | probe period, jittered +/-10% so backends are not probed in lockstep |
| `health.timeout` | duration | `500ms` | per-probe deadline |
| `health.rise` | int | `2` | consecutive SERVING responses needed to become healthy |
| `health.fall` | int | `3` | consecutive failures needed to become unhealthy |
| `health.service` | string | `""` | gRPC health service name; empty means the server's overall health |
| `rate_limit.enabled` | bool | `false` | master switch |
| `rate_limit.rps` | float | `50000` | global sustained rate |
| `rate_limit.burst` | int | `2 * rps` | global bucket depth; omit the key to derive it |
| `rate_limit.per_client` | bool | `false` | also limit per client key (`x-client-id`, else peer IP) |
| `rate_limit.per_client_rps` | float | `5000` | per-client sustained rate |
| `rate_limit.per_client_burst` | int | `2 * per_client_rps` | per-client bucket depth |
| `circuit_breaker.enabled` | bool | `false` | master switch |
| `circuit_breaker.window` | duration | `10s` | rolling window the failure ratio is measured over |
| `circuit_breaker.buckets` | int | `10` | window subdivisions; more means finer expiry, more bookkeeping |
| `circuit_breaker.min_requests` | int | `20` | floor below which the ratio is not trusted |
| `circuit_breaker.failure_ratio` | float | `0.5` | trip threshold, in `(0,1]` |
| `circuit_breaker.open_timeout` | duration | `5s` | how long open lasts before half-open probing |
| `circuit_breaker.half_open_max` | int | `5` | concurrent probes admitted while half-open; that many successes close it, one failure reopens it |
| `logging.level` | string | `info` | `debug`, `info`, `warn` or `error` |
| `logging.format` | string | `json` | `json` or `text` |
| `logging.sample_rate` | float | `1` | fraction of OK RPCs logged; non-OK is always logged |
| `proxy.max_recv_msg_size` | int | `16777216` | 16 MiB |
| `proxy.max_send_msg_size` | int | `16777216` | 16 MiB |
| `proxy.shutdown_grace` | duration | `10s` | drain budget on SIGINT/SIGTERM |

### Environment overrides

| variable | overrides |
|---|---|
| `LB_LISTEN` | `listen` |
| `LB_ADMIN_LISTEN` | `admin_listen` |
| `LB_BACKENDS` | the whole `backends` list: `id=addr,id=addr` or bare `addr,addr` (positional ids `backend-N`, weight 100) |
| `LB_LOG_LEVEL` | `logging.level` |
| `LB_RATE_LIMIT_RPS` | `rate_limit.rps` |
| `LB_HASH_HEADER` | `routing.hash_header` |
| `LB_MAX_ATTEMPTS` | `routing.max_attempts` |

An unset **or empty** variable leaves the value alone — container runtimes
routinely inject empty variables, and those must not wipe a valid config.

### Flags

| binary | flags |
|---|---|
| `lb` | `-config`, `-listen`, `-admin-listen`, `-log-level` |
| `backend` | `-listen` (`:50051`), `-admin-listen` (`:9101`), `-id` (hostname), `-delay` (`0`), `-fail-ratio` (`0`), `-health` (`serving`\|`not_serving`); env fallbacks `BACKEND_LISTEN`, `BACKEND_ID`, `BACKEND_DELAY`, `BACKEND_FAIL_RATIO` |
| `loadgen` | see [Benchmarking](#benchmarking) |

---

## Run it locally

Three commands. The first two in one terminal, the third in another.

```sh
make run-backends   # three upstreams on :50051/:50052/:50053, backgrounded
make run-lb         # the LB on :8080, admin on :9090, foreground
make bench          # in a second terminal: load-test it
```

`make run-backends` injects 2 ms, 8 ms and 25 ms of latency respectively, so the
dashboard shows a real spread rather than three identical lines. It writes pids
to `bin/.run/backends.pid` (inside the gitignored `bin/`); `make stop-backends`
cleans up.

While it is running:

```sh
curl -s localhost:9090/readyz   | jq   # {"status":"ok","healthy_backends":3,"backends":3}
curl -s localhost:9090/backends | jq   # id, addr, state, breaker_state, weight
curl -s localhost:9090/metrics  | grep '^lb_'
go tool pprof -http=: localhost:9090/debug/pprof/profile?seconds=30
```

## Run it in Docker

One command:

```sh
make up      # == docker compose up -d --build
```

| service | host address |
|---|---|
| LB, gRPC | `localhost:8080` |
| LB, admin | <http://localhost:9090/metrics> |
| backend-1/2/3, gRPC | `localhost:50051` / `50052` / `50053` |
| backend-1/2/3, admin | `localhost:9101` / `9102` / `9103` |
| Prometheus | <http://localhost:9091> |
| Grafana | <http://localhost:3000> (anonymous viewer; dashboard `nw-lb-go / nw-lb-go RED`) |

Prometheus is published on 9091 because the LB's admin server already owns 9090
on the host. Inside the network it is still `prometheus:9090`.

Benchmark the compose stack — `loadgen` sits behind the `bench` profile so it
does not start with everything else:

```sh
docker compose --profile bench run --rm loadgen
docker compose --profile bench run --rm loadgen -rps 5000 -duration 60s \
  -label hot -out /bench/results/hot.json
```

Arguments after `loadgen` replace the service's whole command, so the second
form relies on the `LOADGEN_TARGET=lb:8080` environment variable the compose
service sets. Pass `-target lb:8080` explicitly if you run the image by hand.

Reports land in `./bench/results` on the host. `make down` stops everything and
drops the Prometheus and Grafana volumes.

The `lb`, `backend` and `loadgen` images are
`gcr.io/distroless/static-debian12:nonroot` with a single static binary and no
shell, so `docker compose exec lb sh` will not work. Use `docker compose logs`.

---

## Benchmarking

```sh
make bench                                     # 2000 rps, 30s, open loop
make bench BENCH_RPS=8000 BENCH_DURATION=60s   # override any knob
make bench-baseline                            # same load straight at backend-1
```

Every knob is a make variable: `BENCH_TARGET`, `BENCH_MODE`, `BENCH_RPS`,
`BENCH_DURATION`, `BENCH_WARMUP`, `BENCH_CONCURRENCY`, `BENCH_CONNS`,
`BENCH_KEYS`, `BENCH_PAYLOAD`, `BENCH_DELAY_MS`, `BENCH_METHOD`, `BENCH_SLO`,
`BENCH_LABEL`, `BENCH_OUT`.

Or drive `loadgen` directly:

| flag | default | meaning |
|---|---|---|
| `-target` | `127.0.0.1:8080` | `host:port` to dial |
| `-mode` | `open` | `open` = fixed schedule; `closed` = tight loop |
| `-rps` | `500` | requests per second to schedule (open mode) |
| `-duration` | `10s` | measurement window |
| `-concurrency` | `50` | worker goroutines |
| `-conns` | `0` | client connections; 0 means `min(concurrency, NumCPU)` |
| `-warmup` | `100` | **a request count, not a duration** — executed and discarded before measuring |
| `-payload` | `128` | request payload bytes |
| `-delay-ms` | `0` | server-side think time asked of the backend |
| `-keys` | `1000` | distinct `x-session-id` values; `0` sends no affinity header |
| `-method` | `unary` | `unary` or `server_stream` |
| `-stream-count` | `5` | messages per `server_stream` response |
| `-slo` | `200ms` | compared against p99 response time |
| `-label` | `loadgen` | recorded in the report |
| `-out` | `""` | JSON path; empty prints only the table |

### Reading the results

`loadgen` prints a table and writes the same data as JSON:

```
=== lb-open-800rps ===
target    127.0.0.1:8080
mode      open, method unary
load      4 conns, 64 workers, 256 B payload, 0 ms server delay
duration  6.021 s
rps       800.000 requested, 797.231 achieved
requests  4800 total, 4800 ok, 0 errors
errors    none
backends  backend-1=1421  backend-2=1509  backend-3=1870
keys      1000 distinct, 0 with more than one backend

  timer (ms)    p50     p90     p95     p99   p99.9     max    mean
     service  8.861  25.747  26.023  26.906  29.085  32.043  13.590
    response  9.085  25.940  26.262  27.233  31.109  32.333  13.784

SLO: PASS  p99 response 27.233 ms vs SLO 200.000 ms; 4800/4800 under SLO (100.0000%), 0 violations
```

Read it in this order:

1. **`rps` requested vs achieved.** If achieved is materially below requested in
   open mode, the generator could not keep its schedule and every latency below
   is optimistic. Fix that before believing anything else — more `-conns`, more
   `-concurrency`, or a smaller `-rps`.
2. **`keys ... with more than one backend`.** This is the affinity assertion.
   With `-keys > 0` it must be `0`. Anything else means a key moved, which only
   legitimately happens if ring membership changed mid-run.
3. **`backends`.** The observed distribution, read from
   `EchoResponse.backend_id`. With equal weights it should be roughly even —
   consistent hashing is not perfectly uniform, and a spread of a few percent
   over 200 virtual nodes is expected, not a bug.
4. **`response` vs `service`.** See below. The gap is queueing the client
   suffered, and it is the honest number.
5. **`SLO`.** p99 of *response* time against `-slo`, plus the fraction under it.

JSON fields mirror the table: `rps_requested`, `rps_achieved`, `total`, `ok`,
`errors`, `errors_by_code`, `service_time` and `response_time` (each with `p50`,
`p90`, `p95`, `p99`, `p999`, `max`, `mean` in milliseconds), `slo_ms`,
`slo_violations`, `slo_fraction_under`, `backend_distribution`,
`distinct_keys`, `keys_with_multiple_backends`.

---

## Metrics

Everything is registered on a private `*prometheus.Registry`, never
`prometheus.DefaultRegisterer`, so importing the package cannot pollute another
process's metrics.

| name | type | labels |
|---|---|---|
| `lb_requests_total` | counter | `method`, `code` |
| `lb_request_duration_seconds` | histogram | `method`, `code` |
| `lb_requests_inflight` | gauge | `method` |
| `lb_upstream_requests_total` | counter | `backend`, `code` |
| `lb_upstream_duration_seconds` | histogram | `backend` |
| `lb_failovers_total` | counter | `method`, `reason` (the gRPC code that triggered the retry) |
| `lb_rate_limited_total` | counter | `method` |
| `lb_rejected_total` | counter | `method`, `reason` (`circuit_open`) |
| `lb_panics_total` | counter | `method` |
| `lb_backend_healthy` | gauge | `backend` (1 healthy, 0 not) |
| `lb_circuit_state` | gauge | `name` (0 closed, 1 open, 2 half-open) |
| `lb_ring_members` | gauge | — |
| `lb_ring_virtual_nodes` | gauge | — |
| `lb_health_check_duration_seconds` | histogram | `backend`, `result` |

Latency buckets, shared by the request and upstream histograms:

```
0.0002 0.0005 0.001 0.002 0.005 0.01 0.02 0.05 0.1 0.15 0.2 0.3 0.5 1 2.5 5
```

`0.2` is an **exact boundary**, which is the whole point: the SLO can be read
straight off the `le="0.2"` bucket without interpolating a quantile across it.

Alerting rules live in `deploy/prometheus/alerts.yml` and the Grafana dashboard
in `deploy/grafana/dashboards/red.json`.

### Admin endpoints

| endpoint | port | behaviour |
|---|---|---|
| `/metrics` | 9090 | Prometheus exposition, OpenMetrics negotiated |
| `/healthz` | 9090 | 200 once the server is serving — liveness |
| `/readyz` | 9090 | 200 only while at least one backend is healthy — readiness |
| `/backends` | 9090 | JSON: id, addr, state, breaker_state, weight, healthy |
| `/debug/pprof/` | 9090 | the standard `net/http/pprof` surface |
| `/healthz`, `/metrics` | 9101 | the same idea on each demo backend |

---

## SLO methodology

The SLO is **p99 response time under 200 ms**, and each of those words is doing
work.

**Open loop, not closed loop.** In `-mode open` the generator schedules request
*i* at a fixed `start + i/rps` and fires it whether or not earlier requests have
come back. A closed-loop generator — issue, wait, issue again — cannot do this:
when the server slows down, the client automatically slows down with it and
stops sending exactly the requests that would have been slowest. `-mode closed`
exists for comparison, and its numbers are not an SLO.

**Coordinated omission is avoided by measuring from the scheduled time.** The
report carries two timers for every request:

- **service time** — send to response. What the server took.
- **response time** — *scheduled* to response. What the caller experienced,
  including the time the request sat waiting for a worker because the previous
  ones had not finished.

They are equal only while the system keeps up. Under load, response time is
larger, and it is the honest one: a user who asked at T does not care that their
request was not put on the wire until T+50ms. **The SLO is evaluated against
p99 response time.** Quoting p99 service time from a client that fell behind is
the classic way to publish a latency number that is off by an order of
magnitude.

**Percentiles come from the full sorted sample, nearest-rank.** Every
measurement is retained and sorted at the end. No streaming estimator, no
t-digest, no averaging of pre-computed quantiles — the last of which is not a
percentile of anything.

**Warmup is discarded.** `-warmup` requests run before the measurement window so
that connection setup, TLS-free handshakes, lazy `grpc.NewClient` resolution and
Go's JIT-free-but-still-cold caches are not in the sample.

**The server-side view uses histograms, and aggregation happens before the
quantile.** In PromQL that means
`histogram_quantile(0.99, sum by (le) (rate(lb_request_duration_seconds_bucket[1m])))`
and never an average of per-instance p99s.

Client-side p99 and server-side p99 will not match exactly, and that is
information: the difference is network plus the client's own scheduling.

---

## Measured results

Produced by `cmd/loadgen` in open-loop mode. Latency covers **successful requests only**; failures
are summarised separately and governed by an error budget, so a run that sheds load cannot report a
passing SLO.

**Environment.** Apple M2 Pro, 10 cores, macOS 26.5.2, Go 1.25.0, grpc-go v1.83.1. Load generator,
load balancer and all three backends run as separate processes **on the same host** and compete for
those 10 cores. Backends run with **no injected delay** (`-delay 0`), so these figures isolate proxy
overhead rather than upstream service time. Payload 128 B, unary, `-keys 5000`.

**Invocation.** Every row of the table below used exactly these flags, varying only `-rps`:

```sh
./bin/loadgen -target 127.0.0.1:8080 -rps <R> -duration 15s \
  -concurrency 500 -conns 16 -keys 5000 -warmup 1000 -slo 200ms
```

### Latency vs offered load, through the load balancer

| Offered | Achieved | Requests | Errors | p50 | p95 | **p99** | max | Split keys | SLO |
|--------:|---------:|---------:|-------:|----:|----:|--------:|----:|-----------:|:----|
| 5,000  | 5,000  | 75,000  | 0 | 0.26 | 0.56 | **1.11** | 26.3 | 0 | PASS |
| 10,000 | 10,000 | 150,000 | 0 | 0.33 | 0.71 | **1.66** | 35.2 | 0 | PASS |
| 20,000 | 19,960 | 300,000 | 0 | 0.57 | 4.74 | **32.04** | 78.9 | 0 | PASS |
| 25,000 | 24,999 | 375,000 | 0 | 0.75 | 3.65 | **9.72** | 42.7 | 0 | PASS |
| 30,000 | 29,999 | 450,000 | 0 | 1.14 | 10.22 | **40.32** | 81.4 | 0 | PASS |
| 35,000 | 34,998 | 525,000 | 0 | 1.88 | 19.66 | **45.29** | 82.6 | 0 | PASS |
| 40,000 | 39,994 | 600,000 | 0 | 3.56 | 49.10 | **96.90** | 133.8 | 0 | PASS |
| 45,000 | 44,995 | 675,000 | 0 | 6.06 | — | **69.40** | 118.2 | 0 | PASS |
| 50,000 | 49,940 | 750,000 | 0 | 18.05 | — | **65.96** | 99.9 | 0 | PASS |
| 55,000 | 52,614 | — | 0 | — | — | **696.07** | — | — | **FAIL** |

Milliseconds, response time (scheduled → complete). "Split keys" is the number of the 5,000 session
keys that were served by more than one backend — zero at every rate, so affinity held throughout.
Run-to-run variance in the tail is visible (the 20,000 row's p99 exceeds the 25,000 row's); these are
single 15-second runs on a desktop, not an isolated lab.

**Check the host load before trusting any of this.** Every row above was taken with a 1-minute load
average near 4. Re-running the identical 20,000 rps row later, with an unrelated browser workload
pushing the load average to 20-30, produced a p99 of 294 ms instead of 32 ms — a FAIL on the same
code. The tell that this is contention and not the proxy: p50 barely moves (0.43 ms vs 0.34 ms)
while p99 explodes, and a direct-to-backend control run stays at 0.82 ms because it needs two
processes rather than four. The load balancer wants ~2.4 cores of the 10 at these rates, and it is
the first thing to be starved. `uptime` before a benchmark is not optional here.

**Verdict: the 200 ms p99 requirement holds to 50,000 rps**, with 134 ms of headroom there. The host
saturates at roughly **52,000 rps**.

### Two measurement traps this run walked into

Both are worth stating because they each produced a *wrong* number first.

**The load generator starved before the load balancer did.** An earlier sweep reported saturation at
~38,000 rps. That was the generator, not the proxy: with `-concurrency 400`, 400 workers each waiting
on a ~10 ms round trip cannot issue more than ~40,000 rps no matter how fast the target is. Raising
`-concurrency` to 500 and then 800 moved the ceiling to ~52,000 rps, where it stopped moving. **A
saturation point that rises when you add client workers is your own bottleneck.** The 50,000 rps
result was then re-checked with the rate limiter raised from 50,000 to 500,000, to confirm the
limiter was not the thing being measured; 55,000 rps still saturated at ~52,600.

**Service time hides the failure that response time shows.** At 55,000 rps, service time (send →
response) p99 stayed at **38 ms** while response time (scheduled → response) reached **696 ms**. The
system was 18x past its latency budget and the naive timer showed no problem at all. A closed-loop
generator stops issuing load exactly when the system stalls, so it never measures the backlog it
caused. Open-loop scheduling — every request pinned to `start + i/rps` regardless of whether the
previous one finished — is what makes the failure visible.

### Cost of the proxy hop

Measured against a backend directly versus through the load balancer, same host, same load:

| | Direct | Via LB | Added |
|---|-------:|-------:|------:|
| p50 @ 20,000 rps | 0.12 | 0.57 | +0.45 |
| p99 @ 20,000 rps | 0.47 | 32.04 | — |

Median cost of the hop is a few hundred microseconds. The tail is not a like-for-like comparison:
the load balancer is a third process contending for the same 10 cores and is the hottest one at load
(2.4 cores at 25,000 rps, measured with `ps`). On dedicated hardware the tail gap would be smaller;
this is a co-resident worst case.

### Failover under load, and what at-most-once costs

A backend `SIGKILL`ed mid-run (no graceful drain) at 8,000 rps across 3,000 session keys, run three
times under each retry policy:

| `routing.retry_policy` | Errors / 192,000 | Error rate | p99 (ms) | Delivery |
|---|---:|---:|---:|---|
| `connect-failure` (default) | 10, 7, 3 | ~0.004% | 1.7, 1.3 | at-most-once |
| `unavailable` | 0, 0, 0 | 0% | 1.5, 1.0 | at-least-once |

That is the whole trade, measured. The default surfaces roughly **four failed requests per hundred
thousand** during a hard kill of one backend in three, and costs nothing in latency. Those handful
of requests are exactly the ones that were already delivered to the dying backend: replaying them is
what the other policy does, and for a method with side effects that means executing it twice.

Under either policy the routing behaviour is identical:

```
requests  192000 total, 191993 ok, 7 errors
backends  backend-1=76343  backend-2=22964  backend-3=92693
keys      3000 distinct, 961 with more than one backend
lb_failovers_total  0 -> 5243
```

Thousands of requests still fail over transparently even under the safe default, because a dead
backend's connection failures are precisely the case where the request provably never arrived. What
the default declines is the *ambiguous* case, not failover in general.

The stickiness column is the consistent-hashing guarantee, measured: 961 of 3,000 keys (32%) changed
backend — almost exactly the dead backend's share of the ring. The other 68% never moved. A naive
`hash(key) % N` would have remapped **every** key.

Choose `unavailable` only when every method behind the balancer is idempotent. `none` declines even
the safe replay, which is the right setting when the caller does its own retrying.

### Middleware, measured

| Mechanism | Configuration | Observed |
|---|---|---|
| Rate limiting | `rps 2000, burst 2000`, offered 6,000 rps | 13,998 allowed / 22,002 `ResourceExhausted`; `lb_rate_limited_total` matched exactly |
| Circuit breaking | `min_requests 20, failure_ratio 0.5`, all backends failing | 21 requests reached backends, then **3,979 of 4,000 fail-fast** without a network hop |
| Recovery | all backends restored | every breaker returned to closed and load rebalanced across all three |

### Reproducing

```sh
make build
./bin/backend -listen 127.0.0.1:50051 -admin-listen 127.0.0.1:9101 -id backend-1 &
./bin/backend -listen 127.0.0.1:50052 -admin-listen 127.0.0.1:9102 -id backend-2 &
./bin/backend -listen 127.0.0.1:50053 -admin-listen 127.0.0.1:9103 -id backend-3 &
./bin/lb -config config/lb.local.yaml &
./bin/loadgen -target 127.0.0.1:8080 -rps 50000 -duration 15s \
  -concurrency 800 -conns 24 -keys 5000 -warmup 1000
```

Numbers will differ on other hardware. Because everything is co-resident, the saturation point
measures *this host*, not the load balancer in isolation: roughly a third of the CPU here is spent
generating load rather than serving it. If you raise `-rps` past the knee, raise `-concurrency`
first and confirm the ceiling stops moving — otherwise you are measuring the generator.

---

## Troubleshooting

**`config: parse ...: field X not found in type ...`**
Unknown YAML keys are rejected on purpose. Check the spelling against the
options table above.

**`config: ...: backends: at least one backend is required`**
The config file has no `backends` list and `LB_BACKENDS` is unset. With no
`-config` at all, `LB_BACKENDS` is the only way to name a backend.

**LB starts, but `/readyz` returns 503**
No backend has passed `health.rise` consecutive probes. `curl
localhost:9090/backends` shows each one's state. The usual cause is an address
the LB cannot reach from where it is running: `config/lb.yaml` uses compose
service names and only resolves inside the compose network, while
`config/lb.local.yaml` uses `127.0.0.1` and only works on the host.

**Every RPC returns `Unavailable` with `circuit open`**
A method breaker tripped. It closes itself after `circuit_breaker.open_timeout`
plus `half_open_max` successful probes. Watch `lb_circuit_state`; if it will not
stay closed, the upstream is genuinely failing.

**Every RPC returns `ResourceExhausted`**
The rate limiter. Either `rate_limit.rps` is below your offered load, or
`per_client` is on and one client key is absorbing the whole per-client bucket.
`lb_rate_limited_total` confirms it.

**`loadgen`: `target ... is not accepting connections`**
Nothing is listening on `-target`. For `make bench` that means `make run-lb` is
not running, or it exited — check its output.

**Achieved rps is well below requested rps**
The generator is the bottleneck, not the server, and the latency numbers are
optimistic. Raise `-conns` and `-concurrency`, or lower `-rps`.

**p99 is around 25-30 ms on a local run and that seems high**
Expected: `make run-backends` injects 25 ms into backend-3, and roughly a third
of keys hash to it. Restart the backends with equal `-delay` if you want to
measure the proxy.

**Grafana shows "No data"**
Check <http://localhost:9091/targets>. If `lb` or `backends` are down, the
containers are not up or the ports moved. If targets are up but panels are
empty, no traffic has been sent yet — run the `bench` profile.

**Grafana panels say "datasource not found"**
The dashboard pins the datasource uid `nw-lb-prometheus`, defined in
`deploy/grafana/provisioning/datasources/prometheus.yml`. If you renamed one,
rename both.

**`docker compose exec lb sh` fails**
There is no shell in the image — distroless, single static binary, by design.
Use `docker compose logs -f lb`.

**Docker on Linux: loadgen cannot write `/bench/results`**
The container runs as uid 65532 and the bind-mounted host directory is owned by
you. `chmod 777 bench/results`, or add `user: "${UID}:${GID}"` to the `loadgen`
service. Docker Desktop on macOS and Windows maps ownership and needs neither.

**`make run-backends` says backends are already running**
A stale `bin/.run/backends.pid` from a run that was killed. `make stop-backends`,
or delete the file.

**Port 9090 is already in use**
Something else on the host owns it — often another Prometheus. Change
`admin_listen`, pass `-admin-listen`, or set `LB_ADMIN_LISTEN`.

---

## Make targets

`make` with no arguments prints them all.

| target | what it does |
|---|---|
| `build` | build `lb`, `backend`, `loadgen` into `./bin` |
| `proto` | regenerate `gen/echo/v1` (the generated code is committed; only needed if the `.proto` changes) |
| `test` / `test-race` / `vet` / `lint` / `cover` | the check suite |
| `run-backends` / `stop-backends` / `run-lb` | the local three-backend topology |
| `bench` / `bench-baseline` | measure through the LB, and straight at a backend |
| `docker-build` / `up` / `down` / `logs` | the compose stack |
| `clean` | remove `bin/` (build output and run state), coverage and benchmark reports |

## Layout

```
cmd/lb              the load balancer
cmd/backend         the demo upstream (EchoService + grpc health + reflection)
cmd/loadgen         the measurement tool
internal/config     three-layer configuration
internal/hashring   consistent hash ring, lock-free reads
internal/circuit    rolling-window circuit breaker
internal/ratelimit  sharded token buckets
internal/metrics    every Prometheus collector
internal/pool       backend identity, conn, health state, per-backend breaker
internal/middleware the interceptor chain
internal/health     the gRPC health checker
internal/balancer   ring plus health filtering
internal/proxy      the transparent L7 forwarder and its passthrough codec
test/e2e            end-to-end tests over the whole stack
config/             lb.yaml (compose) and lb.local.yaml (host)
deploy/docker       one Dockerfile per binary
deploy/prometheus   scrape config and alerting rules
deploy/grafana      provisioning and the RED dashboard
bench/results       loadgen JSON reports (gitignored)
docs/CONTRACT.md    the authoritative package API contract
```
