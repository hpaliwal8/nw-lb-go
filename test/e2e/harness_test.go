package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	echov1 "github.com/hitanshpaliwal/nw-lb-go/gen/echo/v1"
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

const (
	sessionHeader     = "x-session-id"
	tokenHeader       = "x-test-token"
	tokenValue        = "transparent-proxy"
	backendIDHeader   = "x-backend-id"
	backendTrailerKey = "x-backend-trailer"
	forwardedForKey   = "x-forwarded-for"

	// injectedMessage is asserted byte for byte: the point of the status-fidelity test is that
	// nothing between the backend handler and the generated client rewrites a status message.
	injectedMessage = "e2e: injected backend failure"
)

const echoMethod = echov1.EchoService_Echo_FullMethodName

// pollInterval is how often a wait helper re-tests its condition. Everything these tests wait on
// is driven by a goroutine already running, so a short poll converges as fast as the event does.
const pollInterval = time.Millisecond

// waitFor blocks until cond reports true, and fails the test if that has not happened by timeout.
// Every wait in this package goes through here: an unconditional sleep would either be slower than
// the event it waits for or, worse, shorter than it on a loaded machine.
func waitFor(tb testing.TB, timeout time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(pollInterval)
	}
}

// echoBackend is a real EchoService server with the standard gRPC health service. It can be
// stopped and restarted on the same address, which is what the failover test needs to prove the
// health checker readmits a recovered backend.
type echoBackend struct {
	echov1.UnimplementedEchoServiceServer

	id string

	// mu guards the serving instance, which restart replaces wholesale. Handlers never take it:
	// they only read id, the atomics and the request.
	mu      sync.Mutex
	srv     *grpc.Server
	hs      *grpchealth.Server
	lis     net.Listener
	addr    string
	running bool

	calls    atomic.Int64
	lastMD   atomic.Pointer[metadata.MD]
	failCode atomic.Int32
	delayMS  atomic.Int32
}

func newEchoBackend(tb testing.TB, id string) *echoBackend {
	tb.Helper()
	b := &echoBackend{id: id}
	b.start(tb, "127.0.0.1:0")
	tb.Cleanup(b.stop)
	return b
}

func (b *echoBackend) start(tb testing.TB, addr string) {
	tb.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running {
		return
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		tb.Fatalf("backend %s: listen on %s: %v", b.id, addr, err)
	}
	srv := grpc.NewServer()
	hs := grpchealth.NewServer()
	echov1.RegisterEchoServiceServer(srv, b)
	healthpb.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	b.srv, b.hs, b.lis, b.addr, b.running = srv, hs, lis, lis.Addr().String(), true
	go func() { _ = srv.Serve(lis) }()
}

// restart brings the backend back up on the address it owned before, so the load balancer's
// existing connection and ring membership stay valid.
func (b *echoBackend) restart(tb testing.TB) {
	tb.Helper()
	b.start(tb, b.address())
}

// stop takes the backend down hard from the client's point of view: the listener closes so the
// address stops accepting, and GracefulStop tears down the established connections.
func (b *echoBackend) stop() {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return
	}
	srv, lis := b.srv, b.lis
	b.running = false
	b.mu.Unlock()

	_ = lis.Close()
	srv.GracefulStop()
}

func (b *echoBackend) address() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.addr
}

// setServing flips the health service answer without touching the data path, which is how a
// backend is removed from the healthy set while still able to serve.
func (b *echoBackend) setServing(serving bool) {
	b.mu.Lock()
	hs := b.hs
	b.mu.Unlock()
	if hs == nil {
		return
	}
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		st = healthpb.HealthCheckResponse_SERVING
	}
	hs.SetServingStatus("", st)
}

func (b *echoBackend) injectFailure(c codes.Code, delay time.Duration) {
	b.delayMS.Store(int32(delay.Milliseconds()))
	b.failCode.Store(int32(c))
}

func (b *echoBackend) clearFailure() {
	b.failCode.Store(int32(codes.OK))
	b.delayMS.Store(0)
}

func (b *echoBackend) metadataSeen() metadata.MD {
	if md := b.lastMD.Load(); md != nil {
		return *md
	}
	return nil
}

func (b *echoBackend) observe(ctx context.Context) {
	b.calls.Add(1)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		b.lastMD.Store(&md)
	}
}

// apply reproduces the demo backend's fault knobs: the per-request delay_ms/fail_code fields and
// the process-wide injection a test installs with injectFailure.
func (b *echoBackend) apply(req *echov1.EchoRequest) error {
	if d := time.Duration(b.delayMS.Load()) * time.Millisecond; d > 0 {
		time.Sleep(d)
	}
	if d := time.Duration(req.GetDelayMs()) * time.Millisecond; d > 0 {
		time.Sleep(d)
	}
	if c := codes.Code(b.failCode.Load()); c != codes.OK {
		return status.Error(c, injectedMessage)
	}
	if c := codes.Code(req.GetFailCode()); c != codes.OK {
		return status.Error(c, injectedMessage)
	}
	return nil
}

func (b *echoBackend) reply(req *echov1.EchoRequest, seq uint32) *echov1.EchoResponse {
	return &echov1.EchoResponse{
		Payload:        req.GetPayload(),
		BackendId:      b.id,
		BackendAddr:    b.address(),
		Seq:            seq,
		ServedUnixNano: time.Now().UnixNano(),
	}
}

func (b *echoBackend) Echo(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	b.observe(ctx)
	_ = grpc.SetHeader(ctx, metadata.Pairs(backendIDHeader, b.id))
	_ = grpc.SetTrailer(ctx, metadata.Pairs(backendTrailerKey, b.id))
	if err := b.apply(req); err != nil {
		return nil, err
	}
	return b.reply(req, 0), nil
}

func (b *echoBackend) ServerStream(req *echov1.EchoRequest, ss grpc.ServerStreamingServer[echov1.EchoResponse]) error {
	b.observe(ss.Context())
	_ = ss.SetHeader(metadata.Pairs(backendIDHeader, b.id))
	ss.SetTrailer(metadata.Pairs(backendTrailerKey, b.id))
	if err := b.apply(req); err != nil {
		return err
	}
	for i := range int(req.GetStreamCount()) {
		if err := ss.Send(b.reply(req, uint32(i))); err != nil {
			return err
		}
	}
	return nil
}

func (b *echoBackend) ClientStream(cs grpc.ClientStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	_ = cs.SetHeader(metadata.Pairs(backendIDHeader, b.id))
	cs.SetTrailer(metadata.Pairs(backendTrailerKey, b.id))

	var (
		count uint32
		last  = &echov1.EchoRequest{}
	)
	for {
		req, err := cs.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		b.observe(cs.Context())
		count++
		last = req
	}
	if err := b.apply(last); err != nil {
		return err
	}
	// Seq carries the number of messages the backend actually received, so the client can prove
	// the proxy forwarded every one of them exactly once.
	return cs.SendAndClose(b.reply(last, count))
}

func (b *echoBackend) BidiStream(bs grpc.BidiStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	_ = bs.SetHeader(metadata.Pairs(backendIDHeader, b.id))
	bs.SetTrailer(metadata.Pairs(backendTrailerKey, b.id))

	var seq uint32
	for {
		req, err := bs.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		b.observe(bs.Context())
		if err := b.apply(req); err != nil {
			return err
		}
		if err := bs.Send(b.reply(req, seq)); err != nil {
			return err
		}
		seq++
	}
}

// testLB is the whole stack: backends, the load balancer process wired exactly as cmd/lb wires
// it, and a generated client dialled at the load balancer.
type testLB struct {
	cfg      config.Config
	backends []*echoBackend

	metrics  *metrics.Metrics
	pool     *pool.Pool
	health   *health.Checker
	bal      *balancer.Balancer
	limiter  *ratelimit.Limiter
	breakers *circuit.Registry

	srv    *grpc.Server
	addr   string
	conn   *grpc.ClientConn
	client echov1.EchoServiceClient

	healthCancel context.CancelFunc
	stopOnce     sync.Once
	shutdownOnce sync.Once
}

type lbOptions struct {
	backends int
	mutate   func(*config.Config)
}

type lbOption func(*lbOptions)

func withBackends(n int) lbOption {
	return func(o *lbOptions) { o.backends = n }
}

// withConfig runs f over the configuration after the backend list has been filled in, so a test
// can retune any knob without restating the whole config.
func withConfig(f func(*config.Config)) lbOption {
	return func(o *lbOptions) { o.mutate = f }
}

// newTestLB builds the production pipeline in process: config -> metrics -> pool -> health checker
// -> balancer -> proxy -> middleware chain -> grpc.Server, then dials it with the generated
// echov1 client. It returns only once every backend has been marked healthy by the real health
// checker, so a test never races the first probe.
func newTestLB(tb testing.TB, opts ...lbOption) *testLB {
	tb.Helper()

	o := lbOptions{backends: 3}
	for _, opt := range opts {
		opt(&o)
	}

	lb := &testLB{}
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.AdminListen = "127.0.0.1:0"
	// Fast enough that a health transition is observed in milliseconds, slow enough that the
	// probes themselves are not the dominant load on a backend.
	cfg.Health = config.Health{Interval: 25 * time.Millisecond, Timeout: time.Second, Rise: 1, Fall: 2}
	cfg.CircuitBreaker.Enabled = true
	cfg.Logging.Level = "error"

	for i := range o.backends {
		b := newEchoBackend(tb, fmt.Sprintf("backend-%d", i+1))
		lb.backends = append(lb.backends, b)
		cfg.Backends = append(cfg.Backends, config.Backend{
			ID:     b.id,
			Addr:   b.address(),
			Weight: config.DefaultWeight,
		})
	}
	if o.mutate != nil {
		o.mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config: %v", err)
	}
	lb.cfg = cfg

	log := slog.New(slog.DiscardHandler)

	lb.metrics = metrics.New()

	p, err := pool.New(cfg, lb.metrics)
	if err != nil {
		tb.Fatalf("pool.New: %v", err)
	}
	lb.pool = p

	ctx, cancel := context.WithCancel(context.Background())
	lb.healthCancel = cancel
	lb.health = health.New(p, cfg.Health, lb.metrics, log)
	lb.health.Start(ctx)

	lb.bal = balancer.New(p, cfg.Routing, lb.metrics)
	px := proxy.New(lb.bal, cfg, lb.metrics, log)

	lb.limiter = ratelimit.New(ratelimit.Config{
		Enabled:        cfg.RateLimit.Enabled,
		RPS:            cfg.RateLimit.RPS,
		Burst:          cfg.RateLimit.Burst,
		PerClient:      cfg.RateLimit.PerClient,
		PerClientRPS:   cfg.RateLimit.PerClientRPS,
		PerClientBurst: cfg.RateLimit.PerClientBurst,
	})
	if cfg.CircuitBreaker.Enabled {
		lb.breakers = circuit.NewRegistry(circuit.Settings{
			Window:       cfg.CircuitBreaker.Window,
			Buckets:      cfg.CircuitBreaker.Buckets,
			MinRequests:  cfg.CircuitBreaker.MinRequests,
			FailureRatio: cfg.CircuitBreaker.FailureRatio,
			OpenTimeout:  cfg.CircuitBreaker.OpenTimeout,
			HalfOpenMax:  cfg.CircuitBreaker.HalfOpenMax,
			OnStateChange: func(name string, _, to circuit.State) {
				lb.metrics.SetCircuitState(name, int(to))
			},
		})
	}

	// The pipeline order is fixed by the contract; anything else changes what the breaker and the
	// rate limiter can observe.
	chain := middleware.Chain(
		middleware.Recovery(log, lb.metrics),
		middleware.Context(cfg.Routing.HashHeader),
		middleware.Logging(log, cfg.Logging),
		middleware.Metrics(lb.metrics),
		middleware.RateLimit(lb.limiter, lb.metrics),
		middleware.CircuitBreak(lb.breakers, lb.metrics),
	)

	sopts := append(px.ServerOptions(),
		grpc.ChainStreamInterceptor(chain),
		grpc.MaxRecvMsgSize(cfg.Proxy.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.Proxy.MaxSendMsgSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		// The pool dials with PermitWithoutStream, and so does the test client; without a matching
		// enforcement policy the server would answer an idle ping with ENHANCE_YOUR_CALM.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.NumStreamWorkers(uint32(runtime.NumCPU())),
	)
	lb.srv = grpc.NewServer(sopts...)

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		tb.Fatalf("listen %s: %v", cfg.Listen, err)
	}
	lb.addr = lis.Addr().String()
	go func() { _ = lb.srv.Serve(lis) }()

	conn, err := grpc.NewClient(lb.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("grpc.NewClient(%s): %v", lb.addr, err)
	}
	lb.conn = conn
	lb.client = echov1.NewEchoServiceClient(conn)

	tb.Cleanup(lb.shutdown)

	waitFor(tb, 10*time.Second, "every backend to be marked healthy", func() bool {
		return len(p.Healthy()) == len(lb.backends)
	})
	return lb
}

// gracefulStop drains the load balancer's own server. It is separate from shutdown so a test can
// assert on the drain itself and still get the rest of the teardown from Cleanup.
func (lb *testLB) gracefulStop() {
	lb.stopOnce.Do(lb.srv.GracefulStop)
}

func (lb *testLB) shutdown() {
	lb.shutdownOnce.Do(func() {
		if lb.conn != nil {
			_ = lb.conn.Close()
		}
		lb.gracefulStop()
		lb.health.Stop()
		lb.healthCancel()
		lb.bal.Close()
		_ = lb.pool.Close()
		lb.limiter.Close()
		for _, b := range lb.backends {
			b.stop()
		}
	})
}

func (lb *testLB) backend(tb testing.TB, id string) *echoBackend {
	tb.Helper()
	for _, b := range lb.backends {
		if b.id == id {
			return b
		}
	}
	tb.Fatalf("no backend with id %q", id)
	return nil
}

func (lb *testLB) poolBackend(tb testing.TB, id string) *pool.Backend {
	tb.Helper()
	b, ok := lb.pool.Get(id)
	if !ok {
		tb.Fatalf("pool has no backend %q", id)
	}
	return b
}

func (lb *testLB) backendIDs() []string {
	ids := make([]string, len(lb.backends))
	for i, b := range lb.backends {
		ids[i] = b.id
	}
	return ids
}

// eachBackend applies f to every upstream, which is how a test injects the same fault everywhere.
func (lb *testLB) eachBackend(f func(*echoBackend)) {
	for _, b := range lb.backends {
		f(b)
	}
}

// ownerOf returns the backend that served one call for key. It is the only honest way to observe
// routing from outside: the client is told nothing about the ring.
func (lb *testLB) ownerOf(tb testing.TB, ctx context.Context, key string) string {
	tb.Helper()
	resp, err := lb.client.Echo(withSession(ctx, key), &echov1.EchoRequest{SessionKey: key})
	if err != nil {
		tb.Fatalf("echo %q: %v", key, err)
	}
	return resp.GetBackendId()
}

func (lb *testLB) ownerMap(tb testing.TB, ctx context.Context, keys []string) map[string]string {
	tb.Helper()
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[k] = lb.ownerOf(tb, ctx, k)
	}
	return out
}

func withSession(ctx context.Context, key string) context.Context {
	if key == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, sessionHeader, key)
}

func sessionKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("session-%05d", i)
	}
	return keys
}

func testContext(tb testing.TB) context.Context {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	tb.Cleanup(cancel)
	return ctx
}

// sample is one line of the Prometheus exposition format.
type sample struct {
	labels map[string]string
	value  float64
}

// scrape reads the real metrics handler rather than the collectors, so what the tests assert on is
// exactly what a Prometheus server would see.
func (lb *testLB) scrape(tb testing.TB) map[string][]sample {
	tb.Helper()
	rec := httptest.NewRecorder()
	lb.metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		tb.Fatalf("metrics handler returned %d", rec.Code)
	}
	return parseExposition(rec.Body.String())
}

func parseExposition(body string) map[string][]sample {
	out := make(map[string][]sample)
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var (
			name   string
			rest   string
			labels map[string]string
		)
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				continue
			}
			name = line[:i]
			labels = parseLabels(line[i+1 : j])
			rest = strings.TrimSpace(line[j+1:])
		} else {
			sp := strings.IndexByte(line, ' ')
			if sp < 0 {
				continue
			}
			name, rest = line[:sp], strings.TrimSpace(line[sp+1:])
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		out[name] = append(out[name], sample{labels: labels, value: v})
	}
	return out
}

// parseLabels handles the label sets this project emits: no value contains a comma, a quote or a
// backslash, so splitting is unambiguous.
func parseLabels(s string) map[string]string {
	out := make(map[string]string)
	for part := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		out[k] = strings.Trim(v, `"`)
	}
	return out
}

func matches(s sample, want map[string]string) bool {
	for k, v := range want {
		if s.labels[k] != v {
			return false
		}
	}
	return true
}

// metricSum totals every series of name whose labels include want. A missing metric sums to zero,
// which is the right reading for a counter that has never been incremented.
func metricSum(m map[string][]sample, name string, want map[string]string) float64 {
	var total float64
	for _, s := range m[name] {
		if matches(s, want) {
			total += s.value
		}
	}
	return total
}

func metricValue(tb testing.TB, m map[string][]sample, name string, want map[string]string) float64 {
	tb.Helper()
	found := false
	var v float64
	for _, s := range m[name] {
		if matches(s, want) {
			if found {
				tb.Fatalf("metric %s%v matched more than one series", name, want)
			}
			found, v = true, s.value
		}
	}
	if !found {
		tb.Fatalf("metric %s%v not found", name, want)
	}
	return v
}

// percentile is nearest-rank over the full sample, the same definition cmd/loadgen uses. Summaries
// and interpolation both hide exactly the tail these tests are about.
func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func sortedDurations(d []time.Duration) []time.Duration {
	out := slices.Clone(d)
	slices.Sort(out)
	return out
}
