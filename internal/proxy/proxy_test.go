package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	echov1 "github.com/hitanshpaliwal/nw-lb-go/gen/echo/v1"
	"github.com/hitanshpaliwal/nw-lb-go/internal/balancer"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/middleware"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
)

const (
	sessionHeader  = "x-session-id"
	backendHeader  = "x-backend-header"
	backendTrailer = "x-backend-trailer"
	tokenHeader    = "x-test-token"
)

// echoHook replaces the default unary behaviour for a test. It is fixed before the backend starts
// serving so no RPC can observe it changing.
type echoHook func(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error)

// backend is a real EchoService implementation. Everything a test asserts on is an atomic, because
// the assertion runs on the test goroutine while the value was written on a server goroutine.
type backend struct {
	echov1.UnimplementedEchoServiceServer

	id   string
	addr string
	srv  *grpc.Server
	hs   *grpchealth.Server

	onEcho echoHook

	// delivered counts request messages actually handed to this backend's handlers. A retry that
	// duplicated a request would show up here and nowhere else.
	delivered atomic.Int64
	lastMD    atomic.Pointer[metadata.MD]
	// deadlineNS is the budget the backend saw on its own context, 0 when the call carried none.
	deadlineNS atomic.Int64

	stopOnce sync.Once
}

func (b *backend) reply(req *echov1.EchoRequest, seq uint32) *echov1.EchoResponse {
	return &echov1.EchoResponse{
		Payload:        req.GetPayload(),
		BackendId:      b.id,
		BackendAddr:    b.addr,
		Seq:            seq,
		ServedUnixNano: time.Now().UnixNano(),
	}
}

func (b *backend) observe(ctx context.Context) {
	b.delivered.Add(1)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		b.lastMD.Store(&md)
	}
	var budget int64
	if dl, ok := ctx.Deadline(); ok {
		budget = int64(time.Until(dl))
	}
	b.deadlineNS.Store(budget)
}

func (b *backend) Echo(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	b.observe(ctx)
	_ = grpc.SetHeader(ctx, metadata.Pairs(backendHeader, b.id))
	_ = grpc.SetTrailer(ctx, metadata.Pairs(backendTrailer, b.id))
	if b.onEcho != nil {
		return b.onEcho(ctx, req)
	}
	if c := codes.Code(req.GetFailCode()); c != codes.OK {
		return nil, status.Errorf(c, "backend %s refused", b.id)
	}
	return b.reply(req, 0), nil
}

func (b *backend) ServerStream(req *echov1.EchoRequest, ss grpc.ServerStreamingServer[echov1.EchoResponse]) error {
	b.observe(ss.Context())
	_ = grpc.SetTrailer(ss.Context(), metadata.Pairs(backendTrailer, b.id))
	for i := range int(req.GetStreamCount()) {
		if err := ss.Send(b.reply(req, uint32(i))); err != nil {
			return err
		}
	}
	if c := codes.Code(req.GetFailCode()); c != codes.OK {
		return status.Errorf(c, "backend %s failed after %d messages", b.id, req.GetStreamCount())
	}
	return nil
}

func (b *backend) ClientStream(cs grpc.ClientStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	_ = grpc.SetTrailer(cs.Context(), metadata.Pairs(backendTrailer, b.id))
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
	if c := codes.Code(last.GetFailCode()); c != codes.OK {
		return status.Errorf(c, "backend %s refused", b.id)
	}
	return cs.SendAndClose(b.reply(last, count))
}

func (b *backend) BidiStream(bs grpc.BidiStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	_ = grpc.SetTrailer(bs.Context(), metadata.Pairs(backendTrailer, b.id))
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
		if c := codes.Code(req.GetFailCode()); c != codes.OK {
			return status.Errorf(c, "backend %s refused", b.id)
		}
		if err := bs.Send(b.reply(req, seq)); err != nil {
			return err
		}
		seq++
	}
}

func (b *backend) stop()         { b.stopOnce.Do(b.srv.Stop) }
func (b *backend) gracefulStop() { b.stopOnce.Do(b.srv.GracefulStop) }

func startBackend(tb testing.TB, id string, hook echoHook) *backend {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	b := &backend{
		id:     id,
		addr:   lis.Addr().String(),
		srv:    grpc.NewServer(),
		hs:     grpchealth.NewServer(),
		onEcho: hook,
	}
	echov1.RegisterEchoServiceServer(b.srv, b)
	healthpb.RegisterHealthServer(b.srv, b.hs)
	b.hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = b.srv.Serve(lis) }()
	tb.Cleanup(b.stop)
	return b
}

type harness struct {
	backends []*backend
	pool     *pool.Pool
	bal      *balancer.Balancer
	m        *metrics.Metrics
	proxy    *Proxy
	conn     *grpc.ClientConn
	client   echov1.EchoServiceClient
}

type harnessOption func(*harnessConfig)

type harnessConfig struct {
	hook         echoHook
	interceptors []grpc.StreamServerInterceptor
	mutate       func(*config.Config)
}

func withEchoHook(h echoHook) harnessOption {
	return func(c *harnessConfig) { c.hook = h }
}

func withInterceptors(is ...grpc.StreamServerInterceptor) harnessOption {
	return func(c *harnessConfig) { c.interceptors = is }
}

func withConfig(f func(*config.Config)) harnessOption {
	return func(c *harnessConfig) { c.mutate = f }
}

// newHarness wires the real chain — config, metrics, pool, balancer, proxy — behind a real
// grpc.Server, and dials it with the generated client. Nothing on the client side knows a proxy is
// involved, which is the property the whole package exists to provide.
func newHarness(tb testing.TB, n int, opts ...harnessOption) *harness {
	tb.Helper()

	var hc harnessConfig
	for _, o := range opts {
		o(&hc)
	}

	cfg := config.Default()
	h := &harness{}
	for i := range n {
		b := startBackend(tb, fmt.Sprintf("backend-%d", i+1), hc.hook)
		h.backends = append(h.backends, b)
		cfg.Backends = append(cfg.Backends, config.Backend{ID: b.id, Addr: b.addr, Weight: config.DefaultWeight})
	}
	if hc.mutate != nil {
		hc.mutate(&cfg)
	}

	h.m = metrics.New()
	p, err := pool.New(cfg, h.m)
	if err != nil {
		tb.Fatalf("pool.New: %v", err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	h.pool = p

	// The health checker is not part of this package; marking the pool healthy up front is what it
	// would have done after its first successful probe.
	for _, b := range p.Backends() {
		b.SetState(pool.StateHealthy)
	}
	p.NotifyChange()

	h.bal = balancer.New(p, cfg.Routing, h.m)
	tb.Cleanup(h.bal.Close)
	h.proxy = New(h.bal, cfg, h.m, slog.New(slog.DiscardHandler))

	sopts := h.proxy.ServerOptions()
	if len(hc.interceptors) > 0 {
		sopts = append(sopts, grpc.ChainStreamInterceptor(hc.interceptors...))
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(sopts...)
	go func() { _ = srv.Serve(lis) }()
	tb.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("grpc.NewClient: %v", err)
	}
	tb.Cleanup(func() { _ = conn.Close() })
	h.conn = conn
	h.client = echov1.NewEchoServiceClient(conn)
	return h
}

func (h *harness) byID(tb testing.TB, id string) *backend {
	tb.Helper()
	for _, b := range h.backends {
		if b.id == id {
			return b
		}
	}
	tb.Fatalf("no backend with id %q", id)
	return nil
}

func (h *harness) totalDelivered() int64 {
	var n int64
	for _, b := range h.backends {
		n += b.delivered.Load()
	}
	return n
}

func sessionCtx(ctx context.Context, key string) context.Context {
	if key == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, sessionHeader, key)
}

func testContext(tb testing.TB) context.Context {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	tb.Cleanup(cancel)
	return ctx
}

// metricTotal sums every series of a counter by scraping the registry's own exposition output,
// which keeps the test free of the prometheus data-model package.
func metricTotal(tb testing.TB, m *metrics.Metrics, name string) float64 {
	tb.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var total float64
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			tb.Fatalf("parse %q: %v", line, err)
		}
		total += v
	}
	return total
}

func TestUnaryRoundTrip(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: nil},
		{name: "small", payload: []byte("hello proxy")},
		// Above mem.BufferPoolingThreshold, so the forwarded buffers are genuinely reference
		// counted and a double free would panic instead of passing silently.
		{name: "pooled", payload: bytes.Repeat([]byte("x"), 64<<10)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := h.client.Echo(testContext(t), &echov1.EchoRequest{Payload: tc.payload})
			if err != nil {
				t.Fatalf("Echo: %v", err)
			}
			if !bytes.Equal(resp.GetPayload(), tc.payload) {
				t.Fatalf("payload round-trip mismatch: got %d bytes, want %d", len(resp.GetPayload()), len(tc.payload))
			}
			if resp.GetBackendId() == "" {
				t.Fatal("response carries no backend_id, so the call never reached an upstream")
			}
			h.byID(t, resp.GetBackendId())
		})
	}
}

func TestServerStreamForwarding(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	const want = 25
	payload := []byte("stream")
	stream, err := h.client.ServerStream(testContext(t), &echov1.EchoRequest{Payload: payload, StreamCount: want})
	if err != nil {
		t.Fatalf("ServerStream: %v", err)
	}

	var got []uint32
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv after %d messages: %v", len(got), err)
		}
		if !bytes.Equal(resp.GetPayload(), payload) {
			t.Fatalf("message %d payload = %q, want %q", len(got), resp.GetPayload(), payload)
		}
		got = append(got, resp.GetSeq())
	}
	if len(got) != want {
		t.Fatalf("received %d messages, want %d", len(got), want)
	}
	for i, seq := range got {
		if seq != uint32(i) {
			t.Fatalf("message %d has seq %d: the proxy reordered the stream", i, seq)
		}
	}
}

func TestClientStreamForwarding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msgs int
	}{
		// Zero messages is the legal client-streaming case where the first Recv is io.EOF and the
		// half-close is the only thing that has to reach the upstream.
		{name: "no messages", msgs: 0},
		{name: "one message", msgs: 1},
		{name: "many messages", msgs: 7},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, 3)
			stream, err := h.client.ClientStream(testContext(t))
			if err != nil {
				t.Fatalf("ClientStream: %v", err)
			}
			for i := range tc.msgs {
				if err := stream.Send(&echov1.EchoRequest{Payload: fmt.Appendf(nil, "msg-%d", i)}); err != nil {
					t.Fatalf("Send %d: %v", i, err)
				}
			}
			resp, err := stream.CloseAndRecv()
			if err != nil {
				t.Fatalf("CloseAndRecv: %v", err)
			}
			if got := int(resp.GetSeq()); got != tc.msgs {
				t.Fatalf("backend counted %d messages, want %d", got, tc.msgs)
			}
			if got := h.totalDelivered(); got != int64(tc.msgs) {
				t.Fatalf("backends saw %d messages in total, want %d", got, tc.msgs)
			}
		})
	}
}

func TestBidiStreamForwarding(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	stream, err := h.client.BidiStream(testContext(t))
	if err != nil {
		t.Fatalf("BidiStream: %v", err)
	}

	const n = 8
	for i := range n {
		if err := stream.Send(&echov1.EchoRequest{Payload: fmt.Appendf(nil, "ping-%d", i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if want := fmt.Sprintf("ping-%d", i); string(resp.GetPayload()) != want {
			t.Fatalf("message %d = %q, want %q", i, resp.GetPayload(), want)
		}
		if resp.GetSeq() != uint32(i) {
			t.Fatalf("message %d has seq %d", i, resp.GetSeq())
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after CloseSend = %v, want io.EOF", err)
	}
	if got := h.totalDelivered(); got != n {
		t.Fatalf("backends saw %d messages, want %d", got, n)
	}
}

func TestMetadataReachesBackend(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	ctx := metadata.AppendToOutgoingContext(sessionCtx(testContext(t), "meta-key"), tokenHeader, "s3cret")
	resp, err := h.client.Echo(ctx, &echov1.EchoRequest{})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	md := h.byID(t, resp.GetBackendId()).lastMD.Load()
	if md == nil {
		t.Fatal("backend recorded no incoming metadata")
	}

	tests := []struct {
		key  string
		want string
	}{
		{key: tokenHeader, want: "s3cret"},
		{key: sessionHeader, want: "meta-key"},
	}
	for _, tc := range tests {
		if got := md.Get(tc.key); len(got) != 1 || got[0] != tc.want {
			t.Fatalf("backend metadata %s = %v, want [%q]", tc.key, got, tc.want)
		}
	}
	if got := md.Get(forwardedForHeader); len(got) != 1 || net.ParseIP(got[0]) == nil {
		t.Fatalf("backend metadata %s = %v, want a single IP appended by the proxy", forwardedForHeader, got)
	}
}

func TestHeaderAndTrailerPropagate(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	var header, trailer metadata.MD
	resp, err := h.client.Echo(testContext(t), &echov1.EchoRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got := header.Get(backendHeader); len(got) != 1 || got[0] != resp.GetBackendId() {
		t.Fatalf("response header %s = %v, want [%q]", backendHeader, got, resp.GetBackendId())
	}
	if got := trailer.Get(backendTrailer); len(got) != 1 || got[0] != resp.GetBackendId() {
		t.Fatalf("response trailer %s = %v, want [%q]", backendTrailer, got, resp.GetBackendId())
	}
}

func TestUpstreamStatusPropagates(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	tests := []struct {
		name string
		code codes.Code
	}{
		{name: "invalid argument", code: codes.InvalidArgument},
		{name: "permission denied", code: codes.PermissionDenied},
		{name: "not found", code: codes.NotFound},
		// FailedPrecondition is not on the retry list, so this also proves a non-retryable failure
		// is surfaced rather than replayed onto the next candidate.
		{name: "failed precondition", code: codes.FailedPrecondition},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := h.totalDelivered()
			var trailer metadata.MD
			_, err := h.client.Echo(testContext(t), &echov1.EchoRequest{FailCode: uint32(tc.code)}, grpc.Trailer(&trailer))
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("Echo error = %v, want a gRPC status", err)
			}
			if st.Code() != tc.code {
				t.Fatalf("status code = %s, want %s", st.Code(), tc.code)
			}
			if !strings.HasPrefix(st.Message(), "backend ") || !strings.HasSuffix(st.Message(), " refused") {
				t.Fatalf("status message = %q, want the backend's own message verbatim", st.Message())
			}
			if got := trailer.Get(backendTrailer); len(got) != 1 {
				t.Fatalf("trailer %s = %v, want the upstream trailer on the error path too", backendTrailer, got)
			}
			if got := h.totalDelivered() - before; got != 1 {
				t.Fatalf("request delivered %d times, want exactly 1", got)
			}
		})
	}
}

func TestDeadlinePropagates(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	const budget = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	resp, err := h.client.Echo(ctx, &echov1.EchoRequest{})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	got := time.Duration(h.byID(t, resp.GetBackendId()).deadlineNS.Load())
	if got <= 0 || got > budget {
		t.Fatalf("backend saw a remaining budget of %v, want it inside (0, %v]", got, budget)
	}
}

func TestClientCancellationReachesBackend(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{}, 1)
	observed := make(chan struct{}, 1)
	h := newHarness(t, 3, withEchoHook(func(ctx context.Context, _ *echov1.EchoRequest) (*echov1.EchoResponse, error) {
		entered <- struct{}{}
		<-ctx.Done()
		observed <- struct{}{}
		return nil, ctx.Err()
	}))

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := h.client.Echo(ctx, &echov1.EchoRequest{})
		errc <- err
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the call never reached a backend")
	}
	cancel()

	select {
	case <-observed:
	case <-time.After(10 * time.Second):
		t.Fatal("the backend's context was never cancelled: the client's cancellation did not propagate upstream")
	}
	if err := <-errc; status.Code(err) != codes.Canceled {
		t.Fatalf("client error = %v, want %s", err, codes.Canceled)
	}
}

func TestStickiness(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)
	ctx := testContext(t)

	const (
		keys   = 200
		repeat = 3
	)
	owners := make(map[string]string, keys)
	seen := make(map[string]int, len(h.backends))

	for i := range keys {
		key := fmt.Sprintf("session-%03d", i)
		for range repeat {
			resp, err := h.client.Echo(sessionCtx(ctx, key), &echov1.EchoRequest{SessionKey: key})
			if err != nil {
				t.Fatalf("Echo(%s): %v", key, err)
			}
			id := resp.GetBackendId()
			if prev, ok := owners[key]; ok && prev != id {
				t.Fatalf("key %s landed on %s and then on %s: affinity is not stable", key, prev, id)
			}
			owners[key] = id
			seen[id]++
		}
	}
	if len(owners) != keys {
		t.Fatalf("resolved %d keys, want %d", len(owners), keys)
	}
	if len(seen) != len(h.backends) {
		t.Fatalf("only %d of %d backends were used; the ring is not spreading keys", len(seen), len(h.backends))
	}
}

func TestFailoverAfterBackendStops(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)
	ctx := testContext(t)

	// Learn the real ownership first, so the test kills a backend that actually owns traffic
	// rather than assuming a hash outcome.
	const keys = 60
	owners := make(map[string]string, keys)
	for i := range keys {
		key := fmt.Sprintf("failover-%03d", i)
		resp, err := h.client.Echo(sessionCtx(ctx, key), &echov1.EchoRequest{})
		if err != nil {
			t.Fatalf("warmup Echo(%s): %v", key, err)
		}
		owners[key] = resp.GetBackendId()
	}

	victimID := owners["failover-000"]
	var affected []string
	for key, id := range owners {
		if id == victimID {
			affected = append(affected, key)
		}
	}
	if len(affected) == 0 {
		t.Fatal("no key was owned by the victim backend")
	}

	before := metricTotal(t, h.m, "lb_failovers_total")
	h.byID(t, victimID).gracefulStop()

	for _, key := range affected {
		resp, err := h.client.Echo(sessionCtx(ctx, key), &echov1.EchoRequest{})
		if err != nil {
			t.Fatalf("Echo(%s) after %s stopped: %v (failover did not cover the loss)", key, victimID, err)
		}
		if resp.GetBackendId() == victimID {
			t.Fatalf("Echo(%s) reports it was served by the stopped backend %s", key, victimID)
		}
	}

	if after := metricTotal(t, h.m, "lb_failovers_total"); after <= before {
		t.Fatalf("lb_failovers_total = %v, want more than %v", after, before)
	}
}

// TestNoRetryAfterResponseForwarded is the correctness test that matters most: an upstream that
// fails only after it has already produced a response message must be reported to the caller, never
// silently replayed onto another backend.
func TestNoRetryAfterResponseForwarded(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	stream, err := h.client.ServerStream(testContext(t), &echov1.EchoRequest{
		Payload:     []byte("once"),
		StreamCount: 1,
		FailCode:    uint32(codes.Unavailable),
	})
	if err != nil {
		t.Fatalf("ServerStream: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if string(resp.GetPayload()) != "once" {
		t.Fatalf("first message = %q, want %q", resp.GetPayload(), "once")
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("second Recv = %v, want %s surfaced to the caller", err, codes.Unavailable)
	}

	if got := h.totalDelivered(); got != 1 {
		t.Fatalf("the request was delivered %d times, want exactly 1: a retryable code after a forwarded message must not be retried", got)
	}
	if got := metricTotal(t, h.m, "lb_failovers_total"); got != 0 {
		t.Fatalf("lb_failovers_total = %v, want 0", got)
	}
}

// TestNoRetryAfterClientSentSecondMessage covers the other half of replay safety: once the client
// has sent a message the proxy did not buffer, the RPC cannot start over anywhere.
func TestNoRetryAfterClientSentSecondMessage(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)

	stream, err := h.client.BidiStream(testContext(t))
	if err != nil {
		t.Fatalf("BidiStream: %v", err)
	}
	if err := stream.Send(&echov1.EchoRequest{Payload: []byte("first")}); err != nil {
		t.Fatalf("Send first: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv first: %v", err)
	}
	// The second message asks the backend for Unavailable, a code the proxy would happily retry if
	// nothing had been forwarded yet.
	if err := stream.Send(&echov1.EchoRequest{FailCode: uint32(codes.Unavailable)}); err != nil {
		t.Fatalf("Send second: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv = %v, want %s", err, codes.Unavailable)
	}
	if got := h.totalDelivered(); got != 2 {
		t.Fatalf("backends saw %d messages, want 2: the stream must not have been replayed", got)
	}
	if got := metricTotal(t, h.m, "lb_failovers_total"); got != 0 {
		t.Fatalf("lb_failovers_total = %v, want 0", got)
	}
}

func TestRequestInfoIsPopulated(t *testing.T) {
	t.Parallel()

	var captured atomic.Pointer[middleware.RequestInfo]
	capture := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if info, ok := middleware.FromContext(ss.Context()); ok {
			captured.Store(info)
		}
		return err
	}
	h := newHarness(t, 3, withInterceptors(middleware.Context(sessionHeader), capture))

	resp, err := h.client.Echo(sessionCtx(testContext(t), "info-key"), &echov1.EchoRequest{})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	info := captured.Load()
	if info == nil {
		t.Fatal("no RequestInfo was captured")
	}
	if got := info.Attempts(); got != 1 {
		t.Fatalf("Attempts() = %d, want 1", got)
	}
	if got := info.Backend(); got != resp.GetBackendId() {
		t.Fatalf("Backend() = %q, want %q", got, resp.GetBackendId())
	}
	if got := info.ResolvedHashKey(); got != "info-key" {
		t.Fatalf("ResolvedHashKey() = %q, want %q", got, "info-key")
	}
}

func TestNoBackendsAvailable(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	m := metrics.New()
	p, err := pool.New(cfg, m)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	bal := balancer.New(p, cfg.Routing, m)
	t.Cleanup(bal.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(New(bal, cfg, m, nil).ServerOptions()...)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	_, err = echov1.NewEchoServiceClient(conn).Echo(testContext(t), &echov1.EchoRequest{})
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable || st.Message() != "no backends available" {
		t.Fatalf("Echo = %v, want %s %q", err, codes.Unavailable, "no backends available")
	}
}

func TestServerOptions(t *testing.T) {
	t.Parallel()
	if got := len(New(nil, config.Default(), nil, nil).ServerOptions()); got != 2 {
		t.Fatalf("ServerOptions() has %d options, want 2 (codec + unknown service handler)", got)
	}
}

func TestForwardMD(t *testing.T) {
	t.Parallel()

	base := func(pairs ...string) context.Context {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
		return peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 4444}})
	}

	tests := []struct {
		name    string
		ctx     context.Context
		absent  []string
		present map[string]string
	}{
		{
			name:    "hop by hop keys are dropped",
			ctx:     base("connection", "keep-alive", "te", "trailers", "trailer", "x", "transfer-encoding", "chunked", "upgrade", "h2c", "keep-alive", "timeout=5", "proxy-authorization", "basic zzz"),
			absent:  []string{"connection", "te", "trailer", "transfer-encoding", "upgrade", "keep-alive", "proxy-authorization"},
			present: map[string]string{forwardedForHeader: "10.1.2.3"},
		},
		{
			name:    "reserved pseudo headers are dropped",
			ctx:     base(":authority", "example.test", ":path", "/x", "authority", "keep-me"),
			absent:  []string{":authority", ":path"},
			present: map[string]string{"authority": "keep-me"},
		},
		{
			name:    "application metadata and request id survive",
			ctx:     base(middleware.RequestIDHeader, "abc123", sessionHeader, "s-1", tokenHeader, "t"),
			present: map[string]string{middleware.RequestIDHeader: "abc123", sessionHeader: "s-1", tokenHeader: "t"},
		},
		{
			name:    "existing forwarded-for chain is extended",
			ctx:     base(forwardedForHeader, "203.0.113.7"),
			present: map[string]string{forwardedForHeader: "203.0.113.7"},
		},
		{
			name:    "no incoming metadata still records the peer",
			ctx:     peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 4444}}),
			present: map[string]string{forwardedForHeader: "10.1.2.3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := forwardMD(tc.ctx)
			for _, k := range tc.absent {
				if got := md.Get(k); len(got) != 0 {
					t.Fatalf("key %q survived with %v, want it stripped", k, got)
				}
			}
			for k, want := range tc.present {
				got := md.Get(k)
				if len(got) == 0 || got[0] != want {
					t.Fatalf("key %q = %v, want it to start with %q", k, got, want)
				}
			}
		})
	}

	// The chain must be appended to, never replaced.
	md := forwardMD(base(forwardedForHeader, "203.0.113.7"))
	if got := md.Get(forwardedForHeader); len(got) != 2 || got[1] != "10.1.2.3" {
		t.Fatalf("%s = %v, want the peer appended to the existing chain", forwardedForHeader, got)
	}
}

func TestConcurrentRPCs(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 3)
	ctx := testContext(t)

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				key := fmt.Sprintf("conc-%d-%d", w, i)
				payload := bytes.Repeat([]byte{byte(w)}, 2<<10)
				resp, err := h.client.Echo(sessionCtx(ctx, key), &echov1.EchoRequest{Payload: payload})
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(resp.GetPayload(), payload) {
					errs <- fmt.Errorf("payload mismatch for %s", key)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Echo: %v", err)
	}
}

func BenchmarkProxyUnary(b *testing.B) {
	h := newHarness(b, 3)
	ctx := metadata.AppendToOutgoingContext(context.Background(), sessionHeader, "bench")
	req := &echov1.EchoRequest{Payload: bytes.Repeat([]byte("b"), 256)}

	if _, err := h.client.Echo(ctx, req); err != nil {
		b.Fatalf("warmup: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := h.client.Echo(ctx, req); err != nil {
			b.Fatalf("Echo: %v", err)
		}
	}
}

// A backend that sets response headers and then fails must still have those headers reach the
// client. They are the caller's only view of application metadata attached to a failed RPC, and
// forwarding them is what makes the proxy transparent on the error path too.
func TestHeaderForwardedWhenUpstreamFails(t *testing.T) {
	h := newHarness(t, 1)

	var header, trailer metadata.MD
	_, err := h.client.Echo(context.Background(),
		&echov1.EchoRequest{FailCode: uint32(codes.FailedPrecondition)},
		grpc.Header(&header), grpc.Trailer(&trailer))

	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("Echo() code = %v, want %v", got, codes.FailedPrecondition)
	}
	want := h.backends[0].id
	if got := header.Get(backendHeader); len(got) == 0 || got[0] != want {
		t.Errorf("response header %q = %v, want %q — headers set before the failure were dropped",
			backendHeader, got, want)
	}
	if got := trailer.Get(backendTrailer); len(got) == 0 || got[0] != want {
		t.Errorf("response trailer %q = %v, want %q", backendTrailer, got, want)
	}
}

// totalDelivered counts how many backends actually ran the handler for an RPC. Anything above one
// means the request executed more than once.
func totalDelivered(h *harness) int64 {
	var n int64
	for _, b := range h.backends {
		n += b.delivered.Load()
	}
	return n
}

// A backend that receives a request, acts on it, and only then fails must not have that request
// replayed elsewhere. A gRPC status cannot distinguish "never arrived" from "processed, then died",
// so under the default policy the failure is surfaced rather than duplicated. This is the whole
// point of retry_policy: at-most-once by default.
func TestDefaultPolicyDoesNotReplayAfterDelivery(t *testing.T) {
	h := newHarness(t, 3)

	_, err := h.client.Echo(context.Background(),
		&echov1.EchoRequest{FailCode: uint32(codes.Unavailable), SessionKey: "k"})

	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("Echo() code = %v, want %v", got, codes.Unavailable)
	}
	if got := totalDelivered(h); got != 1 {
		t.Errorf("request executed on %d backends, want exactly 1 — a post-delivery failure was replayed", got)
	}
}

// The opt-in policy trades that guarantee for availability, and must actually do so.
func TestUnavailablePolicyReplaysAfterDelivery(t *testing.T) {
	h := newHarness(t, 3, withConfig(func(c *config.Config) {
		c.Routing.RetryPolicy = config.RetryUnavailable
	}))

	_, err := h.client.Echo(context.Background(),
		&echov1.EchoRequest{FailCode: uint32(codes.Unavailable), SessionKey: "k"})

	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("Echo() code = %v, want %v", got, codes.Unavailable)
	}
	if got := totalDelivered(h); got < 2 {
		t.Errorf("request executed on %d backends, want it replayed across candidates under %q",
			got, config.RetryUnavailable)
	}
}

// retry_policy "none" declines even the safe connect-failure replay.
func TestNonePolicyNeverReplays(t *testing.T) {
	h := newHarness(t, 3, withConfig(func(c *config.Config) {
		c.Routing.RetryPolicy = config.RetryNone
	}))
	h.backends[0].stop()

	// Drive several keys so at least one hashes to the stopped backend.
	var sawUnavailable bool
	for i := range 50 {
		_, err := h.client.Echo(context.Background(),
			&echov1.EchoRequest{SessionKey: fmt.Sprintf("key-%d", i)})
		if status.Code(err) == codes.Unavailable {
			sawUnavailable = true
			break
		}
	}
	if !sawUnavailable {
		t.Error("no request failed with Unavailable; a dead backend was still retried away under \"none\"")
	}
}

// A backend that answers with a real application status and then drops the stream must not have
// that status replayed across the fleet: the server saw the request, so the answer is final.
func TestSetupPathDoesNotReplayApplicationStatus(t *testing.T) {
	h := newHarness(t, 3)

	_, err := h.client.Echo(context.Background(),
		&echov1.EchoRequest{FailCode: uint32(codes.PermissionDenied), SessionKey: "k"})

	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("Echo() code = %v, want %v", got, codes.PermissionDenied)
	}
	if got := totalDelivered(h); got != 1 {
		t.Errorf("request executed on %d backends, want exactly 1 — an application status was replayed", got)
	}
}
