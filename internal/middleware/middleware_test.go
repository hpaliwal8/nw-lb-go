package middleware

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const testMethod = "/echo.v1.EchoService/Echo"

// fakeStream is the minimal grpc.ServerStream an interceptor touches: it only ever calls
// Context. The embedded nil interface makes any other call panic loudly instead of silently
// succeeding, which is what a test wants.
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeStream) Context() context.Context { return f.ctx }

func newStream(ctx context.Context) *fakeStream { return &fakeStream{ctx: ctx} }

// countingStream records SendMsg calls so a test can prove a wrapper forwards embedded methods.
type countingStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent int
}

func (c *countingStream) Context() context.Context { return c.ctx }

func (c *countingStream) SendMsg(any) error {
	c.sent++
	return nil
}

// fakeTransportStream satisfies grpc.ServerTransportStream so grpc.SetHeader has somewhere to
// write. Without one in the context SetHeader only returns an error.
type fakeTransportStream struct {
	mu     sync.Mutex
	method string
	header metadata.MD
}

func (f *fakeTransportStream) Method() string { return f.method }

func (f *fakeTransportStream) SetHeader(md metadata.MD) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.header == nil {
		f.header = metadata.MD{}
	}
	for k, vs := range md {
		f.header[k] = append(f.header[k], vs...)
	}
	return nil
}

func (f *fakeTransportStream) SendHeader(metadata.MD) error { return nil }

func (f *fakeTransportStream) SetTrailer(metadata.MD) error { return nil }

func (f *fakeTransportStream) get(key string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.header.Get(key)
}

func streamInfo(method string) *grpc.StreamServerInfo {
	return &grpc.StreamServerInfo{FullMethod: method, IsServerStream: true}
}

// okHandler records that it ran and returns err.
func handlerReturning(err error, calls *int) grpc.StreamHandler {
	return func(any, grpc.ServerStream) error {
		if calls != nil {
			*calls++
		}
		return err
	}
}

func withPeer(ctx context.Context, addr net.Addr) context.Context {
	return peer.NewContext(ctx, &peer.Peer{Addr: addr})
}

func tcpAddr(ip string, port int) net.Addr {
	return &net.TCPAddr{IP: net.ParseIP(ip), Port: port}
}

// scrape renders the private registry through the real exposition handler, so tests read the same
// bytes Prometheus would.
func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler: status %d", rec.Code)
	}
	return rec.Body.String()
}

// sampleValue returns the value of one exposition series, or 0 when it is absent — an untouched
// counter has no series at all, and "absent" and "zero" mean the same thing to these tests.
func sampleValue(t *testing.T, m *metrics.Metrics, series string) float64 {
	t.Helper()
	for line := range strings.SplitSeq(scrape(t, m), "\n") {
		name, raw, ok := strings.Cut(line, " ")
		if !ok || name != series {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			t.Fatalf("series %q: unparsable value %q: %v", series, raw, err)
		}
		return v
	}
	return 0
}

func TestRequestInfoAccessors(t *testing.T) {
	t.Run("zero value reads empty", func(t *testing.T) {
		var ri RequestInfo
		if got := ri.Backend(); got != "" {
			t.Errorf("Backend() = %q, want empty", got)
		}
		if got := ri.Attempts(); got != 0 {
			t.Errorf("Attempts() = %d, want 0", got)
		}
		if got := ri.ResolvedHashKey(); got != "" {
			t.Errorf("ResolvedHashKey() = %q, want empty", got)
		}
	})

	t.Run("nil receiver is inert", func(t *testing.T) {
		var ri *RequestInfo
		ri.SetBackend("b1")
		ri.AddAttempt()
		ri.SetHashKey("k")
		if ri.Backend() != "" || ri.Attempts() != 0 || ri.ResolvedHashKey() != "" || ri.id() != "" {
			t.Fatal("nil RequestInfo returned a value")
		}
	})

	t.Run("mutations are observable", func(t *testing.T) {
		ri := &RequestInfo{ID: "abc", HashKey: "from-context"}
		if got := ri.ResolvedHashKey(); got != "from-context" {
			t.Errorf("ResolvedHashKey() = %q, want the published field", got)
		}
		ri.SetBackend("b1")
		ri.SetBackend("b2")
		ri.AddAttempt()
		ri.AddAttempt()
		ri.SetHashKey("override")

		if got := ri.Backend(); got != "b2" {
			t.Errorf("Backend() = %q, want b2", got)
		}
		if got := ri.Attempts(); got != 2 {
			t.Errorf("Attempts() = %d, want 2", got)
		}
		if got := ri.ResolvedHashKey(); got != "override" {
			t.Errorf("ResolvedHashKey() = %q, want override", got)
		}
		if ri.HashKey != "from-context" {
			t.Errorf("HashKey field = %q, want it left immutable", ri.HashKey)
		}
	})
}

func TestContextRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() context.Context
		want bool
	}{
		{"absent", func() context.Context { return context.Background() }, false},
		{"nil context", func() context.Context { return nil }, false},
		{"nil info", func() context.Context { return NewContext(context.Background(), nil) }, false},
		{"present", func() context.Context {
			return NewContext(context.Background(), &RequestInfo{ID: "x"})
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FromContext(tc.ctx())
			if ok != tc.want {
				t.Fatalf("FromContext ok = %v, want %v", ok, tc.want)
			}
			if ok && got == nil {
				t.Fatal("FromContext reported ok with a nil RequestInfo")
			}
		})
	}
}

func TestWrapServerStream(t *testing.T) {
	base := &countingStream{ctx: context.Background()}
	ctx1 := context.WithValue(context.Background(), ctxKey{}, &RequestInfo{ID: "one"})
	ctx2 := context.WithValue(context.Background(), ctxKey{}, &RequestInfo{ID: "two"})

	w1 := WrapServerStream(base, ctx1)
	if w1.Context() != ctx1 {
		t.Fatal("wrapper did not report the new context")
	}
	if err := w1.SendMsg(nil); err != nil || base.sent != 1 {
		t.Fatalf("SendMsg not forwarded to the embedded stream: err=%v sent=%d", err, base.sent)
	}

	w2 := WrapServerStream(w1, ctx2)
	if w2.Context() != ctx2 {
		t.Fatal("re-wrap did not report the newest context")
	}
	inner, ok := w2.(*wrappedStream)
	if !ok {
		t.Fatalf("re-wrap returned %T, want *wrappedStream", w2)
	}
	if inner.ServerStream != grpc.ServerStream(base) {
		t.Fatal("re-wrap nested a wrapper instead of replacing it")
	}

	if got := WrapServerStream(base, nil); got != grpc.ServerStream(base) {
		t.Fatal("a nil context should leave the stream untouched")
	}
}

func TestChainOrder(t *testing.T) {
	var trace []string
	mark := func(name string) grpc.StreamServerInterceptor {
		return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			trace = append(trace, "enter:"+name)
			err := handler(srv, ss)
			trace = append(trace, "exit:"+name)
			return err
		}
	}

	tests := []struct {
		name  string
		build func() grpc.StreamServerInterceptor
		want  []string
	}{
		{
			name:  "three in order",
			build: func() grpc.StreamServerInterceptor { return Chain(mark("a"), mark("b"), mark("c")) },
			want:  []string{"enter:a", "enter:b", "enter:c", "handler", "exit:c", "exit:b", "exit:a"},
		},
		{
			name:  "empty is pass-through",
			build: func() grpc.StreamServerInterceptor { return Chain() },
			want:  []string{"handler"},
		},
		{
			name:  "single is unwrapped",
			build: func() grpc.StreamServerInterceptor { return Chain(mark("only")) },
			want:  []string{"enter:only", "handler", "exit:only"},
		},
		{
			name:  "nil entries are dropped",
			build: func() grpc.StreamServerInterceptor { return Chain(nil, mark("a"), nil, mark("b")) },
			want:  []string{"enter:a", "enter:b", "handler", "exit:b", "exit:a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trace = nil
			interceptor := tc.build()
			err := interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
				func(any, grpc.ServerStream) error {
					trace = append(trace, "handler")
					return nil
				})
			if err != nil {
				t.Fatalf("interceptor returned %v", err)
			}
			if strings.Join(trace, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("trace = %v, want %v", trace, tc.want)
			}
		})
	}
}

func TestChainSingleAvoidsWrapping(t *testing.T) {
	// Chain(x) must hand back x itself: an extra closure frame here runs on every single request.
	var one grpc.StreamServerInterceptor = passThrough
	got := Chain(one)
	if reflect.ValueOf(got).Pointer() != reflect.ValueOf(one).Pointer() {
		t.Fatal("Chain with one interceptor wrapped it instead of returning it")
	}
	if reflect.ValueOf(Chain()).Pointer() != reflect.ValueOf(passThrough).Pointer() {
		t.Fatal("Chain with no interceptors is not the pass-through")
	}
}

func TestClientKey(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{
			name: "metadata wins",
			ctx: withPeer(metadata.NewIncomingContext(context.Background(),
				metadata.Pairs(ClientIDHeader, "tenant-7")), tcpAddr("10.0.0.7", 4321)),
			want: "tenant-7",
		},
		{
			name: "empty metadata value falls through to peer",
			ctx: withPeer(metadata.NewIncomingContext(context.Background(),
				metadata.Pairs(ClientIDHeader, "")), tcpAddr("10.0.0.7", 4321)),
			want: "10.0.0.7",
		},
		{
			name: "peer only",
			ctx:  withPeer(context.Background(), tcpAddr("192.168.1.5", 9)),
			want: "192.168.1.5",
		},
		{
			name: "nothing at all",
			ctx:  context.Background(),
			want: "unknown",
		},
		{
			name: "unix socket peer",
			ctx:  withPeer(context.Background(), &net.UnixAddr{Name: "/tmp/lb.sock", Net: "unix"}),
			want: "/tmp/lb.sock",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClientKey(tc.ctx); got != tc.want {
				t.Fatalf("ClientKey = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHashKey(t *testing.T) {
	tests := []struct {
		name   string
		header string
		ctx    context.Context
		want   string
	}{
		{
			name:   "metadata value",
			header: "x-session-id",
			ctx: withPeer(metadata.NewIncomingContext(context.Background(),
				metadata.Pairs("x-session-id", "sess-42")), tcpAddr("10.0.0.7", 4321)),
			want: "sess-42",
		},
		{
			name:   "header lookup is case-insensitive",
			header: "X-Session-ID",
			ctx: metadata.NewIncomingContext(context.Background(),
				metadata.Pairs("x-session-id", "sess-42")),
			want: "sess-42",
		},
		{
			name:   "falls back to peer ip without the port",
			header: "x-session-id",
			ctx:    withPeer(context.Background(), tcpAddr("10.1.2.3", 51234)),
			want:   "10.1.2.3",
		},
		{
			name:   "ipv6 peer",
			header: "x-session-id",
			ctx:    withPeer(context.Background(), tcpAddr("2001:db8::1", 443)),
			want:   "2001:db8::1",
		},
		{
			name:   "empty header name skips metadata",
			header: "",
			ctx: withPeer(metadata.NewIncomingContext(context.Background(),
				metadata.Pairs("x-session-id", "sess-42")), tcpAddr("10.1.2.3", 51234)),
			want: "10.1.2.3",
		},
		{
			name:   "no affinity available",
			header: "x-session-id",
			ctx:    context.Background(),
			want:   "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := HashKey(tc.ctx, tc.header); got != tc.want {
				t.Fatalf("HashKey = %q, want %q", got, tc.want)
			}
		})
	}
}
