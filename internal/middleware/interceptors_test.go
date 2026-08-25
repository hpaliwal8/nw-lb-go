package middleware

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func bufferLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func decodeRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// fakeClock drives circuit.Settings.Now so breaker timing is exact instead of slept for.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestRecovery(t *testing.T) {
	tests := []struct {
		name     string
		handler  grpc.StreamHandler
		wantCode codes.Code
		wantMsg  string
		wantPanc float64
	}{
		{
			name:     "panic becomes internal",
			handler:  func(any, grpc.ServerStream) error { panic("boom") },
			wantCode: codes.Internal,
			wantMsg:  "internal error",
			wantPanc: 1,
		},
		{
			name: "runtime panic becomes internal",
			handler: func(any, grpc.ServerStream) error {
				var p *RequestInfo
				_ = p.StartedAt
				return nil
			},
			wantCode: codes.Internal,
			wantMsg:  "internal error",
			wantPanc: 1,
		},
		{
			name:     "error passes through untouched",
			handler:  func(any, grpc.ServerStream) error { return status.Error(codes.NotFound, "nope") },
			wantCode: codes.NotFound,
			wantMsg:  "nope",
			wantPanc: 0,
		},
		{
			name:     "success passes through",
			handler:  func(any, grpc.ServerStream) error { return nil },
			wantCode: codes.OK,
			wantPanc: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := metrics.New()
			buf := &bytes.Buffer{}
			interceptor := Recovery(bufferLogger(buf), m)

			err := interceptor(nil, newStream(context.Background()), streamInfo(testMethod), tc.handler)

			st := status.Convert(err)
			if st.Code() != tc.wantCode {
				t.Fatalf("code = %s, want %s", st.Code(), tc.wantCode)
			}
			if tc.wantMsg != "" && st.Message() != tc.wantMsg {
				t.Fatalf("message = %q, want %q", st.Message(), tc.wantMsg)
			}
			got := sampleValue(t, m, `lb_panics_total{method="`+testMethod+`"}`)
			if got != tc.wantPanc {
				t.Fatalf("lb_panics_total = %v, want %v", got, tc.wantPanc)
			}
			if tc.wantPanc > 0 {
				recs := decodeRecords(t, buf)
				if len(recs) != 1 {
					t.Fatalf("logged %d records, want 1", len(recs))
				}
				if recs[0]["level"] != "ERROR" {
					t.Errorf("level = %v, want ERROR", recs[0]["level"])
				}
				if stack, _ := recs[0]["stack"].(string); !strings.Contains(stack, "middleware") {
					t.Errorf("stack attr does not name this package: %q", stack)
				}
			}
		})
	}
}

func TestRecoveryNilDependencies(t *testing.T) {
	prev := slog.Default()
	slog.SetDefault(discardLogger())
	t.Cleanup(func() { slog.SetDefault(prev) })

	interceptor := Recovery(nil, nil)
	err := interceptor(nil, newStream(context.Background()), nil,
		func(any, grpc.ServerStream) error { panic("boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %s, want Internal", status.Code(err))
	}
}

func TestContextInterceptor(t *testing.T) {
	const validID = "0123456789abcdef"

	tests := []struct {
		name       string
		md         metadata.MD
		hashHeader string
		wantID     string // "" means "expect a freshly generated id"
		wantHash   string
	}{
		{
			name:       "generates an id when none is supplied",
			md:         metadata.MD{},
			hashHeader: "x-session-id",
			wantHash:   "10.0.0.7",
		},
		{
			name:       "reuses a sane incoming id",
			md:         metadata.Pairs(RequestIDHeader, validID),
			hashHeader: "x-session-id",
			wantID:     validID,
			wantHash:   "10.0.0.7",
		},
		{
			name:       "reuses a non-hex but printable id",
			md:         metadata.Pairs(RequestIDHeader, "trace/abc-123"),
			hashHeader: "x-session-id",
			wantID:     "trace/abc-123",
			wantHash:   "10.0.0.7",
		},
		{
			name:       "rejects an empty incoming id",
			md:         metadata.Pairs(RequestIDHeader, ""),
			hashHeader: "x-session-id",
			wantHash:   "10.0.0.7",
		},
		{
			name:       "rejects an oversized incoming id",
			md:         metadata.Pairs(RequestIDHeader, strings.Repeat("a", maxRequestIDLen+1)),
			hashHeader: "x-session-id",
			wantHash:   "10.0.0.7",
		},
		{
			name:       "rejects a non-printable incoming id",
			md:         metadata.Pairs(RequestIDHeader, "abc\ndef"),
			hashHeader: "x-session-id",
			wantHash:   "10.0.0.7",
		},
		{
			name:       "resolves the hash key from metadata",
			md:         metadata.Pairs("x-session-id", "sess-9"),
			hashHeader: "x-session-id",
			wantHash:   "sess-9",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := &fakeTransportStream{method: testMethod}
			ctx := grpc.NewContextWithServerTransportStream(context.Background(), transport)
			ctx = metadata.NewIncomingContext(ctx, tc.md)
			ctx = withPeer(ctx, tcpAddr("10.0.0.7", 44444))

			var got *RequestInfo
			err := Context(tc.hashHeader)(nil, newStream(ctx), streamInfo(testMethod),
				func(_ any, ss grpc.ServerStream) error {
					info, ok := FromContext(ss.Context())
					if !ok {
						t.Fatal("handler saw no RequestInfo")
					}
					got = info
					return nil
				})
			if err != nil {
				t.Fatalf("interceptor returned %v", err)
			}

			if got.Method != testMethod {
				t.Errorf("Method = %q, want %q", got.Method, testMethod)
			}
			if got.StartedAt.IsZero() {
				t.Error("StartedAt was never set")
			}
			if got.HashKey != tc.wantHash {
				t.Errorf("HashKey = %q, want %q", got.HashKey, tc.wantHash)
			}

			switch tc.wantID {
			case "":
				if len(got.ID) != 2*requestIDBytes {
					t.Fatalf("generated id %q has length %d, want %d", got.ID, len(got.ID), 2*requestIDBytes)
				}
				if _, err := hex.DecodeString(got.ID); err != nil {
					t.Fatalf("generated id %q is not hex: %v", got.ID, err)
				}
			default:
				if got.ID != tc.wantID {
					t.Fatalf("ID = %q, want %q", got.ID, tc.wantID)
				}
			}

			if hdr := transport.get(RequestIDHeader); len(hdr) != 1 || hdr[0] != got.ID {
				t.Fatalf("response header %s = %v, want [%s]", RequestIDHeader, hdr, got.ID)
			}
		})
	}
}

func TestContextInterceptorGeneratesDistinctIDs(t *testing.T) {
	seen := make(map[string]struct{}, 256)
	interceptor := Context("x-session-id")
	for range 256 {
		err := interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
			func(_ any, ss grpc.ServerStream) error {
				info, _ := FromContext(ss.Context())
				if _, dup := seen[info.ID]; dup {
					t.Fatalf("duplicate request id %q", info.ID)
				}
				seen[info.ID] = struct{}{}
				return nil
			})
		if err != nil {
			t.Fatalf("interceptor returned %v", err)
		}
	}
}

func TestContextInterceptorWithoutTransportStream(t *testing.T) {
	// grpc.SetHeader fails without a transport stream in the context; that must not fail the RPC.
	var ran bool
	err := Context("x-session-id")(nil, newStream(context.Background()), streamInfo(testMethod),
		func(any, grpc.ServerStream) error {
			ran = true
			return nil
		})
	if err != nil || !ran {
		t.Fatalf("interceptor returned %v, handler ran = %v", err, ran)
	}
}

func TestValidRequestID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{"hex", "0123456789abcdef", true},
		{"printable punctuation", "trace-1/seg:2", true},
		{"max length", strings.Repeat("a", maxRequestIDLen), true},
		{"empty", "", false},
		{"too long", strings.Repeat("a", maxRequestIDLen+1), false},
		{"newline", "abc\ndef", false},
		{"tab", "abc\tdef", false},
		{"non-ascii", "abcé", false},
		{"del", "abc\x7f", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := validRequestID(tc.id); got != tc.want {
				t.Fatalf("validRequestID(%q) = %v, want %v", tc.id, got, tc.want)
			}
		})
	}
}

func TestLoggingLevels(t *testing.T) {
	tests := []struct {
		code  codes.Code
		level string
	}{
		{codes.OK, "INFO"},
		{codes.Canceled, "INFO"},
		{codes.InvalidArgument, "WARN"},
		{codes.NotFound, "WARN"},
		{codes.AlreadyExists, "WARN"},
		{codes.PermissionDenied, "WARN"},
		{codes.Unauthenticated, "WARN"},
		{codes.FailedPrecondition, "WARN"},
		{codes.OutOfRange, "WARN"},
		{codes.Unknown, "ERROR"},
		{codes.DeadlineExceeded, "ERROR"},
		{codes.ResourceExhausted, "ERROR"},
		{codes.Aborted, "ERROR"},
		{codes.Unimplemented, "ERROR"},
		{codes.Internal, "ERROR"},
		{codes.Unavailable, "ERROR"},
		{codes.DataLoss, "ERROR"},
	}

	for _, tc := range tests {
		t.Run(tc.code.String(), func(t *testing.T) {
			buf := &bytes.Buffer{}
			var handlerErr error
			if tc.code != codes.OK {
				handlerErr = status.Error(tc.code, "failed")
			}
			err := Logging(bufferLogger(buf), config.Logging{SampleRate: 1})(
				nil, newStream(context.Background()), streamInfo(testMethod), handlerReturning(handlerErr, nil))
			if status.Code(err) != tc.code {
				t.Fatalf("interceptor changed the code to %s", status.Code(err))
			}

			recs := decodeRecords(t, buf)
			if len(recs) != 1 {
				t.Fatalf("logged %d records, want 1", len(recs))
			}
			if recs[0]["level"] != tc.level {
				t.Fatalf("level = %v, want %v", recs[0]["level"], tc.level)
			}
			if recs[0]["code"] != tc.code.String() {
				t.Fatalf("code attr = %v, want %v", recs[0]["code"], tc.code)
			}
		})
	}
}

func TestLoggingFields(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := withPeer(context.Background(), tcpAddr("10.9.8.7", 5555))
	ctx = NewContext(ctx, &RequestInfo{
		ID:        "req-1",
		Method:    testMethod,
		StartedAt: time.Now().Add(-25 * time.Millisecond),
		HashKey:   "sess-1",
	})

	err := Logging(bufferLogger(buf), config.Logging{SampleRate: 1})(
		nil, newStream(ctx), streamInfo(testMethod),
		func(_ any, ss grpc.ServerStream) error {
			info, _ := FromContext(ss.Context())
			info.SetBackend("backend-b")
			info.AddAttempt()
			info.AddAttempt()
			info.SetHashKey("sess-override")
			return nil
		})
	if err != nil {
		t.Fatalf("interceptor returned %v", err)
	}

	recs := decodeRecords(t, buf)
	if len(recs) != 1 {
		t.Fatalf("logged %d records, want 1", len(recs))
	}
	rec := recs[0]

	strFields := map[string]string{
		"request_id": "req-1",
		"method":     testMethod,
		"code":       "OK",
		"backend":    "backend-b",
		"hash_key":   "sess-override",
		"peer":       "10.9.8.7:5555",
	}
	for k, want := range strFields {
		if got, _ := rec[k].(string); got != want {
			t.Errorf("%s = %v, want %q", k, rec[k], want)
		}
	}
	if got, _ := rec["attempts"].(float64); got != 2 {
		t.Errorf("attempts = %v, want 2", rec["attempts"])
	}
	ms, ok := rec["duration_ms"].(float64)
	if !ok {
		t.Fatalf("duration_ms = %v, want a number", rec["duration_ms"])
	}
	if ms < 25 {
		t.Errorf("duration_ms = %v, want it measured from RequestInfo.StartedAt (>= 25)", ms)
	}
	if math.Abs(ms*100-math.Round(ms*100)) > 1e-6 {
		t.Errorf("duration_ms = %v, want at most 2 decimal places", ms)
	}
}

func TestLoggingFieldsWithoutRequestInfo(t *testing.T) {
	buf := &bytes.Buffer{}
	err := Logging(bufferLogger(buf), config.Logging{SampleRate: 1})(
		nil, newStream(context.Background()), streamInfo(testMethod),
		handlerReturning(errors.New("plain error"), nil))
	if status.Code(err) != codes.Unknown {
		t.Fatalf("code = %s, want Unknown", status.Code(err))
	}

	recs := decodeRecords(t, buf)
	if len(recs) != 1 {
		t.Fatalf("logged %d records, want 1", len(recs))
	}
	for _, k := range []string{"request_id", "backend", "hash_key", "peer"} {
		if got, _ := recs[0][k].(string); got != "" {
			t.Errorf("%s = %q, want empty", k, got)
		}
	}
	if got, _ := recs[0]["error"].(string); got != "plain error" {
		t.Errorf("error = %q, want %q", got, "plain error")
	}
}

func TestLoggingSampling(t *testing.T) {
	tests := []struct {
		name      string
		rate      float64
		draw      float64
		code      codes.Code
		wantRecs  int
		wantDrawn bool
	}{
		{name: "rate 1 logs every ok without drawing", rate: 1, code: codes.OK, wantRecs: 1},
		{name: "rate 0 drops ok without drawing", rate: 0, code: codes.OK, wantRecs: 0},
		{name: "rate 0 still logs errors", rate: 0, code: codes.Unavailable, wantRecs: 1},
		{name: "rate 1 logs errors", rate: 1, code: codes.Internal, wantRecs: 1},
		{name: "draw below rate logs", rate: 0.5, draw: 0.4, code: codes.OK, wantRecs: 1, wantDrawn: true},
		{name: "draw at rate drops", rate: 0.5, draw: 0.5, code: codes.OK, wantRecs: 0, wantDrawn: true},
		{name: "draw above rate drops", rate: 0.5, draw: 0.9, code: codes.OK, wantRecs: 0, wantDrawn: true},
		{name: "errors bypass the sampler", rate: 0.5, draw: 0.9, code: codes.Unavailable, wantRecs: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			draws := 0
			sample := func() float64 {
				draws++
				return tc.draw
			}
			var handlerErr error
			if tc.code != codes.OK {
				handlerErr = status.Error(tc.code, "failed")
			}

			interceptor := logging(bufferLogger(buf), config.Logging{SampleRate: tc.rate}, sample)
			_ = interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
				handlerReturning(handlerErr, nil))

			if got := len(decodeRecords(t, buf)); got != tc.wantRecs {
				t.Fatalf("logged %d records, want %d", got, tc.wantRecs)
			}
			if drawn := draws > 0; drawn != tc.wantDrawn {
				t.Fatalf("sampler drawn = %v (%d draws), want %v", drawn, draws, tc.wantDrawn)
			}
		})
	}
}

func TestLoggingNilLogger(t *testing.T) {
	prev := slog.Default()
	slog.SetDefault(discardLogger())
	t.Cleanup(func() { slog.SetDefault(prev) })

	var calls int
	if err := Logging(nil, config.Logging{SampleRate: 1})(
		nil, newStream(context.Background()), streamInfo(testMethod), handlerReturning(nil, &calls)); err != nil {
		t.Fatalf("interceptor returned %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1", calls)
	}
}

func TestMillis(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want float64
	}{
		{0, 0},
		{time.Microsecond, 0},
		{5 * time.Microsecond, 0.01},
		{time.Millisecond, 1},
		{1500 * time.Microsecond, 1.5},
		{1234 * time.Microsecond, 1.23},
		{1236 * time.Microsecond, 1.24},
		{2 * time.Second, 2000},
	}
	for _, tc := range tests {
		t.Run(tc.d.String(), func(t *testing.T) {
			if got := millis(tc.d); math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("millis(%s) = %v, want %v", tc.d, got, tc.want)
			}
		})
	}
}

func TestMetricsInterceptor(t *testing.T) {
	m := metrics.New()
	interceptor := Metrics(m)

	inflightDuringCall := -1.0
	err := interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
		func(any, grpc.ServerStream) error {
			inflightDuringCall = sampleValue(t, m, `lb_requests_inflight{method="`+testMethod+`"}`)
			return nil
		})
	if err != nil {
		t.Fatalf("interceptor returned %v", err)
	}
	if inflightDuringCall != 1 {
		t.Errorf("inflight during the call = %v, want 1", inflightDuringCall)
	}

	_ = interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
		handlerReturning(status.Error(codes.Unavailable, "down"), nil))

	checks := []struct {
		series string
		want   float64
	}{
		{`lb_requests_total{code="OK",method="` + testMethod + `"}`, 1},
		{`lb_requests_total{code="Unavailable",method="` + testMethod + `"}`, 1},
		{`lb_request_duration_seconds_count{code="OK",method="` + testMethod + `"}`, 1},
		{`lb_request_duration_seconds_count{code="Unavailable",method="` + testMethod + `"}`, 1},
		{`lb_requests_inflight{method="` + testMethod + `"}`, 0},
	}
	for _, c := range checks {
		if got := sampleValue(t, m, c.series); got != c.want {
			t.Errorf("%s = %v, want %v", c.series, got, c.want)
		}
	}
}

func TestMetricsInterceptorUsesFullMethodAndSurvivesMissingInfo(t *testing.T) {
	m := metrics.New()
	if err := Metrics(m)(nil, newStream(context.Background()), nil, handlerReturning(nil, nil)); err != nil {
		t.Fatalf("interceptor returned %v", err)
	}
	if got := sampleValue(t, m, `lb_requests_total{code="OK",method="unknown"}`); got != 1 {
		t.Fatalf("lb_requests_total for the unknown method = %v, want 1", got)
	}

	var calls int
	if err := Metrics(nil)(nil, newStream(context.Background()), nil, handlerReturning(nil, &calls)); err != nil {
		t.Fatalf("nil metrics interceptor returned %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times with nil metrics, want 1", calls)
	}
}

func TestRateLimit(t *testing.T) {
	m := metrics.New()
	// A rate this low refills nothing over the life of the test, so the burst is the whole budget
	// and the outcome does not depend on how fast the machine runs.
	l := ratelimit.New(ratelimit.Config{Enabled: true, RPS: 0.0001, Burst: 2})
	t.Cleanup(l.Close)

	interceptor := RateLimit(l, m)
	ctx := withPeer(context.Background(), tcpAddr("10.0.0.1", 1000))

	var calls int
	for i := range 3 {
		err := interceptor(nil, newStream(ctx), streamInfo(testMethod), handlerReturning(nil, &calls))
		switch {
		case i < 2 && err != nil:
			t.Fatalf("request %d rejected inside the burst: %v", i, err)
		case i == 2:
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("request %d code = %s, want ResourceExhausted", i, status.Code(err))
			}
			if msg := status.Convert(err).Message(); msg != "rate limit exceeded" {
				t.Fatalf("message = %q, want %q", msg, "rate limit exceeded")
			}
		}
	}
	if calls != 2 {
		t.Fatalf("handler ran %d times, want 2 — a denied request must not reach it", calls)
	}
	if got := sampleValue(t, m, `lb_rate_limited_total{method="`+testMethod+`"}`); got != 1 {
		t.Fatalf("lb_rate_limited_total = %v, want 1", got)
	}
}

func TestRateLimitPerClientKey(t *testing.T) {
	l := ratelimit.New(ratelimit.Config{
		Enabled:        true,
		RPS:            0.0001,
		Burst:          10,
		PerClient:      true,
		PerClientRPS:   0.0001,
		PerClientBurst: 1,
	})
	t.Cleanup(l.Close)
	interceptor := RateLimit(l, nil)

	clientCtx := func(id string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs(ClientIDHeader, id))
	}

	tests := []struct {
		name string
		ctx  context.Context
		want codes.Code
	}{
		{"first request from a", clientCtx("a"), codes.OK},
		{"second request from a is denied", clientCtx("a"), codes.ResourceExhausted},
		{"first request from b is independent", clientCtx("b"), codes.OK},
		{"second request from b is denied", clientCtx("b"), codes.ResourceExhausted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := interceptor(nil, newStream(tc.ctx), streamInfo(testMethod), handlerReturning(nil, nil))
			if status.Code(err) != tc.want {
				t.Fatalf("code = %s, want %s", status.Code(err), tc.want)
			}
		})
	}
}

func TestRateLimitNilLimiter(t *testing.T) {
	var calls int
	if err := RateLimit(nil, nil)(nil, newStream(context.Background()), streamInfo(testMethod),
		handlerReturning(nil, &calls)); err != nil {
		t.Fatalf("interceptor returned %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1", calls)
	}
}

func TestIsFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ok status", status.Error(codes.OK, ""), false},
		{"unavailable", status.Error(codes.Unavailable, "x"), true},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "x"), true},
		{"internal", status.Error(codes.Internal, "x"), true},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "x"), true},
		{"unknown", status.Error(codes.Unknown, "x"), true},
		{"plain error is unknown", errors.New("boom"), true},
		{"wrapped status keeps its code", status.Error(codes.InvalidArgument, "x"), false},
		{"invalid argument", status.Error(codes.InvalidArgument, "x"), false},
		{"not found", status.Error(codes.NotFound, "x"), false},
		{"already exists", status.Error(codes.AlreadyExists, "x"), false},
		{"permission denied", status.Error(codes.PermissionDenied, "x"), false},
		{"unauthenticated", status.Error(codes.Unauthenticated, "x"), false},
		{"failed precondition", status.Error(codes.FailedPrecondition, "x"), false},
		{"out of range", status.Error(codes.OutOfRange, "x"), false},
		{"aborted", status.Error(codes.Aborted, "x"), false},
		{"unimplemented", status.Error(codes.Unimplemented, "x"), false},
		{"data loss", status.Error(codes.DataLoss, "x"), false},
		{"canceled", status.Error(codes.Canceled, "x"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsFailure(tc.err); got != tc.want {
				t.Fatalf("IsFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestCircuitBreak(t *testing.T) {
	clock := newClock()
	m := metrics.New()
	reg := circuit.NewRegistry(circuit.Settings{
		Window:       time.Minute,
		Buckets:      6,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  5 * time.Second,
		HalfOpenMax:  1,
		Now:          clock.Now,
	})
	interceptor := CircuitBreak(reg, m)

	call := func(t *testing.T, handlerErr error) (error, bool) {
		t.Helper()
		ran := false
		err := interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
			func(any, grpc.ServerStream) error {
				ran = true
				return handlerErr
			})
		return err, ran
	}

	unavailable := status.Error(codes.Unavailable, "down")

	for i := range 2 {
		err, ran := call(t, unavailable)
		if !ran {
			t.Fatalf("failure %d never reached the handler", i)
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("failure %d code = %s, want Unavailable", i, status.Code(err))
		}
	}

	if got := reg.Get(testMethod).State(); got != circuit.StateOpen {
		t.Fatalf("breaker state = %s, want open", got)
	}

	err, ran := call(t, nil)
	if ran {
		t.Fatal("an open breaker still ran the handler")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("fail-fast code = %s, want Unavailable", status.Code(err))
	}
	if msg := status.Convert(err).Message(); msg != "circuit open" {
		t.Fatalf("fail-fast message = %q, want %q", msg, "circuit open")
	}
	if got := sampleValue(t, m, `lb_rejected_total{method="`+testMethod+`",reason="circuit_open"}`); got != 1 {
		t.Fatalf("lb_rejected_total = %v, want 1", got)
	}

	clock.Advance(5 * time.Second)
	if got := reg.Get(testMethod).State(); got != circuit.StateHalfOpen {
		t.Fatalf("after OpenTimeout state = %s, want half-open", got)
	}

	if _, ran := call(t, nil); !ran {
		t.Fatal("half-open breaker refused the probe")
	}
	if got := reg.Get(testMethod).State(); got != circuit.StateClosed {
		t.Fatalf("after a successful probe state = %s, want closed", got)
	}
}

func TestCircuitBreakIgnoresClientErrors(t *testing.T) {
	clock := newClock()
	reg := circuit.NewRegistry(circuit.Settings{
		Window:       time.Minute,
		Buckets:      6,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  5 * time.Second,
		HalfOpenMax:  1,
		Now:          clock.Now,
	})
	interceptor := CircuitBreak(reg, nil)

	for range 10 {
		var calls int
		err := interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
			handlerReturning(status.Error(codes.InvalidArgument, "bad"), &calls))
		if status.Code(err) != codes.InvalidArgument || calls != 1 {
			t.Fatalf("code = %s, handler calls = %d", status.Code(err), calls)
		}
	}
	if got := reg.Get(testMethod).State(); got != circuit.StateClosed {
		t.Fatalf("client errors tripped the breaker: state = %s", got)
	}
}

func TestCircuitBreakIsPerMethod(t *testing.T) {
	const other = "/echo.v1.EchoService/Stream"
	clock := newClock()
	reg := circuit.NewRegistry(circuit.Settings{
		Window:       time.Minute,
		Buckets:      6,
		MinRequests:  2,
		FailureRatio: 0.5,
		OpenTimeout:  5 * time.Second,
		HalfOpenMax:  1,
		Now:          clock.Now,
	})
	interceptor := CircuitBreak(reg, nil)

	for range 2 {
		_ = interceptor(nil, newStream(context.Background()), streamInfo(testMethod),
			handlerReturning(status.Error(codes.Internal, "boom"), nil))
	}

	var calls int
	if err := interceptor(nil, newStream(context.Background()), streamInfo(other),
		handlerReturning(nil, &calls)); err != nil {
		t.Fatalf("the healthy method was rejected: %v", err)
	}
	if calls != 1 {
		t.Fatalf("healthy method handler ran %d times, want 1", calls)
	}
}

func TestCircuitBreakNilRegistry(t *testing.T) {
	var calls int
	if err := CircuitBreak(nil, nil)(nil, newStream(context.Background()), streamInfo(testMethod),
		handlerReturning(nil, &calls)); err != nil {
		t.Fatalf("interceptor returned %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1", calls)
	}
}

// TestRequestInfoMutationDuringLogging is the -race guard for the one genuinely concurrent access
// in this package: the proxy mutates RequestInfo from its own goroutines while Logging reads the
// same value once the handler has returned. The mutator keeps running across that read on purpose.
func TestRequestInfoMutationDuringLogging(t *testing.T) {
	stop := make(chan struct{})
	var wg sync.WaitGroup

	interceptor := Chain(
		Context("x-session-id"),
		Logging(discardLogger(), config.Logging{SampleRate: 1}),
	)

	handler := func(_ any, ss grpc.ServerStream) error {
		info, ok := FromContext(ss.Context())
		if !ok {
			return errors.New("no RequestInfo")
		}
		for w := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					info.SetBackend("backend-" + string(rune('a'+w)))
					info.AddAttempt()
					info.SetHashKey("key")
					_ = info.Backend()
					_ = info.Attempts()
					_ = info.ResolvedHashKey()
				}
			}()
		}
		return nil
	}

	ctx := withPeer(context.Background(), tcpAddr("10.0.0.3", 7777))
	for range 50 {
		if err := interceptor(nil, newStream(ctx), streamInfo(testMethod), handler); err != nil {
			t.Fatalf("interceptor returned %v", err)
		}
	}

	close(stop)
	wg.Wait()
}
