package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	echov1 "github.com/hitanshpaliwal/nw-lb-go/gen/echo/v1"
)

// syncWriter collects log output written by the server goroutine.
type syncWriter struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func mapEnv(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestParseOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		want    options
		wantErr bool
	}{
		{
			name: "defaults",
			want: options{listen: defaultListen, adminListen: defaultAdminListen, id: defaultID(), health: "serving"},
		},
		{
			name: "env fallbacks",
			env: map[string]string{
				"BACKEND_LISTEN":     ":7000",
				"BACKEND_ID":         "backend-env",
				"BACKEND_DELAY":      "5ms",
				"BACKEND_FAIL_RATIO": "0.25",
			},
			want: options{listen: ":7000", adminListen: defaultAdminListen, id: "backend-env", delay: 5 * time.Millisecond, failRatio: 0.25, health: "serving"},
		},
		{
			name: "bare env delay is milliseconds",
			env:  map[string]string{"BACKEND_ID": "b", "BACKEND_DELAY": "7"},
			want: options{listen: defaultListen, adminListen: defaultAdminListen, id: "b", delay: 7 * time.Millisecond, health: "serving"},
		},
		{
			name: "flags override env",
			args: []string{"-listen", ":9", "-id", "flag-id", "-delay", "1s", "-fail-ratio", "0.5", "-health", "not_serving", "-admin-listen", ":8"},
			env:  map[string]string{"BACKEND_LISTEN": ":7000", "BACKEND_ID": "backend-env", "BACKEND_DELAY": "5ms", "BACKEND_FAIL_RATIO": "0.25"},
			want: options{listen: ":9", adminListen: ":8", id: "flag-id", delay: time.Second, failRatio: 0.5, health: "not_serving"},
		},
		{
			name: "empty env value falls back to default",
			env:  map[string]string{"BACKEND_LISTEN": "", "BACKEND_ID": "b", "BACKEND_DELAY": "", "BACKEND_FAIL_RATIO": ""},
			want: options{listen: defaultListen, adminListen: defaultAdminListen, id: "b", health: "serving"},
		},
		{
			name:    "fail ratio above one",
			args:    []string{"-fail-ratio", "1.5"},
			wantErr: true,
		},
		{
			name:    "negative fail ratio",
			args:    []string{"-fail-ratio", "-0.1"},
			wantErr: true,
		},
		{
			name:    "negative delay",
			args:    []string{"-delay", "-1s"},
			wantErr: true,
		},
		{
			name:    "unknown health value",
			args:    []string{"-health", "maybe"},
			wantErr: true,
		},
		{
			name:    "empty listen",
			args:    []string{"-listen", ""},
			wantErr: true,
		},
		{
			name:    "unparsable env delay",
			env:     map[string]string{"BACKEND_DELAY": "soon"},
			wantErr: true,
		},
		{
			name:    "unparsable env fail ratio",
			env:     map[string]string{"BACKEND_FAIL_RATIO": "half"},
			wantErr: true,
		},
		{
			name:    "positional argument",
			args:    []string{"serve"},
			wantErr: true,
		},
		{
			name:    "unknown flag",
			args:    []string{"-nope"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseOptions(tc.args, mapEnv(tc.env), io.Discard)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseOptions(%v) = %+v, want error", tc.args, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseOptions(%v): %v", tc.args, err)
			}
			if got != tc.want {
				t.Fatalf("parseOptions(%v) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

func TestOptionsServingStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		health string
		want   grpc_health_v1.HealthCheckResponse_ServingStatus
	}{
		{"serving", grpc_health_v1.HealthCheckResponse_SERVING},
		{"not_serving", grpc_health_v1.HealthCheckResponse_NOT_SERVING},
	}
	for _, tc := range tests {
		t.Run(tc.health, func(t *testing.T) {
			t.Parallel()
			if got := (options{health: tc.health}).servingStatus(); got != tc.want {
				t.Fatalf("servingStatus() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEchoInjectedFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		failRatio float64
		roll      float64
		failCode  uint32
		wantCode  codes.Code
	}{
		{name: "ok", roll: 0.99, wantCode: codes.OK},
		{name: "explicit fail code", failCode: uint32(codes.NotFound), roll: 0.99, wantCode: codes.NotFound},
		{name: "ratio trips", failRatio: 0.5, roll: 0.25, wantCode: codes.Unavailable},
		{name: "ratio spares", failRatio: 0.5, roll: 0.75, wantCode: codes.OK},
		{name: "zero ratio never trips", failRatio: 0, roll: 0, wantCode: codes.OK},
		{name: "full ratio always trips", failRatio: 1, roll: 0.999999, wantCode: codes.Unavailable},
		{name: "explicit code beats ratio", failRatio: 0.5, roll: 0.1, failCode: uint32(codes.InvalidArgument), wantCode: codes.InvalidArgument},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newServer(options{id: "backend-1", failRatio: tc.failRatio}, "10.0.0.1:50051", prometheus.NewRegistry())
			s.randFloat = func() float64 { return tc.roll }

			resp, err := s.Echo(context.Background(), &echov1.EchoRequest{Payload: []byte("hi"), FailCode: tc.failCode})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("Echo code = %v (err %v), want %v", got, err, tc.wantCode)
			}
			if tc.wantCode != codes.OK {
				if resp != nil {
					t.Fatalf("Echo returned a response alongside %v", err)
				}
				if msg := status.Convert(err).Message(); msg != "injected failure" {
					t.Fatalf("Echo message = %q, want %q", msg, "injected failure")
				}
				return
			}
			if string(resp.GetPayload()) != "hi" {
				t.Fatalf("payload = %q, want %q", resp.GetPayload(), "hi")
			}
			if resp.GetBackendId() != "backend-1" || resp.GetBackendAddr() != "10.0.0.1:50051" {
				t.Fatalf("identity = (%q, %q), want (backend-1, 10.0.0.1:50051)", resp.GetBackendId(), resp.GetBackendAddr())
			}
			if resp.GetServedUnixNano() == 0 {
				t.Fatal("served_unix_nano is unset")
			}
		})
	}
}

func TestPause(t *testing.T) {
	t.Parallel()

	t.Run("waits at least the requested delay", func(t *testing.T) {
		t.Parallel()
		s := newServer(options{id: "b", delay: 10 * time.Millisecond}, "addr", prometheus.NewRegistry())
		start := time.Now()
		if err := s.pause(context.Background(), 10); err != nil {
			t.Fatalf("pause: %v", err)
		}
		if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
			t.Fatalf("pause returned after %s, want at least 20ms", elapsed)
		}
	})

	t.Run("no delay does not touch the clock", func(t *testing.T) {
		t.Parallel()
		s := newServer(options{id: "b"}, "addr", prometheus.NewRegistry())
		if err := s.pause(context.Background(), 0); err != nil {
			t.Fatalf("pause: %v", err)
		}
	})

	t.Run("cancelled context wins", func(t *testing.T) {
		t.Parallel()
		s := newServer(options{id: "b", delay: time.Hour}, "addr", prometheus.NewRegistry())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := s.pause(ctx, 0)
		if got := status.Code(err); got != codes.Canceled {
			t.Fatalf("pause code = %v (err %v), want %v", got, err, codes.Canceled)
		}
	})
}

// startBackend runs a backend on ephemeral ports and returns its addresses. The server is
// shut down through the context during test cleanup, which also asserts a clean exit.
func startBackend(t *testing.T, env map[string]string, args ...string) (grpcAddr, adminAddr string, logs *syncWriter) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	logs = &syncWriter{}
	type addrs struct{ grpc, admin string }
	ready := make(chan addrs, 1)
	done := make(chan error, 1)

	full := append([]string{"-listen", "127.0.0.1:0", "-admin-listen", "127.0.0.1:0"}, args...)
	go func() {
		done <- run(ctx, full, logs, mapEnv(env), func(g, a string) { ready <- addrs{g, a} })
	}()

	var got addrs
	select {
	case got = <-ready:
	case err := <-done:
		cancel()
		t.Fatalf("run exited before becoming ready: %v (logs: %s)", err, logs)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("timed out waiting for the backend to start")
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned %v (logs: %s)", err, logs)
			}
		case <-time.After(15 * time.Second):
			t.Error("timed out waiting for the backend to shut down")
		}
	})

	return got.grpc, got.admin, logs
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestRunServesEchoHealthAndAdmin(t *testing.T) {
	grpcAddr, adminAddr, logs := startBackend(t, nil, "-id", "backend-test")
	conn := dial(t, grpcAddr)
	client := echov1.NewEchoServiceClient(conn)

	t.Run("unary echo", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		before := time.Now().UnixNano()
		resp, err := client.Echo(ctx, &echov1.EchoRequest{Payload: []byte("ping"), SessionKey: "k1", DelayMs: 1})
		if err != nil {
			t.Fatalf("Echo: %v", err)
		}
		if string(resp.GetPayload()) != "ping" {
			t.Fatalf("payload = %q, want %q", resp.GetPayload(), "ping")
		}
		if resp.GetBackendId() != "backend-test" {
			t.Fatalf("backend_id = %q, want backend-test", resp.GetBackendId())
		}
		if resp.GetBackendAddr() != grpcAddr {
			t.Fatalf("backend_addr = %q, want %q", resp.GetBackendAddr(), grpcAddr)
		}
		if resp.GetServedUnixNano() < before {
			t.Fatalf("served_unix_nano = %d, want >= %d", resp.GetServedUnixNano(), before)
		}
	})

	t.Run("unary fail code", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := client.Echo(ctx, &echov1.EchoRequest{FailCode: uint32(codes.ResourceExhausted)})
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("Echo code = %v (err %v), want %v", got, err, codes.ResourceExhausted)
		}
	})

	t.Run("server stream", func(t *testing.T) {
		tests := []struct {
			name  string
			count uint32
			want  int
		}{
			{name: "zero means one", count: 0, want: 1},
			{name: "explicit count", count: 3, want: 3},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				stream, err := client.ServerStream(ctx, &echov1.EchoRequest{Payload: []byte("s"), StreamCount: tc.count})
				if err != nil {
					t.Fatalf("ServerStream: %v", err)
				}
				var seqs []uint32
				for {
					resp, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("Recv: %v", err)
					}
					if resp.GetBackendId() != "backend-test" {
						t.Fatalf("backend_id = %q, want backend-test", resp.GetBackendId())
					}
					seqs = append(seqs, resp.GetSeq())
				}
				if len(seqs) != tc.want {
					t.Fatalf("received %d messages (seqs %v), want %d", len(seqs), seqs, tc.want)
				}
				for i, seq := range seqs {
					if seq != uint32(i) {
						t.Fatalf("seqs = %v, want ascending from 0", seqs)
					}
				}
			})
		}
	})

	t.Run("client stream reports byte count", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := client.ClientStream(ctx)
		if err != nil {
			t.Fatalf("ClientStream: %v", err)
		}
		payloads := [][]byte{[]byte("ab"), []byte("cde"), []byte("")}
		total := 0
		for _, p := range payloads {
			if err := stream.Send(&echov1.EchoRequest{Payload: p}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			total += len(p)
		}
		resp, err := stream.CloseAndRecv()
		if err != nil {
			t.Fatalf("CloseAndRecv: %v", err)
		}
		if got := string(resp.GetPayload()); got != strconv.Itoa(total) {
			t.Fatalf("payload = %q, want %q", got, strconv.Itoa(total))
		}
	})

	t.Run("bidi stream echoes every message", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := client.BidiStream(ctx)
		if err != nil {
			t.Fatalf("BidiStream: %v", err)
		}
		want := []string{"one", "two", "three"}
		for _, msg := range want {
			if err := stream.Send(&echov1.EchoRequest{Payload: []byte(msg)}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			resp, err := stream.Recv()
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if string(resp.GetPayload()) != msg {
				t.Fatalf("payload = %q, want %q", resp.GetPayload(), msg)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("Recv after CloseSend = %v, want io.EOF", err)
		}
	})

	t.Run("health reports serving", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		hc := grpc_health_v1.NewHealthClient(conn)
		for _, service := range []string{"", echov1.EchoService_ServiceDesc.ServiceName} {
			resp, err := hc.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: service})
			if err != nil {
				t.Fatalf("Check(%q): %v", service, err)
			}
			if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
				t.Fatalf("Check(%q) = %v, want SERVING", service, resp.GetStatus())
			}
		}
	})

	t.Run("admin endpoints", func(t *testing.T) {
		if body, code := httpGet(t, "http://"+adminAddr+"/healthz"); code != http.StatusOK || body != "ok" {
			t.Fatalf("/healthz = %d %q, want 200 \"ok\"", code, body)
		}
		body, code := httpGet(t, "http://"+adminAddr+"/metrics")
		if code != http.StatusOK {
			t.Fatalf("/metrics status = %d, want 200", code)
		}
		if !strings.Contains(body, "backend_requests_total") {
			t.Fatalf("/metrics does not expose backend_requests_total:\n%s", body)
		}
		if !strings.Contains(body, `backend_requests_total{code="OK",method="Echo"}`) {
			t.Fatalf("/metrics is missing the Echo/OK series:\n%s", body)
		}
	})

	if !strings.Contains(logs.String(), `"msg":"backend started"`) {
		t.Fatalf("startup was not logged: %s", logs)
	}
	if !strings.Contains(logs.String(), `"id":"backend-test"`) {
		t.Fatalf("startup log is missing the backend id: %s", logs)
	}
}

func TestRunHealthFlagNotServing(t *testing.T) {
	grpcAddr, _, _ := startBackend(t, nil, "-id", "backend-down", "-health", "not_serving")
	conn := dial(t, grpcAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Check = %v, want NOT_SERVING", resp.GetStatus())
	}
}

func TestRunShutsDownCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &syncWriter{}
	ready := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		done <- run(ctx, []string{"-listen", "127.0.0.1:0", "-admin-listen", "127.0.0.1:0", "-id", "backend-stop"},
			logs, mapEnv(nil), func(g, _ string) { ready <- g })
	}()

	var grpcAddr string
	select {
	case grpcAddr = <-ready:
	case err := <-done:
		t.Fatalf("run exited before becoming ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the backend to start")
	}

	conn := dial(t, grpcAddr)
	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	if _, err := echov1.NewEchoServiceClient(conn).Echo(callCtx, &echov1.EchoRequest{}); err != nil {
		t.Fatalf("Echo: %v", err)
	}

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	// The drain window must actually be observed: it is what lets the load balancer's
	// health checker see NOT_SERVING before the process disappears.
	if elapsed := time.Since(start); elapsed < drainDelay {
		t.Fatalf("shutdown took %s, want at least the %s drain", elapsed, drainDelay)
	}
	if !strings.Contains(logs.String(), `"msg":"backend stopped"`) {
		t.Fatalf("shutdown was not logged: %s", logs)
	}
}

func TestRunInvalidOptions(t *testing.T) {
	t.Parallel()

	err := run(context.Background(), []string{"-fail-ratio", "2"}, io.Discard, mapEnv(nil), nil)
	if err == nil {
		t.Fatal("run with an invalid -fail-ratio returned nil")
	}
}

func httpGet(t *testing.T, url string) (body string, code int) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(b), resp.StatusCode
}
