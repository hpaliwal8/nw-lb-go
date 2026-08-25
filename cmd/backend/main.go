// Command backend is the demo upstream used to exercise the load balancer.
//
// It is deliberately controllable: callers ask it to burn time or fail through the
// EchoRequest fields, and the operator can add a base latency or a random failure ratio
// through flags, so failover, circuit breaking and latency behaviour can be measured
// without a second real service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	echov1 "github.com/hitanshpaliwal/nw-lb-go/gen/echo/v1"
)

const (
	defaultListen      = ":50051"
	defaultAdminListen = ":9101"

	maxMsgSize = 16 << 20

	// drainDelay is how long the process keeps serving after flipping its health status to
	// NOT_SERVING. The load balancer's health checker polls at ~1s but only needs one probe
	// to observe the flip and take this backend out of rotation before connections drop.
	drainDelay = 250 * time.Millisecond

	adminShutdownTimeout = 5 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stderr, os.LookupEnv, nil); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "backend: %v\n", err)
		os.Exit(1)
	}
}

// run is main without the process-global bits so tests can drive a real server: args
// excludes the program name, logs go to logw, the environment is read through getenv, and
// ready (when non-nil) is called with the resolved listener addresses once both are bound,
// which is the only way a caller can learn the port when listening on :0.
func run(ctx context.Context, args []string, logw io.Writer, getenv func(string) (string, bool), ready func(grpcAddr, adminAddr string)) error {
	opts, err := parseOptions(args, getenv, logw)
	if err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(logw, &slog.HandlerOptions{Level: slog.LevelInfo}))

	lis, err := net.Listen("tcp", opts.listen)
	if err != nil {
		return fmt.Errorf("listen %q: %w", opts.listen, err)
	}
	adminLis, err := net.Listen("tcp", opts.adminListen)
	if err != nil {
		lis.Close()
		return fmt.Errorf("listen %q: %w", opts.adminListen, err)
	}

	reg := prometheus.NewRegistry()
	srv := newServer(opts, lis.Addr().String(), reg)

	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		// The load balancer keeps idle connections warm with 30s pings and no active stream;
		// the default enforcement policy would answer those with a GOAWAY.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	echov1.RegisterEchoServiceServer(gs, srv)

	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(gs, hs)
	hs.SetServingStatus("", opts.servingStatus())
	hs.SetServingStatus(echov1.EchoService_ServiceDesc.ServiceName, opts.servingStatus())

	reflection.Register(gs)

	adminSrv := &http.Server{
		Handler:           adminHandler(reg),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 2)
	go func() {
		if err := gs.Serve(lis); err != nil {
			serveErr <- fmt.Errorf("grpc serve: %w", err)
		}
	}()
	go func() {
		if err := adminSrv.Serve(adminLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("admin serve: %w", err)
		}
	}()

	log.Info("backend started",
		"id", opts.id,
		"listen", lis.Addr().String(),
		"admin_listen", adminLis.Addr().String(),
		"delay", opts.delay.String(),
		"fail_ratio", opts.failRatio,
		"health", opts.health,
	)
	if ready != nil {
		ready(lis.Addr().String(), adminLis.Addr().String())
	}

	var runErr error
	select {
	case runErr = <-serveErr:
	case <-ctx.Done():
	}

	log.Info("backend draining", "id", opts.id, "drain", drainDelay.String())
	hs.Shutdown()
	timer := time.NewTimer(drainDelay)
	<-timer.C
	timer.Stop()

	gs.GracefulStop()

	// The parent context is already cancelled on the signal path, so the admin shutdown gets
	// its own budget rather than inheriting a dead deadline.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adminShutdownTimeout)
	defer cancel()
	if err := adminSrv.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("admin shutdown: %w", err))
	}

	log.Info("backend stopped", "id", opts.id, "error", runErr)
	return runErr
}

type options struct {
	listen      string
	adminListen string
	id          string
	delay       time.Duration
	failRatio   float64
	health      string
}

func (o options) servingStatus() grpc_health_v1.HealthCheckResponse_ServingStatus {
	if o.health == "not_serving" {
		return grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}
	return grpc_health_v1.HealthCheckResponse_SERVING
}

// parseOptions resolves flags over environment fallbacks: an env var supplies the flag's
// default, so an explicit flag always wins.
func parseOptions(args []string, getenv func(string) (string, bool), out io.Writer) (options, error) {
	if getenv == nil {
		getenv = os.LookupEnv
	}

	defDelay, err := envDuration(getenv, "BACKEND_DELAY", 0)
	if err != nil {
		return options{}, err
	}
	defFailRatio, err := envFloat(getenv, "BACKEND_FAIL_RATIO", 0)
	if err != nil {
		return options{}, err
	}

	fs := flag.NewFlagSet("backend", flag.ContinueOnError)
	fs.SetOutput(out)

	var o options
	fs.StringVar(&o.listen, "listen", envString(getenv, "BACKEND_LISTEN", defaultListen), "gRPC listen address")
	fs.StringVar(&o.adminListen, "admin-listen", defaultAdminListen, "HTTP admin listen address serving /healthz and /metrics")
	fs.StringVar(&o.id, "id", envString(getenv, "BACKEND_ID", defaultID()), "backend identity reported in every response")
	fs.DurationVar(&o.delay, "delay", defDelay, "base latency added to every reply")
	fs.Float64Var(&o.failRatio, "fail-ratio", defFailRatio, "fraction of unary calls failed with Unavailable, 0..1")
	fs.StringVar(&o.health, "health", "serving", "initial health status: serving|not_serving")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if o.listen == "" {
		return options{}, errors.New("-listen must not be empty")
	}
	if o.adminListen == "" {
		return options{}, errors.New("-admin-listen must not be empty")
	}
	if o.id == "" {
		return options{}, errors.New("-id must not be empty")
	}
	if o.delay < 0 {
		return options{}, fmt.Errorf("-delay must not be negative, got %s", o.delay)
	}
	if o.failRatio < 0 || o.failRatio > 1 {
		return options{}, fmt.Errorf("-fail-ratio must be in [0,1], got %v", o.failRatio)
	}
	switch o.health {
	case "serving", "not_serving":
	default:
		return options{}, fmt.Errorf("-health must be serving or not_serving, got %q", o.health)
	}
	return o, nil
}

func defaultID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "backend"
}

func envString(getenv func(string) (string, bool), key, def string) string {
	if v, ok := getenv(key); ok && v != "" {
		return v
	}
	return def
}

// envDuration accepts a Go duration ("5ms") or a bare number, which container
// environments tend to supply and which is read as milliseconds.
func envDuration(getenv func(string) (string, bool), key string, def time.Duration) (time.Duration, error) {
	v, ok := getenv(key)
	if !ok || v == "" {
		return def, nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d, nil
	}
	ms, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: not a duration", key, v)
	}
	return time.Duration(ms * float64(time.Millisecond)), nil
}

func envFloat(getenv func(string) (string, bool), key string, def float64) (float64, error) {
	v, ok := getenv(key)
	if !ok || v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: not a number", key, v)
	}
	return f, nil
}

func adminHandler(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// server implements EchoService. Every field is immutable after construction, so the
// handlers need no locking; the counter and the rand source are themselves concurrent.
type server struct {
	echov1.UnimplementedEchoServiceServer

	id        string
	addr      string
	baseDelay time.Duration
	failRatio float64

	requests *prometheus.CounterVec

	// randFloat draws the fail-ratio dice; tests replace it to remove the randomness.
	randFloat func() float64
}

func newServer(o options, addr string, reg prometheus.Registerer) *server {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "backend_requests_total",
		Help: "Total RPCs handled by this demo backend.",
	}, []string{"method", "code"})
	reg.MustRegister(requests)

	return &server{
		id:        o.id,
		addr:      addr,
		baseDelay: o.delay,
		failRatio: o.failRatio,
		requests:  requests,
		randFloat: rand.Float64,
	}
}

func (s *server) Echo(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	resp, err := s.echo(ctx, req)
	s.record("Echo", err)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *server) echo(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	if err := s.pause(ctx, req.GetDelayMs()); err != nil {
		return nil, err
	}
	if err := injectedFailure(req); err != nil {
		return nil, err
	}
	// Only unary calls are subject to the operator's fail ratio: a stream that dies
	// mid-flight is a different failure mode from a rejected request.
	if s.failRatio > 0 && s.randFloat() < s.failRatio {
		return nil, status.Error(codes.Unavailable, "injected failure")
	}
	return s.reply(req.GetPayload(), 0), nil
}

func (s *server) ServerStream(req *echov1.EchoRequest, stream grpc.ServerStreamingServer[echov1.EchoResponse]) error {
	err := s.serverStream(req, stream)
	s.record("ServerStream", err)
	return err
}

func (s *server) serverStream(req *echov1.EchoRequest, stream grpc.ServerStreamingServer[echov1.EchoResponse]) error {
	ctx := stream.Context()
	n := max(req.GetStreamCount(), 1)
	for i := uint32(0); i < n; i++ {
		if err := s.pause(ctx, req.GetDelayMs()); err != nil {
			return err
		}
		if err := injectedFailure(req); err != nil {
			return err
		}
		if err := stream.Send(s.reply(req.GetPayload(), i)); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) ClientStream(stream grpc.ClientStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	err := s.clientStream(stream)
	s.record("ClientStream", err)
	return err
}

func (s *server) clientStream(stream grpc.ClientStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	ctx := stream.Context()
	total := 0
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := s.pause(ctx, req.GetDelayMs()); err != nil {
			return err
		}
		if err := injectedFailure(req); err != nil {
			return err
		}
		total += len(req.GetPayload())
	}
	return stream.SendAndClose(s.reply([]byte(strconv.Itoa(total)), 0))
}

func (s *server) BidiStream(stream grpc.BidiStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	err := s.bidiStream(stream)
	s.record("BidiStream", err)
	return err
}

func (s *server) bidiStream(stream grpc.BidiStreamingServer[echov1.EchoRequest, echov1.EchoResponse]) error {
	ctx := stream.Context()
	for seq := uint32(0); ; seq++ {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.pause(ctx, req.GetDelayMs()); err != nil {
			return err
		}
		if err := injectedFailure(req); err != nil {
			return err
		}
		if err := stream.Send(s.reply(req.GetPayload(), seq)); err != nil {
			return err
		}
	}
}

func (s *server) reply(payload []byte, seq uint32) *echov1.EchoResponse {
	return &echov1.EchoResponse{
		Payload:        payload,
		BackendId:      s.id,
		BackendAddr:    s.addr,
		Seq:            seq,
		ServedUnixNano: time.Now().UnixNano(),
	}
}

// pause burns the requested think time. Sleeping is the point of this process — it is the
// upstream under test, not the load balancer's hot path — but it still yields to a
// cancelled call instead of holding the stream open for a client that has gone away.
func (s *server) pause(ctx context.Context, extraMs uint32) error {
	d := s.baseDelay + time.Duration(extraMs)*time.Millisecond
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func injectedFailure(req *echov1.EchoRequest) error {
	if code := codes.Code(req.GetFailCode()); code != codes.OK {
		return status.Error(code, "injected failure")
	}
	return nil
}

// record is the only per-request bookkeeping: a counter, never a log line, because the
// demo runs at load-generator rates.
func (s *server) record(method string, err error) {
	s.requests.WithLabelValues(method, status.Code(err).String()).Inc()
}
