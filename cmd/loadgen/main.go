// Command loadgen drives load at a gRPC target and reports the latency distribution it observed.
//
// The tool exists to answer one question — does routing stay under the SLO — so it is built
// around the failure mode that makes most load generators lie: coordinated omission. In open
// mode each request has a fixed scheduled start (start + i/rps) that does not move when the
// system under test slows down, and the report carries both service time (send → response) and
// response time (scheduled → response). A generator that measures only service time reports
// healthy latency for a target that is falling behind, because the requests it never managed to
// issue on time are exactly the slow ones.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	echov1 "github.com/hitanshpaliwal/nw-lb-go/gen/echo/v1"
)

const (
	modeOpen   = "open"
	modeClosed = "closed"

	methodUnary  = "unary"
	methodStream = "server_stream"

	// sessionHeader is the metadata key the load balancer hashes on by default
	// (config.Routing.HashHeader).
	sessionHeader = "x-session-id"

	// requestTimeout bounds a single RPC. It is deliberately NOT derived from the run context:
	// a request already in flight when SIGINT arrives must be allowed to finish, otherwise
	// shutdown injects a burst of Canceled samples into the very tail being measured.
	requestTimeout = 30 * time.Second

	// connectTimeout bounds the initial connection so a dead target fails with a clear message
	// instead of producing a run made entirely of Unavailable.
	connectTimeout = 5 * time.Second

	maxMsgSize = 64 << 20
)

type options struct {
	target      string
	rps         float64
	duration    time.Duration
	concurrency int
	mode        string
	warmup      int
	payload     int
	delayMs     int
	keys        int
	conns       int
	method      string
	streamCount int
	out         string
	label       string
	slo         time.Duration
	errorBudget float64
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	// LOADGEN_TARGET backstops -target so that overriding the command in a container (which
	// replaces the whole argument list) cannot silently drop the target and point the run at
	// loopback inside its own namespace.
	defaultTarget := "127.0.0.1:8080"
	if v, ok := os.LookupEnv("LOADGEN_TARGET"); ok && v != "" {
		defaultTarget = v
	}
	fs.StringVar(&o.target, "target", defaultTarget, "host:port of the gRPC target (env LOADGEN_TARGET)")
	fs.Float64Var(&o.rps, "rps", 500, "requests per second to schedule (open mode)")
	fs.DurationVar(&o.duration, "duration", 10*time.Second, "length of the measurement window")
	fs.IntVar(&o.concurrency, "concurrency", 50, "worker goroutines issuing requests")
	fs.StringVar(&o.mode, "mode", modeOpen, "open (fixed schedule, no coordinated omission) or closed (tight loop)")
	fs.IntVar(&o.warmup, "warmup", 100, "requests executed and discarded before measuring")
	fs.IntVar(&o.payload, "payload", 128, "request payload size in bytes")
	fs.IntVar(&o.delayMs, "delay-ms", 0, "server-side think time per request")
	fs.IntVar(&o.keys, "keys", 1000, "distinct x-session-id values; 0 sends no affinity header")
	fs.IntVar(&o.conns, "conns", 0, "client connections; 0 means min(concurrency, NumCPU)")
	fs.StringVar(&o.method, "method", methodUnary, "unary or server_stream")
	fs.IntVar(&o.streamCount, "stream-count", 5, "messages per server_stream response")
	fs.StringVar(&o.out, "out", "", "path to write the JSON report to; empty prints only the table")
	fs.StringVar(&o.label, "label", "loadgen", "label recorded in the report")
	fs.DurationVar(&o.slo, "slo", 200*time.Millisecond, "response-time SLO, compared against p99")
	fs.Float64Var(&o.errorBudget, "error-budget", 0.001,
		"fraction of failed RPCs tolerated before the run fails outright")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if err := o.validate(); err != nil {
		return options{}, err
	}
	return o, nil
}

func (o *options) validate() error {
	if o.target == "" {
		return errors.New("-target is required")
	}
	switch o.mode {
	case modeOpen:
		if o.rps <= 0 {
			return errors.New("-rps must be > 0 in open mode")
		}
	case modeClosed:
	default:
		return fmt.Errorf("-mode must be %q or %q, got %q", modeOpen, modeClosed, o.mode)
	}
	switch o.method {
	case methodUnary:
		o.streamCount = 0
	case methodStream:
		if o.streamCount < 1 {
			return errors.New("-stream-count must be >= 1")
		}
	default:
		return fmt.Errorf("-method must be %q or %q, got %q", methodUnary, methodStream, o.method)
	}
	if o.duration <= 0 {
		return errors.New("-duration must be > 0")
	}
	if o.concurrency < 1 {
		return errors.New("-concurrency must be >= 1")
	}
	if o.warmup < 0 {
		return errors.New("-warmup must be >= 0")
	}
	if o.payload < 0 {
		return errors.New("-payload must be >= 0")
	}
	if o.delayMs < 0 {
		return errors.New("-delay-ms must be >= 0")
	}
	if o.keys < 0 {
		return errors.New("-keys must be >= 0")
	}
	if o.errorBudget < 0 || o.errorBudget > 1 {
		return errors.New("-error-budget must be in [0,1]")
	}
	if o.slo <= 0 {
		return errors.New("-slo must be > 0")
	}
	if o.conns <= 0 {
		o.conns = max(1, min(o.concurrency, runtime.NumCPU()))
	}
	return nil
}

func run(args []string, stdout, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Restore default signal handling once the first signal has been seen, so a second Ctrl-C
	// kills the process even if the target has wedged an in-flight RPC.
	go func() {
		<-ctx.Done()
		stop()
	}()

	conns := make([]*grpc.ClientConn, 0, o.conns)
	defer func() {
		for _, cc := range conns {
			cc.Close()
		}
	}()
	for range o.conns {
		cc, err := grpc.NewClient(o.target, dialOptions()...)
		if err != nil {
			return fmt.Errorf("creating client for %s: %w", o.target, err)
		}
		conns = append(conns, cc)
	}
	for _, cc := range conns {
		if err := waitReady(ctx, cc, connectTimeout); err != nil {
			if ctx.Err() != nil {
				return errors.New("interrupted before the run started")
			}
			return fmt.Errorf("target %s is not accepting connections: %w", o.target, err)
		}
	}

	keyTable := buildKeyTable(o.keys)
	payload := make([]byte, o.payload)
	callers := make([]*caller, o.concurrency)
	for w := range callers {
		callers[w] = newCaller(conns[w%len(conns)], o, payload)
	}

	warmup(ctx, o, callers, keyTable)

	start := time.Now()
	var perWorker [][]sample
	if o.mode == modeOpen {
		perWorker = runOpen(ctx, o, callers, keyTable, start)
	} else {
		perWorker = runClosed(ctx, o, callers, keyTable, start)
	}
	elapsed := time.Since(start)
	scheduled := o.warmupExcludedTotal()
	interrupted := ctx.Err() != nil

	rep := buildReport(mergeSamples(perWorker), runParams{
		Label:        o.label,
		Target:       o.target,
		Mode:         o.mode,
		Method:       o.method,
		StreamCount:  o.streamCount,
		RPSRequested: rpsRequested(o),
		Elapsed:      elapsed,
		Scheduled:    scheduled,
		Interrupted:  interrupted,
		SLO:          o.slo,
		ErrorBudget:  o.errorBudget,
		Conns:        o.conns,
		Concurrency:  o.concurrency,
		PayloadBytes: o.payload,
		DelayMs:      o.delayMs,
	})

	if o.out != "" {
		b, err := rep.JSON()
		if err != nil {
			return fmt.Errorf("encoding report: %w", err)
		}
		if err := os.WriteFile(o.out, b, 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", o.out, err)
		}
	}
	if _, err := io.WriteString(stdout, rep.Table()); err != nil {
		return err
	}
	if ctx.Err() != nil {
		fmt.Fprintln(stderr, "loadgen: interrupted; the report covers only what was collected")
	}
	return nil
}

func dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}

// waitReady drives the lazy connection to READY up front. Checking connectivity rather than
// firing a probe RPC keeps a backend that deliberately returns errors (fail_code, fail-ratio)
// from being mistaken for a target that is down.
func waitReady(ctx context.Context, cc *grpc.ClientConn, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cc.Connect()
	for {
		s := cc.GetState()
		switch s {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return errors.New("connection is shut down")
		}
		if !cc.WaitForStateChange(ctx, s) {
			return fmt.Errorf("still %s after %s: %w", s, timeout, context.Cause(ctx))
		}
	}
}

// buildKeyTable materialises the session keys once. Formatting a key per request would put an
// allocation on the hot path for no reason.
func buildKeyTable(n int) []string {
	if n <= 0 {
		return nil
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

func keyAt(keys []string, i int) string {
	if len(keys) == 0 {
		return ""
	}
	return keys[i%len(keys)]
}

// caller issues requests over one connection. Each worker owns one, so the request message can
// be reused across calls without synchronisation: gRPC marshals it before the call returns.
type caller struct {
	client      echov1.EchoServiceClient
	method      string
	req         *echov1.EchoRequest
	streamCount uint32
}

func newCaller(cc *grpc.ClientConn, o options, payload []byte) *caller {
	return &caller{
		client: echov1.NewEchoServiceClient(cc),
		method: o.method,
		req: &echov1.EchoRequest{
			Payload:     payload,
			DelayMs:     uint32(o.delayMs),
			StreamCount: uint32(o.streamCount),
		},
		streamCount: uint32(o.streamCount),
	}
}

// do issues one request and reports the backend that served it and the resulting status code.
// For server_stream the call is complete only once the whole stream has been drained.
func (c *caller) do(ctx context.Context, key string) (string, codes.Code) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	c.req.SessionKey = key
	if key != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, sessionHeader, key)
	}

	if c.method == methodUnary {
		resp, err := c.client.Echo(ctx, c.req)
		if err != nil {
			return "", status.Code(err)
		}
		return resp.GetBackendId(), codes.OK
	}

	stream, err := c.client.ServerStream(ctx, c.req)
	if err != nil {
		return "", status.Code(err)
	}
	var backend string
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return backend, codes.OK
		}
		if err != nil {
			return backend, status.Code(err)
		}
		if backend == "" {
			backend = msg.GetBackendId()
		}
	}
}

// warmup pays for connection setup, HTTP/2 handshakes and code paths that are cold on the first
// call, then throws the results away.
func warmup(ctx context.Context, o options, callers []*caller, keys []string) {
	if o.warmup <= 0 {
		return
	}
	workers := min(o.concurrency, o.warmup)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < o.warmup; i += workers {
				if ctx.Err() != nil {
					return
				}
				callers[w].do(context.Background(), keyAt(keys, i))
			}
		}(w)
	}
	wg.Wait()
}

// slot is one scheduled request in open mode.
type slot struct {
	index int
	at    time.Time
}

// runOpen issues requests against a fixed schedule. The schedule is absolute: when workers fall
// behind, the backlog is recorded as response time rather than pushing later requests out, which
// is exactly the delay a naive generator omits.
//
// There is no artificial cutoff at -duration: the run ends when all rps*duration requests have
// completed, which for an overloaded target is later than -duration. Cutting the run off would
// discard exactly the outstanding requests — the slowest ones, the tail the measurement is for.
// SIGINT stops it early and still reports what was collected.
func runOpen(ctx context.Context, o options, callers []*caller, keys []string, start time.Time) [][]sample {
	n := int(o.rps * o.duration.Seconds())
	if n < 0 {
		n = 0
	}
	slots := make(chan slot, 2*o.concurrency)

	go func() {
		defer close(slots)
		// One timer for the whole schedule. Under Go 1.23+ timer semantics Reset and Stop are
		// safe without draining the channel, so no stale tick can leak into the next wait.
		timer := time.NewTimer(time.Hour)
		timer.Stop()
		defer timer.Stop()
		for i := range n {
			at := start.Add(time.Duration(float64(i) / o.rps * float64(time.Second)))
			if d := time.Until(at); d > 0 {
				timer.Reset(d)
				select {
				case <-timer.C:
				case <-ctx.Done():
					return
				}
			}
			select {
			case slots <- slot{index: i, at: at}:
			case <-ctx.Done():
				return
			}
		}
	}()

	perWorker := make([][]sample, o.concurrency)
	var wg sync.WaitGroup
	for w := range o.concurrency {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]sample, 0, n/o.concurrency+8)
			c := callers[w]
			for s := range slots {
				if ctx.Err() != nil {
					break
				}
				key := keyAt(keys, s.index)
				sentAt := time.Now()
				backend, code := c.do(context.Background(), key)
				local = append(local, sample{
					scheduledAt: s.at,
					sentAt:      sentAt,
					completedAt: time.Now(),
					code:        code,
					backendID:   backend,
					sessionKey:  key,
				})
			}
			perWorker[w] = local
		}(w)
	}
	wg.Wait()
	return perWorker
}

// runClosed keeps -concurrency requests in flight for the whole window. Every worker waits for
// its own response before issuing the next request, so the offered load bends to whatever the
// target can serve: scheduledAt is sentAt and the two timers are identical by construction.
func runClosed(ctx context.Context, o options, callers []*caller, keys []string, start time.Time) [][]sample {
	ctx, cancel := context.WithDeadline(ctx, start.Add(o.duration))
	defer cancel()

	estimate := 1024
	if o.rps > 0 {
		estimate = int(o.rps * o.duration.Seconds())
	}

	perWorker := make([][]sample, o.concurrency)
	var wg sync.WaitGroup
	for w := range o.concurrency {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]sample, 0, estimate/o.concurrency+1)
			c := callers[w]
			for i := 0; ctx.Err() == nil; i++ {
				// Stride by the worker count so every worker walks a disjoint slice of the key
				// space and the keys stay evenly covered.
				key := keyAt(keys, w+i*o.concurrency)
				sentAt := time.Now()
				backend, code := c.do(context.Background(), key)
				local = append(local, sample{
					scheduledAt: sentAt,
					sentAt:      sentAt,
					completedAt: time.Now(),
					code:        code,
					backendID:   backend,
					sessionKey:  key,
				})
			}
			perWorker[w] = local
		}(w)
	}
	wg.Wait()
	return perWorker
}

// rpsRequested is the target rate the run was asked for, or 0 in closed mode where -rps has no
// effect on the offered load and reporting it invites the reader to compare it against the
// achieved rate as though the generator had missed a target it was never given.
func rpsRequested(o options) float64 {
	if o.mode == modeClosed {
		return 0
	}
	return o.rps
}

// warmupExcludedTotal is how many measured requests the run intended to issue. Closed mode has no
// schedule, so it reports 0 and the report falls back to the completed count.
func (o options) warmupExcludedTotal() int {
	if o.mode != modeOpen {
		return 0
	}
	n := int(o.rps * o.duration.Seconds())
	if n < 0 {
		return 0
	}
	return n
}
