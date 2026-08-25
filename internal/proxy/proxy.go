package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hitanshpaliwal/nw-lb-go/internal/balancer"
	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/middleware"
	"github.com/hitanshpaliwal/nw-lb-go/internal/pool"
)

// forwardedForHeader accumulates the chain of proxies a request passed through.
const forwardedForHeader = "x-forwarded-for"

// hopByHop names metadata that describes a single HTTP/2 hop and must not be copied onto the next
// one. Reserved ":"-prefixed pseudo-headers are dropped by prefix rather than by name.
var hopByHop = map[string]struct{}{
	"connection":          {},
	"te":                  {},
	"trailer":             {},
	"transfer-encoding":   {},
	"upgrade":             {},
	"keep-alive":          {},
	"proxy-authorization": {},
}

// upstreamDesc describes every forwarded call as fully bidirectional. The proxy does not know the
// real cardinality of a method it has no schema for, and a bidi description is the only one that
// permits the message pattern of all four kinds.
var upstreamDesc = &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}

// errNotReplayable reports that the client→upstream pump consumed a message that the proxy did not
// buffer, so the RPC can no longer be moved to another backend.
var errNotReplayable = errors.New("proxy: request is no longer replayable")

type Proxy struct {
	bal *balancer.Balancer
	m   *metrics.Metrics
	log *slog.Logger

	hashHeader  string
	maxAttempts int
	retryPolicy string
}

func New(b *balancer.Balancer, cfg config.Config, m *metrics.Metrics, log *slog.Logger) *Proxy {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	attempts := cfg.Routing.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	policy := cfg.Routing.RetryPolicy
	if policy == "" {
		policy = config.RetryConnectFailure
	}
	return &Proxy{
		bal:         b,
		m:           m,
		log:         log,
		hashHeader:  cfg.Routing.HashHeader,
		maxAttempts: attempts,
		retryPolicy: policy,
	}
}

// Handler returns the stream handler to install with grpc.UnknownServiceHandler.
func (p *Proxy) Handler() grpc.StreamHandler { return p.handle }

// ServerOptions returns the two options a server needs to become a proxy: the passthrough codec,
// which keeps bodies opaque, and the catch-all handler that forwards every unregistered method.
func (p *Proxy) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ForceServerCodecV2(Codec{}),
		grpc.UnknownServiceHandler(p.Handler()),
	}
}

func (p *Proxy) handle(_ any, stream grpc.ServerStream) error {
	ctx := stream.Context()
	method, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Internal, "proxy: no method in the server stream context")
	}

	candidates := p.bal.Pick(p.hashKey(ctx), p.maxAttempts)
	if len(candidates) == 0 {
		return status.Error(codes.Unavailable, "no backends available")
	}

	// Derived from the inbound stream context, so the client's deadline bounds the upstream call
	// and a client cancellation tears the upstream stream down. The extra cancel guarantees that
	// an upstream stream abandoned by a retry, or left half-read when this handler returns early,
	// does not stay open on the backend.
	upCtx, cancel := context.WithCancel(metadata.NewOutgoingContext(ctx, forwardMD(ctx)))
	defer cancel()

	info, _ := middleware.FromContext(ctx)
	e := &exchange{p: p, stream: stream, method: method, info: info, sendDone: make(chan error, 1)}
	// The buffered first message is owned by this goroutine for the whole RPC: SendMsg is
	// reference-neutral (see Codec), so every attempt may replay it and exactly one Free is owed.
	defer func() { e.first.Free() }()

	switch err := stream.RecvMsg(&e.first); {
	case err == nil:
		e.haveFirst = true
	case errors.Is(err, io.EOF):
		// A client-streaming call is allowed to send nothing at all. There is no message to
		// replay, but the half-close still has to reach whichever backend is chosen.
		e.halfClosed = true
	default:
		return err
	}

	return e.run(ctx, upCtx, candidates)
}

// hashKey prefers the key the Context interceptor already resolved, so the value the proxy routes
// on is the same one the logs and metrics report.
func (p *Proxy) hashKey(ctx context.Context) string {
	if info, ok := middleware.FromContext(ctx); ok {
		if k := info.ResolvedHashKey(); k != "" {
			return k
		}
	}
	return middleware.HashKey(ctx, p.hashHeader)
}

// forwardMD copies the inbound metadata for the upstream call, dropping what belongs to the
// inbound hop only and recording this proxy's view of the caller in x-forwarded-for. Keys are
// already lowercase coming off the wire; they are lowered again because a direct in-process caller
// may not have bothered.
func forwardMD(ctx context.Context) metadata.MD {
	in, _ := metadata.FromIncomingContext(ctx)
	out := make(metadata.MD, len(in)+1)
	for k, vals := range in {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, ":") {
			continue
		}
		if _, drop := hopByHop[lk]; drop {
			continue
		}
		out[lk] = append(out[lk], vals...)
	}
	if ip := middleware.PeerIP(ctx); ip != "" {
		out.Append(forwardedForHeader, ip)
	}
	return out
}

// exchange is the state of one proxied RPC.
//
// Two goroutines touch it: this RPC's handler goroutine, which owns the upstream→client direction
// and every retry decision, and the pump goroutine, which owns the client→upstream direction. The
// fields under mu are exactly the ones both touch.
type exchange struct {
	p      *Proxy
	stream grpc.ServerStream
	method string
	info   *middleware.RequestInfo

	first     mem.BufferSlice
	haveFirst bool

	// Handler-goroutine only: what has already reached the downstream client. Once either is set
	// the RPC is visible to the caller and can never be replayed elsewhere.
	sentHeader bool
	sentMsg    bool
	// upstreamDone records that the upstream stream reached its final status. grpc.ClientStream's
	// Trailer may only be read after that point, so relaying that ended on a downstream write
	// failure must not touch it.
	upstreamDone bool

	// sendDone is buffered so the pump can publish its result and exit even when this handler has
	// already returned and nobody is listening.
	sendDone chan error

	// mu serialises installing an upstream stream against the pump consuming a client message.
	// Holding it across the whole install is what makes retries honest: the pump can neither
	// observe a stream that has not yet been given the replayed first message, nor slip a second
	// client message past a retry decision that assumed there was none.
	mu         sync.Mutex
	cs         grpc.ClientStream
	pumping    bool
	committed  bool // the pump moved past the buffered first message, or the client's send side died
	halfClosed bool // the client's send side is closed, so a fresh stream must be closed by us
}

func (e *exchange) run(ctx, upCtx context.Context, candidates []*pool.Backend) error {
	var lastErr error
	for i, b := range candidates {
		last := i == len(candidates)-1
		// Reserve, not Available: this commits to the backend and consumes a half-open probe slot
		// that e.record resolves through the returned probe. The last candidate proceeds either
		// way so a fleet whose breakers are all open still gets traffic instead of black-holing.
		probe, admitted := b.Reserve()
		if !admitted && !last {
			continue
		}
		e.info.AddAttempt()
		e.info.SetBackend(b.ID())

		started := time.Now()
		attemptCtx, attemptCancel := context.WithCancel(upCtx)
		defer attemptCancel()

		cs, err := e.install(attemptCtx, b)
		if err != nil {
			attemptCancel()
			if errors.Is(err, errNotReplayable) {
				// install refused before opening a stream, so this backend was never contacted:
				// give the reservation back rather than blaming it for a proxy-side constraint.
				probe.Release()
				return toStatus(orUnavailable(lastErr))
			}
			e.record(b, probe, err, started)
			lastErr = err
			if e.canRetry(ctx, err, i, len(candidates), true) {
				e.failover(err)
				continue
			}
			return toStatus(err)
		}

		err = e.relay(cs)
		e.record(b, probe, err, started)
		if err != nil && e.canRetry(ctx, err, i, len(candidates), false) {
			attemptCancel()
			lastErr = err
			e.failover(err)
			continue
		}
		// Header and trailer are forwarded here rather than inside relay because this is the point
		// the RPC stops being retryable. Sending the header any earlier would set sentHeader and
		// silently disable failover, since a header already on the wire makes a replay observable.
		// The upstream trailer is the caller's only view of application metadata attached to the
		// final status, so both are forwarded on the success and the failure path alike.
		if e.upstreamDone {
			e.forwardHeader(cs)
			e.stream.SetTrailer(cs.Trailer())
		}
		return toStatus(err)
	}
	return toStatus(orUnavailable(lastErr))
}

// orUnavailable covers the paths where every candidate was skipped as unavailable without any
// attempt ever producing an error of its own.
func orUnavailable(err error) error {
	if err == nil {
		return status.Error(codes.Unavailable, "no backends available")
	}
	return err
}

// install opens the upstream stream for one attempt, replays the buffered first message onto it and
// makes it the pump's target, all under mu. A caller that already lost the right to retry gets
// errNotReplayable instead of a stream.
func (e *exchange) install(ctx context.Context, b *pool.Backend) (grpc.ClientStream, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.committed {
		return nil, errNotReplayable
	}

	cs, err := grpc.NewClientStream(ctx, upstreamDesc, b.Conn(), e.method,
		grpc.ForceCodecV2(Codec{}), grpc.WaitForReady(false))
	if err != nil {
		return nil, err
	}
	if e.haveFirst {
		if err := cs.SendMsg(e.first); err != nil {
			return nil, firstSendError(cs, err)
		}
	}

	e.cs = cs
	switch {
	case e.halfClosed:
		// Either there never was a pump, or it has already seen the client's half-close and will
		// not close this replacement. Closing here keeps the upstream from waiting forever.
		_ = cs.CloseSend()
	case !e.pumping:
		e.pumping = true
		go func() { e.sendDone <- e.pump() }()
	}
	return cs, nil
}

// firstSendError turns the io.EOF that SendMsg reports for a broken stream into the real status,
// which only the receive side carries. Without it every setup failure would be recorded as Unknown.
func firstSendError(cs grpc.ClientStream, err error) error {
	if !errors.Is(err, io.EOF) {
		return err
	}
	var discard mem.BufferSlice
	rerr := cs.RecvMsg(&discard)
	discard.Free()
	if rerr != nil {
		return rerr
	}
	return err
}

// pump moves client messages upstream. It reads the current upstream stream under mu on every
// message so that a retry installed between two messages is picked up, and it marks the exchange
// committed before it looks at that stream — a retry decision racing with this call therefore
// either wins outright and gets its replacement used, or loses and is refused.
func (e *exchange) pump() error {
	for {
		var buf mem.BufferSlice
		if err := e.stream.RecvMsg(&buf); err != nil {
			e.mu.Lock()
			cs := e.cs
			if errors.Is(err, io.EOF) {
				e.halfClosed = true
			} else {
				e.committed = true
			}
			e.mu.Unlock()
			if errors.Is(err, io.EOF) {
				return cs.CloseSend()
			}
			return err
		}

		e.mu.Lock()
		e.committed = true
		cs := e.cs
		e.mu.Unlock()

		err := cs.SendMsg(buf)
		buf.Free()
		if err != nil {
			return err
		}
	}
}

// relay moves upstream messages to the client on the handler goroutine. Every slice it receives
// carries one reference owned here and is freed once it has been handed on.
func (e *exchange) relay(cs grpc.ClientStream) error {
	e.upstreamDone = false
	for {
		var buf mem.BufferSlice
		if err := cs.RecvMsg(&buf); err != nil {
			buf.Free()
			e.upstreamDone = true
			if errors.Is(err, io.EOF) {
				e.forwardHeader(cs)
				return nil
			}
			return err
		}

		if !e.sentHeader {
			md, err := cs.Header()
			if err != nil {
				buf.Free()
				// Header only fails by reporting the error that ended the stream.
				e.upstreamDone = true
				return err
			}
			if err := e.stream.SendHeader(md); err != nil {
				buf.Free()
				return err
			}
			e.sentHeader = true
		}

		err := e.stream.SendMsg(buf)
		buf.Free()
		if err != nil {
			return err
		}
		e.sentMsg = true
	}
}

// forwardHeader delivers the upstream header for a call that finished without ever producing a
// message, so header metadata is not lost on an empty server stream.
func (e *exchange) forwardHeader(cs grpc.ClientStream) {
	if e.sentHeader {
		return
	}
	md, err := cs.Header()
	if err != nil || md.Len() == 0 {
		return
	}
	if e.stream.SendHeader(md) == nil {
		e.sentHeader = true
	}
}

// canRetry decides whether a failed attempt may be replayed on the next candidate. Every clause is
// a reason a replay would be observable to the caller as a duplicate or as a truncated stream, so
// all of them must hold.
func (e *exchange) canRetry(ctx context.Context, err error, i, n int, fromSetup bool) bool {
	if i+1 >= n {
		return false
	}
	if e.sentHeader || e.sentMsg {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	e.mu.Lock()
	committed := e.committed
	e.mu.Unlock()
	if committed {
		return false
	}
	if e.p.retryPolicy == config.RetryNone {
		return false
	}

	if fromSetup {
		// The stream could not be established, so nothing reached a backend — but only when the
		// error is the transport saying so. firstSendError recovers the real upstream status when
		// the server answered and then dropped the stream, and a server that answered has already
		// seen the request: replaying a PermissionDenied or an Unimplemented across the fleet
		// would be both useless and, for a method with side effects, unsafe.
		return status.Code(err) == codes.Unavailable
	}

	// Past this point the request was handed to the transport and may have been executed. A gRPC
	// status cannot tell "never arrived" apart from "processed, then the connection died", so the
	// default policy declines rather than risk a duplicate side effect.
	if e.p.retryPolicy != config.RetryUnavailable {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted:
		return true
	case codes.DeadlineExceeded:
		// The deadline that expired was the backend's own, not the caller's; retry only while the
		// caller's budget can still accommodate another attempt.
		dl, ok := ctx.Deadline()
		return !ok || time.Until(dl) > 0
	default:
		return false
	}
}

func (e *exchange) failover(err error) {
	reason := strings.ToLower(status.Code(err).String())
	if e.p.m != nil {
		e.p.m.IncFailover(e.method, reason)
	}
	e.p.log.Debug("proxy failover", "method", e.method, "reason", reason, "error", err)
}

// record applies the attempt's outcome to the backend's breaker and the upstream metrics. The
// failure classification is middleware.IsFailure so a per-backend breaker and a per-method breaker
// never disagree about what counts as the backend's fault.
func (e *exchange) record(b *pool.Backend, probe circuit.Probe, err error, started time.Time) {
	if middleware.IsFailure(err) {
		probe.Failure()
	} else {
		probe.Success()
	}
	if e.p.m != nil {
		e.p.m.ObserveUpstream(b.ID(), status.Code(err).String(), time.Since(started))
	}
}

// toStatus preserves an upstream status error exactly as it arrived, so the caller sees the
// backend's own code and message rather than one invented by the proxy.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	s, _ := status.FromError(err)
	return s.Err()
}
