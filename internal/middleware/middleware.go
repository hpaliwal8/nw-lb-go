// Package middleware holds the gRPC stream interceptors the load balancer runs in front of the
// proxy handler, plus the per-request state they attach to the context.
//
// The pipeline cmd/lb assembles, outermost first:
//
//	Recovery -> Context -> Logging -> Metrics -> RateLimit -> CircuitBreak -> proxy handler
package middleware

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// Metadata keys read and written by the pipeline. gRPC lowercases metadata keys on the wire, so
// these must stay lowercase for metadata.MD.Get to match.
const (
	RequestIDHeader = "x-request-id"
	ClientIDHeader  = "x-client-id"
)

// unknownMethod labels work that reached an interceptor without a *grpc.StreamServerInfo. That
// only happens in tests and in direct calls, but a metric label must never be empty.
const unknownMethod = "unknown"

// RequestInfo carries per-RPC state from the Context interceptor to everything downstream.
//
// The exported fields are written once, before the value is published into the context, and are
// immutable afterwards — that publication is what makes them safe to read from any goroutine
// handling the RPC. Everything the proxy mutates mid-flight lives behind the atomics below, so a
// proxy goroutine writing while Logging reads is race-free.
//
// A RequestInfo must not be copied once published.
type RequestInfo struct {
	ID        string
	Method    string
	StartedAt time.Time
	HashKey   string

	backend  atomic.Pointer[string]
	hashKey  atomic.Pointer[string]
	attempts atomic.Int64
}

// SetBackend records the backend that served (or last attempted) the RPC.
func (r *RequestInfo) SetBackend(id string) {
	if r == nil {
		return
	}
	r.backend.Store(&id)
}

// Backend returns the last value passed to SetBackend, or "" if none.
func (r *RequestInfo) Backend() string {
	if r == nil {
		return ""
	}
	if p := r.backend.Load(); p != nil {
		return *p
	}
	return ""
}

// AddAttempt counts one upstream attempt. The first attempt counts too, so a successful
// single-shot RPC reports one.
func (r *RequestInfo) AddAttempt() {
	if r == nil {
		return
	}
	r.attempts.Add(1)
}

func (r *RequestInfo) Attempts() int {
	if r == nil {
		return 0
	}
	return int(r.attempts.Load())
}

// SetHashKey overrides the affinity key resolved by the Context interceptor. It writes a guarded
// copy rather than the exported HashKey field: the field is published immutably and direct readers
// of it must stay race-free even while the proxy is calling this.
func (r *RequestInfo) SetHashKey(k string) {
	if r == nil {
		return
	}
	r.hashKey.Store(&k)
}

// ResolvedHashKey returns the affinity key actually in force: the last SetHashKey value if there
// was one, else the key the Context interceptor resolved.
func (r *RequestInfo) ResolvedHashKey() string {
	if r == nil {
		return ""
	}
	if p := r.hashKey.Load(); p != nil {
		return *p
	}
	return r.HashKey
}

type ctxKey struct{}

// NewContext returns ctx carrying info.
func NewContext(ctx context.Context, info *RequestInfo) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, ctxKey{}, info)
}

// FromContext returns the RequestInfo attached by the Context interceptor.
func FromContext(ctx context.Context) (*RequestInfo, bool) {
	if ctx == nil {
		return nil, false
	}
	info, ok := ctx.Value(ctxKey{}).(*RequestInfo)
	if !ok || info == nil {
		return nil, false
	}
	return info, true
}

// wrappedStream overrides only Context; every other method comes from the embedded stream, so a
// wrapper stays transparent even as grpc.ServerStream grows methods.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// WrapServerStream returns a grpc.ServerStream whose Context reports ctx. Re-wrapping an existing
// wrapper replaces it instead of nesting, so a chain of interceptors does not build a delegation
// stack that every RecvMsg has to walk.
func WrapServerStream(ss grpc.ServerStream, ctx context.Context) grpc.ServerStream {
	if ctx == nil {
		return ss
	}
	if w, ok := ss.(*wrappedStream); ok {
		return &wrappedStream{ServerStream: w.ServerStream, ctx: ctx}
	}
	return &wrappedStream{ServerStream: ss, ctx: ctx}
}

// ClientKey resolves the rate-limit key: the x-client-id metadata value, else the peer IP, else
// "unknown" so every request lands in some bucket.
func ClientKey(ctx context.Context) string {
	if v := mdValue(ctx, ClientIDHeader); v != "" {
		return v
	}
	if ip := PeerIP(ctx); ip != "" {
		return ip
	}
	return unknownMethod
}

// HashKey resolves the affinity key: the hashHeader metadata value, else the peer IP. An empty
// result means "no affinity" — callers must not treat it as a real key.
func HashKey(ctx context.Context, hashHeader string) string {
	if hashHeader != "" {
		if v := mdValue(ctx, hashHeader); v != "" {
			return v
		}
	}
	return PeerIP(ctx)
}

// PeerIP returns the caller's IP without the port, or "" when there is no peer. Non-IP transports
// (unix sockets, in-process pipes) have no port to strip and yield their address verbatim.
func PeerIP(ctx context.Context) string {
	addr := peerAddr(ctx)
	if addr == nil {
		return ""
	}
	switch a := addr.(type) {
	case *net.TCPAddr:
		if a.IP == nil {
			return ""
		}
		return a.IP.String()
	case *net.UDPAddr:
		if a.IP == nil {
			return ""
		}
		return a.IP.String()
	}
	s := addr.String()
	if host, _, err := net.SplitHostPort(s); err == nil {
		return host
	}
	return s
}

// PeerAddr returns the caller's address with its port, as logged.
func PeerAddr(ctx context.Context) string {
	if addr := peerAddr(ctx); addr != nil {
		return addr.String()
	}
	return ""
}

func peerAddr(ctx context.Context) net.Addr {
	if ctx == nil {
		return nil
	}
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return nil
	}
	return p.Addr
}

// mdValue returns the first non-empty value for key in the incoming metadata.
//
// ValueFromIncomingContext indexes the context's metadata directly and copies only the values that
// matched. FromIncomingContext deep-copies the entire map — a fresh map plus a fresh []string for
// every header present — which is a per-request allocation storm on a proxy that reads a couple of
// keys from metadata it never mutates.
func mdValue(ctx context.Context, key string) string {
	if ctx == nil {
		return ""
	}
	for _, v := range metadata.ValueFromIncomingContext(ctx, key) {
		if v != "" {
			return v
		}
	}
	return ""
}

func methodOf(info *grpc.StreamServerInfo) string {
	if info == nil || info.FullMethod == "" {
		return unknownMethod
	}
	return info.FullMethod
}
