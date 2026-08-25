package middleware

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// requestIDBytes is 8 random bytes — 16 hex characters. Enough to keep collisions negligible over
// a trace window without bloating every log line and response header.
const requestIDBytes = 8

// maxRequestIDLen bounds an id we accept from the caller. A client-supplied header is echoed back
// and written into every log record, so it has to stay small and printable.
const maxRequestIDLen = 128

// idFallback seeds the non-crypto id path. crypto/rand does not fail in practice, but a request
// must never panic because entropy was unavailable for an instant.
var idFallback atomic.Uint64

// Context establishes the per-RPC identity: it reuses or mints a request id, resolves the affinity
// key, attaches a RequestInfo to the stream context, and echoes the id back in the response header
// so a caller can correlate a failure with the load balancer's logs.
func Context(hashHeader string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		id := requestID(ctx)

		// A header can no longer be set once the proxy has forwarded the upstream header, so this
		// runs before the handler. A failure here means the RPC is already past that point and is
		// not worth failing the request over.
		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, id))

		ri := &RequestInfo{
			ID:        id,
			Method:    methodOf(info),
			StartedAt: time.Now(),
			HashKey:   HashKey(ctx, hashHeader),
		}
		return handler(srv, WrapServerStream(ss, NewContext(ctx, ri)))
	}
}

// requestID reuses an incoming x-request-id when it is plausible, so a trace spanning several
// services keeps one id, and mints a fresh one otherwise.
func requestID(ctx context.Context) string {
	if v := mdValue(ctx, RequestIDHeader); validRequestID(v) {
		return v
	}
	return newRequestID()
}

func validRequestID(v string) bool {
	if v == "" || len(v) > maxRequestIDLen {
		return false
	}
	for i := range len(v) {
		if v[i] < 0x20 || v[i] > 0x7e {
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [requestIDBytes]byte
	if _, err := crand.Read(b[:]); err != nil {
		n := idFallback.Add(1)
		// Mixing a monotonic counter with the wall clock keeps ids unique within a process and
		// unlikely to repeat across restarts.
		binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano())^(n*0x9e3779b97f4a7c15))
	}
	return hex.EncodeToString(b[:])
}
