package middleware

import (
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"github.com/hitanshpaliwal/nw-lb-go/internal/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RateLimit sheds load before the handler runs, so a rejected request costs a token-bucket check
// and nothing else — no upstream connection, no buffered message.
func RateLimit(l *ratelimit.Limiter, m *metrics.Metrics) grpc.StreamServerInterceptor {
	if l == nil {
		return passThrough
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Resolving a client key costs a peer lookup and a host/port split, so only pay for it
		// when the limiter actually consults it.
		key := ""
		if l.PerClient() {
			key = ClientKey(ss.Context())
		}
		if !l.Allow(key) {
			if m != nil {
				m.IncRateLimited(methodOf(info))
			}
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(srv, ss)
	}
}
