package middleware

import (
	"github.com/hitanshpaliwal/nw-lb-go/internal/circuit"
	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RejectReasonCircuitOpen is the reason label on lb_rejected_total for a fail-fast rejection.
const RejectReasonCircuitOpen = "circuit_open"

// CircuitBreak fail-fasts a method whose recent error rate tripped its breaker, so a broken
// dependency stops consuming proxy capacity that healthy methods need. Breakers are keyed on the
// method, not the backend — the per-backend breakers live in internal/pool.
func CircuitBreak(reg *circuit.Registry, m *metrics.Metrics) grpc.StreamServerInterceptor {
	if reg == nil {
		return passThrough
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		method := methodOf(info)
		b := reg.Get(method)
		probe, admitted := b.Admit()
		if !admitted {
			if m != nil {
				m.IncRejected(method, RejectReasonCircuitOpen)
			}
			return status.Error(codes.Unavailable, "circuit open")
		}

		// Admit may have reserved a half-open probe, so every exit path — panics included — must
		// resolve it through this probe. A panic that unwound past here would leave the slot
		// outstanding forever, and once HalfOpenMax of them accumulate the method fail-fasts
		// permanently. Recovery sits outside this interceptor and converts the panic to Internal too
		// late for the breaker to see it, which is also why the panic is counted as a failure here
		// rather than left to that rule.
		defer func() {
			if r := recover(); r != nil {
				probe.Failure()
				panic(r)
			}
			if IsFailure(err) {
				probe.Failure()
			} else {
				probe.Success()
			}
		}()

		return handler(srv, ss)
	}
}

// IsFailure reports whether err should count against a circuit breaker. Only codes that indicate
// the server or the path to it is unhealthy count; a client's own bad request must never trip a
// breaker, or one misbehaving caller could fail-fast everyone else. internal/proxy applies the
// same rule to per-backend breakers, so the two must stay one function.
func IsFailure(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.ResourceExhausted, codes.Unknown:
		return true
	default:
		return false
	}
}
