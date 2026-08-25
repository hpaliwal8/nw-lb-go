package middleware

import (
	"context"
	"log/slog"
	"runtime/debug"

	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Recovery turns a panic anywhere below it into codes.Internal. It must be the outermost
// interceptor: a panic that escapes it takes the whole process down with it.
//
// The recovered value and stack are logged but never sent to the client — a panic message can
// carry internals a caller has no business seeing.
func Recovery(log *slog.Logger, m *metrics.Metrics) grpc.StreamServerInterceptor {
	l := loggerOr(log)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			method := methodOf(info)
			if m != nil {
				m.IncPanic(method)
			}
			ctx := ss.Context()
			attrs := []slog.Attr{slog.String("method", method)}
			// Recovery sits outside Context, so its stream only carries a RequestInfo when a
			// caller nested the two the other way round.
			if id := requestIDOf(ctx); id != "" {
				attrs = append(attrs, slog.String("request_id", id))
			}
			// debug.Stack runs while the panicking frames are still live, so the trace points at
			// the failure rather than at this deferred function.
			attrs = append(attrs,
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
			)
			l.LogAttrs(ctx, slog.LevelError, "panic recovered", attrs...)
			err = status.Error(codes.Internal, "internal error")
		}()
		return handler(srv, ss)
	}
}

func loggerOr(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

func requestIDOf(ctx context.Context) string {
	if info, ok := FromContext(ctx); ok {
		return info.ID
	}
	return ""
}
