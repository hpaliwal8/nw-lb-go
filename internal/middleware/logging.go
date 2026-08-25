package middleware

import (
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/hitanshpaliwal/nw-lb-go/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Logging emits exactly one record per RPC. Errors are always logged; successes are logged with
// probability cfg.SampleRate, because at the request rates this proxy targets an unsampled access
// log is the most expensive thing in the pipeline.
func Logging(log *slog.Logger, cfg config.Logging) grpc.StreamServerInterceptor {
	return logging(log, cfg, rand.Float64)
}

// logging takes the sampler so tests can drive the sampling decision instead of the RNG.
func logging(log *slog.Logger, cfg config.Logging, sample func() float64) grpc.StreamServerInterceptor {
	l := loggerOr(log)
	rate := cfg.SampleRate
	// The common configuration is "log everything"; short-circuiting keeps the RNG off the hot
	// path entirely rather than drawing a number that can only ever pass.
	logAll := rate >= 1
	dropAll := rate <= 0

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		method := methodOf(info)
		start := time.Now()

		err := handler(srv, ss)

		code := status.Code(err)
		if code == codes.OK {
			switch {
			case logAll:
			case dropAll:
				return err
			case sample() >= rate:
				return err
			}
		}

		ctx := ss.Context()
		level := levelFor(code)
		if !l.Enabled(ctx, level) {
			return err
		}

		ri, _ := FromContext(ctx)
		if ri != nil {
			if !ri.StartedAt.IsZero() {
				start = ri.StartedAt
			}
			if ri.Method != "" {
				method = ri.Method
			}
		}

		attrs := []slog.Attr{
			slog.String("request_id", ri.id()),
			slog.String("method", method),
			slog.String("code", code.String()),
			slog.Float64("duration_ms", millis(time.Since(start))),
			slog.String("backend", ri.Backend()),
			slog.Int("attempts", ri.Attempts()),
			slog.String("hash_key", ri.ResolvedHashKey()),
			slog.String("peer", PeerAddr(ctx)),
		}
		if err != nil {
			attrs = append(attrs, slog.String("error", err.Error()))
		}
		l.LogAttrs(ctx, level, "rpc", attrs...)
		return err
	}
}

func (r *RequestInfo) id() string {
	if r == nil {
		return ""
	}
	return r.ID
}

// levelFor grades a status by who is at fault: a client sending a bad argument is not an incident,
// an Internal or Unavailable is. Canceled is the client hanging up and stays at info.
func levelFor(c codes.Code) slog.Level {
	switch c {
	case codes.OK, codes.Canceled:
		return slog.LevelInfo
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied,
		codes.Unauthenticated, codes.FailedPrecondition, codes.OutOfRange:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// millis renders a duration in milliseconds at two decimals: finer resolution is noise in a log
// line and costs bytes on every record.
func millis(d time.Duration) float64 {
	return math.Round(float64(d.Nanoseconds())/1e4) / 100
}
