package middleware

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hitanshpaliwal/nw-lb-go/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// maxMethodLabels bounds how many distinct method labels reach Prometheus. This proxy forwards
// whatever method name arrives on the wire, so without a cap an unauthenticated client can mint
// unbounded label series by calling /x/1, /x/2, ... and exhaust the scrape target's memory.
const maxMethodLabels = 1024

// otherMethod is the label shared by every method seen after the cap is reached.
const otherMethod = "other"

// boundedLabels caps the distinct label values emitted for a dimension. The cap is approximate
// under concurrent first-sightings, which is fine: it exists to bound growth, not to ration.
type boundedLabels struct {
	max  int
	n    atomic.Int64
	seen sync.Map // label -> struct{}
}

func newBoundedLabels(max int) *boundedLabels {
	if max < 1 {
		max = maxMethodLabels
	}
	return &boundedLabels{max: max}
}

func (b *boundedLabels) label(name string) string {
	if _, ok := b.seen.Load(name); ok {
		return name
	}
	if b.n.Load() >= int64(b.max) {
		return otherMethod
	}
	if _, loaded := b.seen.LoadOrStore(name, struct{}{}); !loaded {
		b.n.Add(1)
	}
	return name
}

// Metrics records the RED signals for every RPC. The method label is the full "/pkg.Svc/Method"
// exactly as received: this proxy forwards unknown services, so there is no schema to derive a
// tidier label from, and rewriting it would break the join with the upstream's own metrics. Only
// once maxMethodLabels distinct methods have been seen do further ones collapse to "other".
func Metrics(m *metrics.Metrics) grpc.StreamServerInterceptor {
	if m == nil {
		return passThrough
	}
	labels := newBoundedLabels(maxMethodLabels)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		method := labels.label(methodOf(info))
		start := time.Now()

		m.IncInflight(method)
		defer m.DecInflight(method)

		err := handler(srv, ss)
		// A panic skips this observation deliberately: Recovery counts it as a panic, and booking
		// it here would need a status code that no client ever saw.
		m.ObserveRequest(method, status.Code(err).String(), time.Since(start))
		return err
	}
}
