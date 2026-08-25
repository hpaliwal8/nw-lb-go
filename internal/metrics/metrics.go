// Package metrics owns every Prometheus collector used by the load balancer.
//
// Collectors live on a private registry so that importing this package never mutates
// prometheus.DefaultRegisterer, and so that tests can build independent Metrics values.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// LatencyBuckets are the histogram boundaries for request and upstream latency. The SLO is
// 200 ms, so the boundaries resolve finely below it and 0.2 is an exact boundary — SLO
// reporting reads the le="0.2" bucket directly.
var LatencyBuckets = []float64{
	0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05,
	0.1, 0.15, 0.2, 0.3, 0.5, 1, 2.5, 5,
}

// Metrics is safe for concurrent use by any number of goroutines; every field is an
// immutable collector handle and the underlying prometheus types are themselves concurrent.
type Metrics struct {
	reg *prometheus.Registry

	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	inflight        *prometheus.GaugeVec

	upstreamRequests *prometheus.CounterVec
	upstreamDuration *prometheus.HistogramVec

	failovers   *prometheus.CounterVec
	rateLimited *prometheus.CounterVec
	rejected    *prometheus.CounterVec
	panics      *prometheus.CounterVec

	backendHealthy   *prometheus.GaugeVec
	circuitState     *prometheus.GaugeVec
	ringMembers      prometheus.Gauge
	ringVirtualNodes prometheus.Gauge

	healthCheckDuration *prometheus.HistogramVec
}

// New builds a Metrics with all collectors registered on a fresh private registry.
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),

		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_requests_total",
			Help: "Total gRPC requests handled by the load balancer.",
		}, []string{"method", "code"}),

		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lb_request_duration_seconds",
			Help:    "End-to-end request latency observed by the load balancer.",
			Buckets: LatencyBuckets,
		}, []string{"method", "code"}),

		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lb_requests_inflight",
			Help: "Requests currently in flight.",
		}, []string{"method"}),

		upstreamRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_upstream_requests_total",
			Help: "Total requests forwarded to upstream backends.",
		}, []string{"backend", "code"}),

		upstreamDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lb_upstream_duration_seconds",
			Help:    "Upstream call latency per backend.",
			Buckets: LatencyBuckets,
		}, []string{"backend"}),

		failovers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_failovers_total",
			Help: "Retries onto a different backend.",
		}, []string{"method", "reason"}),

		rateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_rate_limited_total",
			Help: "Requests rejected by the rate limiter.",
		}, []string{"method"}),

		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_rejected_total",
			Help: "Requests rejected before reaching a backend.",
		}, []string{"method", "reason"}),

		panics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lb_panics_total",
			Help: "Panics recovered in the handler chain.",
		}, []string{"method"}),

		backendHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lb_backend_healthy",
			Help: "1 when the backend is healthy, 0 otherwise.",
		}, []string{"backend"}),

		circuitState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lb_circuit_state",
			Help: "Circuit breaker state: 0=closed, 1=open, 2=half-open.",
		}, []string{"name"}),

		ringMembers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lb_ring_members",
			Help: "Distinct members on the consistent hash ring.",
		}),

		ringVirtualNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lb_ring_virtual_nodes",
			Help: "Points on the consistent hash ring.",
		}),

		healthCheckDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lb_health_check_duration_seconds",
			Help:    "Health probe latency per backend and result.",
			Buckets: LatencyBuckets,
		}, []string{"backend", "result"}),
	}

	m.reg.MustRegister(
		m.requests,
		m.requestDuration,
		m.inflight,
		m.upstreamRequests,
		m.upstreamDuration,
		m.failovers,
		m.rateLimited,
		m.rejected,
		m.panics,
		m.backendHealthy,
		m.circuitState,
		m.ringMembers,
		m.ringVirtualNodes,
		m.healthCheckDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Registry returns the private registry so callers can register their own collectors.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves the exposition format, with OpenMetrics negotiation enabled so exemplars
// and native histograms survive a scrape from a modern Prometheus.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func (m *Metrics) ObserveRequest(method, code string, d time.Duration) {
	m.requests.WithLabelValues(method, code).Inc()
	m.requestDuration.WithLabelValues(method, code).Observe(d.Seconds())
}

func (m *Metrics) IncInflight(method string) { m.inflight.WithLabelValues(method).Inc() }

func (m *Metrics) DecInflight(method string) { m.inflight.WithLabelValues(method).Dec() }

func (m *Metrics) ObserveUpstream(backend, code string, d time.Duration) {
	m.upstreamRequests.WithLabelValues(backend, code).Inc()
	m.upstreamDuration.WithLabelValues(backend).Observe(d.Seconds())
}

func (m *Metrics) IncFailover(method, reason string) {
	m.failovers.WithLabelValues(method, reason).Inc()
}

func (m *Metrics) IncRateLimited(method string) { m.rateLimited.WithLabelValues(method).Inc() }

func (m *Metrics) IncPanic(method string) { m.panics.WithLabelValues(method).Inc() }

func (m *Metrics) IncRejected(method, reason string) {
	m.rejected.WithLabelValues(method, reason).Inc()
}

func (m *Metrics) SetBackendHealth(backend string, healthy bool) {
	v := 0.0
	if healthy {
		v = 1
	}
	m.backendHealthy.WithLabelValues(backend).Set(v)
}

// SetCircuitState takes the integer value of circuit.State (0=closed, 1=open, 2=half-open).
// The int keeps this package free of internal dependencies.
func (m *Metrics) SetCircuitState(name string, state int) {
	m.circuitState.WithLabelValues(name).Set(float64(state))
}

func (m *Metrics) SetRingMembers(n int) { m.ringMembers.Set(float64(n)) }

func (m *Metrics) SetRingVirtualNodes(n int) { m.ringVirtualNodes.Set(float64(n)) }

func (m *Metrics) ObserveHealthCheck(backend, result string, d time.Duration) {
	m.healthCheckDuration.WithLabelValues(backend, result).Observe(d.Seconds())
}
