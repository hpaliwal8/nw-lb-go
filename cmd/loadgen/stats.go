package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc/codes"
)

// sample is one completed request. Every timestamp the report needs is captured here at the
// moment it happens; nothing in this file touches a clock, so the whole statistics path is
// testable against hand-computed inputs.
type sample struct {
	scheduledAt time.Time // when the open-loop schedule said this request should start
	sentAt      time.Time // when a worker actually issued it
	completedAt time.Time // when the response (or the full stream) was received
	code        codes.Code
	backendID   string
	sessionKey  string
}

// serviceTime is what a naive load generator reports: how long the server took once the
// request was actually sent.
func (s sample) serviceTime() time.Duration { return s.completedAt.Sub(s.sentAt) }

// responseTime includes the time the request spent waiting for a worker. Under coordinated
// omission this is the number that moves and serviceTime is the number that lies.
func (s sample) responseTime() time.Duration { return s.completedAt.Sub(s.scheduledAt) }

// latencyStats is one timer's distribution, in milliseconds.
type latencyStats struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

// Report is the full result of a run. Field order matches the JSON output.
type Report struct {
	Label        string  `json:"label"`
	Target       string  `json:"target"`
	Mode         string  `json:"mode"`
	Method       string  `json:"method"`
	StreamCount  int     `json:"stream_count"`
	RPSRequested float64 `json:"rps_requested"`
	RPSAchieved  float64 `json:"rps_achieved"`
	DurationS    float64 `json:"duration_s"`

	// Scheduled is how many requests the run intended to issue; Total is how many completed.
	// They differ when a run is cut short, and a report that did not say so would read exactly
	// like a complete one.
	Scheduled   int  `json:"scheduled"`
	Interrupted bool `json:"interrupted"`

	Total        int            `json:"total"`
	OK           int            `json:"ok"`
	Errors       int            `json:"errors"`
	ErrorsByCode map[string]int `json:"errors_by_code"`

	// Latency covers successful RPCs only. Pooling failures in here inverts the SLO: a load
	// balancer that fast-fails everything would report a sub-millisecond p99 and pass.
	ServiceTime  latencyStats `json:"service_time"`
	ResponseTime latencyStats `json:"response_time"`
	// ErrorLatency keeps failures visible rather than merely excluded.
	ErrorLatency latencyStats `json:"error_latency"`

	SLOMs            float64 `json:"slo_ms"`
	SLOViolations    int     `json:"slo_violations"`
	SLOFractionUnder float64 `json:"slo_fraction_under"`
	// ErrorBudget is the fraction of failed RPCs tolerated before the run fails outright.
	ErrorBudget float64 `json:"error_budget"`
	ErrorRatio  float64 `json:"error_ratio"`

	BackendDistribution      map[string]int `json:"backend_distribution"`
	DistinctKeys             int            `json:"distinct_keys"`
	KeysWithMultipleBackends int            `json:"keys_with_multiple_backends"`

	Conns        int `json:"conns"`
	Concurrency  int `json:"concurrency"`
	PayloadBytes int `json:"payload_bytes"`
	DelayMs      int `json:"delay_ms"`
}

// runParams carries everything the report needs that is not derivable from the samples.
type runParams struct {
	Label        string
	Target       string
	Mode         string
	Method       string
	StreamCount  int
	RPSRequested float64
	Elapsed      time.Duration
	Scheduled    int
	Interrupted  bool
	SLO          time.Duration
	ErrorBudget  float64
	Conns        int
	Concurrency  int
	PayloadBytes int
	DelayMs      int
}

// percentileIndex returns the zero-based nearest-rank index for percentile p over n samples,
// i.e. ceil(p/100*n)-1 clamped into [0,n-1], or -1 when there is no sample.
//
// The rank is computed in integer arithmetic. Evaluating p/100*n in float64 lands a hair either
// side of a whole number for values like 99.9, which moves the reported rank by a full sample —
// exactly where the tail percentiles that decide the SLO verdict live.
func percentileIndex(n int, p float64) int {
	if n <= 0 {
		return -1
	}
	if math.IsNaN(p) || p <= 0 {
		return 0
	}
	if p >= 100 {
		return n - 1
	}
	// p as permyriad: 99.9% -> 999000 parts per million of the sample count.
	pm := int64(math.Round(p * 10000))
	rank := (pm*int64(n) + 999999) / 1000000
	idx := int(rank) - 1
	return min(max(idx, 0), n-1)
}

// percentile returns the nearest-rank percentile of an already sorted slice. Nearest rank always
// reports a latency some request actually experienced; an interpolating estimator reports a
// number nothing ever measured. Returns 0 for an empty sample.
func percentile(sorted []time.Duration, p float64) time.Duration {
	i := percentileIndex(len(sorted), p)
	if i < 0 {
		return 0
	}
	return sorted[i]
}

// latencyStatsOf summarises an ascending-sorted slice of durations.
func latencyStatsOf(sorted []time.Duration) latencyStats {
	if len(sorted) == 0 {
		return latencyStats{}
	}
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	return latencyStats{
		P50:  msRound(percentile(sorted, 50)),
		P90:  msRound(percentile(sorted, 90)),
		P95:  msRound(percentile(sorted, 95)),
		P99:  msRound(percentile(sorted, 99)),
		P999: msRound(percentile(sorted, 99.9)),
		Max:  msRound(sorted[len(sorted)-1]),
		Mean: round(float64(sum)/float64(len(sorted))/float64(time.Millisecond), 3),
	}
}

// mergeSamples flattens the per-worker slices into one. Workers never share a slice during the
// run, so the merge happens exactly once, here, after every worker has stopped.
func mergeSamples(perWorker [][]sample) []sample {
	total := 0
	for _, w := range perWorker {
		total += len(w)
	}
	out := make([]sample, 0, total)
	for _, w := range perWorker {
		out = append(out, w...)
	}
	return out
}

func buildReport(samples []sample, p runParams) Report {
	rep := Report{
		Label:               p.Label,
		Target:              p.Target,
		Mode:                p.Mode,
		Method:              p.Method,
		StreamCount:         p.StreamCount,
		RPSRequested:        round(p.RPSRequested, 3),
		DurationS:           round(p.Elapsed.Seconds(), 3),
		Scheduled:           p.Scheduled,
		Interrupted:         p.Interrupted,
		Total:               len(samples),
		ErrorsByCode:        make(map[string]int),
		SLOMs:               msRound(p.SLO),
		ErrorBudget:         p.ErrorBudget,
		BackendDistribution: make(map[string]int),
		Conns:               p.Conns,
		Concurrency:         p.Concurrency,
		PayloadBytes:        p.PayloadBytes,
		DelayMs:             p.DelayMs,
	}

	service := make([]time.Duration, 0, len(samples))
	response := make([]time.Duration, 0, len(samples))
	keys := make(map[string]struct{})
	keyBackend := make(map[string]string)
	multiBackend := make(map[string]struct{})

	errLatency := make([]time.Duration, 0)

	for _, s := range samples {
		rt := s.responseTime()

		if s.code == codes.OK {
			rep.OK++
			service = append(service, s.serviceTime())
			response = append(response, rt)
			if rt > p.SLO {
				rep.SLOViolations++
			}
		} else {
			rep.Errors++
			rep.ErrorsByCode[s.code.String()]++
			errLatency = append(errLatency, rt)
		}
		if s.backendID != "" {
			rep.BackendDistribution[s.backendID]++
		}
		if s.sessionKey == "" {
			continue
		}
		keys[s.sessionKey] = struct{}{}
		if s.backendID == "" {
			continue
		}
		if prev, seen := keyBackend[s.sessionKey]; !seen {
			keyBackend[s.sessionKey] = s.backendID
		} else if prev != s.backendID {
			multiBackend[s.sessionKey] = struct{}{}
		}
	}

	rep.DistinctKeys = len(keys)
	rep.KeysWithMultipleBackends = len(multiBackend)

	slices.Sort(service)
	slices.Sort(response)
	slices.Sort(errLatency)
	rep.ServiceTime = latencyStatsOf(service)
	rep.ResponseTime = latencyStatsOf(response)
	rep.ErrorLatency = latencyStatsOf(errLatency)

	if n := rep.OK; n > 0 {
		// Six decimals, not three: at 5000 requests a single violation is 0.0002 of the run and
		// would round away entirely.
		rep.SLOFractionUnder = round(float64(n-rep.SLOViolations)/float64(n), 6)
	}
	if rep.Total > 0 {
		rep.ErrorRatio = round(float64(rep.Errors)/float64(rep.Total), 6)
	}
	if secs := p.Elapsed.Seconds(); secs > 0 {
		rep.RPSAchieved = round(float64(len(samples))/secs, 3)
	}
	return rep
}

// sloPass compares p99 response time against the SLO over SUCCESSFUL requests, and additionally
// requires the run to have served traffic within its error budget. Latency alone is not a
// sufficient verdict: fast failures are still failures, and a system rejecting everything in 50µs
// must not be reported as meeting a latency target.
func (r Report) sloPass() bool {
	return r.failReason() == ""
}

// failReason returns why the run failed its SLO, or "" if it passed.
func (r Report) failReason() string {
	switch {
	case r.Total == 0:
		return "no requests"
	case r.Interrupted:
		return fmt.Sprintf("run interrupted after %d of %d scheduled requests", r.Total, r.Scheduled)
	case r.OK == 0:
		return "no successful requests"
	case r.ErrorRatio > r.ErrorBudget:
		return fmt.Sprintf("error ratio %.2f%% exceeds budget %.2f%%", r.ErrorRatio*100, r.ErrorBudget*100)
	case r.ResponseTime.P99 > r.SLOMs:
		return fmt.Sprintf("p99 response %.3f ms exceeds SLO %.3f ms", r.ResponseTime.P99, r.SLOMs)
	default:
		return ""
	}
}

func (r Report) verdict() string {
	if reason := r.failReason(); reason != "" {
		return "SLO: FAIL (" + reason + ")"
	}
	return "SLO: PASS"
}

// JSON renders the report as indented JSON with a trailing newline.
func (r Report) JSON() ([]byte, error) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Table renders the human-readable summary. The last line is the verdict.
func (r Report) Table() string {
	var b strings.Builder

	fmt.Fprintf(&b, "=== %s ===\n", r.Label)
	tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "target\t%s\n", r.Target)
	fmt.Fprintf(tw, "mode\t%s, method %s", r.Mode, r.Method)
	if r.Method == methodStream {
		fmt.Fprintf(tw, " (%d messages)", r.StreamCount)
	}
	fmt.Fprintln(tw)
	fmt.Fprintf(tw, "load\t%d conns, %d workers, %d B payload, %d ms server delay\n",
		r.Conns, r.Concurrency, r.PayloadBytes, r.DelayMs)
	fmt.Fprintf(tw, "duration\t%.3f s\n", r.DurationS)
	if r.Mode == modeClosed {
		fmt.Fprintf(tw, "rps\tn/a requested (closed loop), %.3f achieved\n", r.RPSAchieved)
	} else {
		fmt.Fprintf(tw, "rps\t%.3f requested, %.3f achieved\n", r.RPSRequested, r.RPSAchieved)
	}
	if r.Interrupted || (r.Scheduled > 0 && r.Total < r.Scheduled) {
		fmt.Fprintf(tw, "requests\t%d total, %d ok, %d errors (INTERRUPTED: %d of %d scheduled completed)\n",
			r.Total, r.OK, r.Errors, r.Total, r.Scheduled)
	} else {
		fmt.Fprintf(tw, "requests\t%d total, %d ok, %d errors\n", r.Total, r.OK, r.Errors)
	}
	fmt.Fprintf(tw, "errors\t%s\n", formatCounts(r.ErrorsByCode))
	fmt.Fprintf(tw, "backends\t%s\n", formatCounts(r.BackendDistribution))
	fmt.Fprintf(tw, "keys\t%d distinct, %d with more than one backend\n",
		r.DistinctKeys, r.KeysWithMultipleBackends)
	tw.Flush()

	b.WriteByte('\n')
	lat := tabwriter.NewWriter(&b, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprint(lat, "timer (ms)\tp50\tp90\tp95\tp99\tp99.9\tmax\tmean\t\n")
	writeLatencyRow(lat, "service", r.ServiceTime)
	writeLatencyRow(lat, "response", r.ResponseTime)
	if r.Errors > 0 {
		writeLatencyRow(lat, "errors", r.ErrorLatency)
	}
	lat.Flush()

	b.WriteByte('\n')
	fmt.Fprintf(&b, "%s\n", r.verdict())
	fmt.Fprintf(&b, "  latency  p99 response %.3f ms vs SLO %.3f ms; %d/%d ok requests under SLO (%.4f%%), %d violations\n",
		r.ResponseTime.P99, r.SLOMs, r.OK-r.SLOViolations, r.OK,
		r.SLOFractionUnder*100, r.SLOViolations)
	fmt.Fprintf(&b, "  errors   %d/%d (%.4f%%) vs budget %.4f%%\n",
		r.Errors, r.Total, r.ErrorRatio*100, r.ErrorBudget*100)
	if r.Total == 0 {
		b.WriteString("no samples were recorded\n")
	}
	return b.String()
}

func writeLatencyRow(w *tabwriter.Writer, name string, s latencyStats) {
	fmt.Fprintf(w, "%s\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t\n",
		name, s.P50, s.P90, s.P95, s.P99, s.P999, s.Max, s.Mean)
}

// formatCounts renders a count map in key order so two runs of the same shape print identically.
func formatCounts(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, "  ")
}

func msRound(d time.Duration) float64 {
	return round(float64(d)/float64(time.Millisecond), 3)
}

func round(f float64, decimals int) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	scale := math.Pow(10, float64(decimals))
	return math.Round(f*scale) / scale
}
