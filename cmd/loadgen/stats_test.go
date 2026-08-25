package main

import (
	"encoding/json"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

// ms builds an ascending sample from whole-millisecond values.
func ms(vals ...int) []time.Duration {
	out := make([]time.Duration, len(vals))
	for i, v := range vals {
		out[i] = time.Duration(v) * time.Millisecond
	}
	return out
}

// seqMs is 1ms, 2ms, ... nms: with these values the nearest-rank percentile equals the rank
// itself, so every expectation below can be checked by hand.
func seqMs(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(i+1) * time.Millisecond
	}
	return out
}

func mkSample(schedMs, sentMs, doneMs int, code codes.Code, backend, key string) sample {
	base := time.Unix(0, 0).UTC()
	return sample{
		scheduledAt: base.Add(time.Duration(schedMs) * time.Millisecond),
		sentAt:      base.Add(time.Duration(sentMs) * time.Millisecond),
		completedAt: base.Add(time.Duration(doneMs) * time.Millisecond),
		code:        code,
		backendID:   backend,
		sessionKey:  key,
	}
}

func TestPercentileNearestRank(t *testing.T) {
	tests := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		// n=1: every percentile is the only observation.
		{"n1/p50", ms(7), 50, 7 * time.Millisecond},
		{"n1/p90", ms(7), 90, 7 * time.Millisecond},
		{"n1/p95", ms(7), 95, 7 * time.Millisecond},
		{"n1/p99", ms(7), 99, 7 * time.Millisecond},
		{"n1/p999", ms(7), 99.9, 7 * time.Millisecond},
		{"n1/p100", ms(7), 100, 7 * time.Millisecond},

		// n=2: rank(50) = ceil(1.0) = 1, rank(90) = ceil(1.8) = 2.
		{"n2/p50", ms(10, 20), 50, 10 * time.Millisecond},
		{"n2/p90", ms(10, 20), 90, 20 * time.Millisecond},
		{"n2/p95", ms(10, 20), 95, 20 * time.Millisecond},
		{"n2/p99", ms(10, 20), 99, 20 * time.Millisecond},
		{"n2/p999", ms(10, 20), 99.9, 20 * time.Millisecond},
		{"n2/p100", ms(10, 20), 100, 20 * time.Millisecond},

		// n=3: rank(50) = ceil(1.5) = 2, rank(90) = ceil(2.7) = 3.
		{"n3/p50", ms(10, 20, 30), 50, 20 * time.Millisecond},
		{"n3/p90", ms(10, 20, 30), 90, 30 * time.Millisecond},

		// n=100, values 1..100ms: rank(p) = p, except p99.9 -> ceil(99.9) = 100.
		{"n100/p50", seqMs(100), 50, 50 * time.Millisecond},
		{"n100/p90", seqMs(100), 90, 90 * time.Millisecond},
		{"n100/p95", seqMs(100), 95, 95 * time.Millisecond},
		{"n100/p99", seqMs(100), 99, 99 * time.Millisecond},
		{"n100/p999", seqMs(100), 99.9, 100 * time.Millisecond},
		{"n100/p100", seqMs(100), 100, 100 * time.Millisecond},

		// n=1000, values 1..1000ms: rank(p) = 10p, and rank(99.9) = 999 exactly.
		{"n1000/p50", seqMs(1000), 50, 500 * time.Millisecond},
		{"n1000/p90", seqMs(1000), 90, 900 * time.Millisecond},
		{"n1000/p95", seqMs(1000), 95, 950 * time.Millisecond},
		{"n1000/p99", seqMs(1000), 99, 990 * time.Millisecond},
		{"n1000/p999", seqMs(1000), 99.9, 999 * time.Millisecond},
		{"n1000/p100", seqMs(1000), 100, 1000 * time.Millisecond},

		// Out-of-range percentiles clamp instead of indexing off the ends.
		{"n100/p0", seqMs(100), 0, 1 * time.Millisecond},
		{"n100/negative", seqMs(100), -5, 1 * time.Millisecond},
		{"n100/over100", seqMs(100), 250, 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := slices.Clone(tt.sorted)
			got := percentile(tt.sorted, tt.p)
			if got != tt.want {
				t.Errorf("percentile(n=%d, p=%v) = %v, want %v", len(tt.sorted), tt.p, got, tt.want)
			}
			if !slices.Equal(tt.sorted, before) {
				t.Error("percentile mutated its input")
			}
		})
	}
}

func TestPercentileEmptySample(t *testing.T) {
	for _, p := range []float64{-1, 0, 50, 99, 99.9, 100, 1000, math.NaN()} {
		if got := percentile(nil, p); got != 0 {
			t.Errorf("percentile(nil, %v) = %v, want 0", p, got)
		}
		if got := percentile([]time.Duration{}, p); got != 0 {
			t.Errorf("percentile(empty, %v) = %v, want 0", p, got)
		}
		if got := percentileIndex(0, p); got != -1 {
			t.Errorf("percentileIndex(0, %v) = %d, want -1", p, got)
		}
	}
	if got := latencyStatsOf(nil); got != (latencyStats{}) {
		t.Errorf("latencyStatsOf(nil) = %+v, want zero value", got)
	}
}

func TestLatencyStatsOf(t *testing.T) {
	tests := []struct {
		name   string
		sorted []time.Duration
		want   latencyStats
	}{
		{
			name:   "n1000",
			sorted: seqMs(1000),
			// mean of 1..1000 ms = 500.5 ms.
			want: latencyStats{P50: 500, P90: 900, P95: 950, P99: 990, P999: 999, Max: 1000, Mean: 500.5},
		},
		{
			name:   "n4",
			sorted: ms(10, 20, 40, 300),
			want:   latencyStats{P50: 20, P90: 300, P95: 300, P99: 300, P999: 300, Max: 300, Mean: 92.5},
		},
		{
			name:   "sub-millisecond values round to three decimals",
			sorted: []time.Duration{1234567 * time.Nanosecond, 2 * time.Millisecond},
			want:   latencyStats{P50: 1.235, P90: 2, P95: 2, P99: 2, P999: 2, Max: 2, Mean: 1.617},
		},
		{
			name:   "empty",
			sorted: nil,
			want:   latencyStats{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := latencyStatsOf(tt.sorted); got != tt.want {
				t.Errorf("latencyStatsOf() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestMergeSamplesPreservesCount(t *testing.T) {
	tests := []struct {
		name      string
		perWorker [][]sample
		wantKeys  []string
	}{
		{name: "no workers", perWorker: nil},
		{name: "all workers empty", perWorker: [][]sample{{}, nil, {}}},
		{
			name: "uneven workers",
			perWorker: [][]sample{
				{mkSample(0, 0, 1, codes.OK, "a", "k0"), mkSample(0, 0, 2, codes.OK, "a", "k1")},
				nil,
				{mkSample(0, 0, 3, codes.OK, "b", "k2")},
				{mkSample(0, 0, 4, codes.OK, "b", "k3"), mkSample(0, 0, 5, codes.OK, "b", "k4"), mkSample(0, 0, 6, codes.OK, "b", "k5")},
			},
			wantKeys: []string{"k0", "k1", "k2", "k3", "k4", "k5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := 0
			for _, w := range tt.perWorker {
				want += len(w)
			}
			got := mergeSamples(tt.perWorker)
			if len(got) != want {
				t.Fatalf("mergeSamples() len = %d, want %d", len(got), want)
			}
			var keys []string
			for _, s := range got {
				keys = append(keys, s.sessionKey)
			}
			if !slices.Equal(keys, tt.wantKeys) {
				t.Errorf("merged keys = %v, want %v", keys, tt.wantKeys)
			}
			if want > 0 && buildReport(got, runParams{SLO: time.Second}).Total != want {
				t.Errorf("report total does not match merged sample count")
			}
		})
	}
}

func TestBuildReportErrorsByCode(t *testing.T) {
	tests := []struct {
		name       string
		samples    []sample
		wantOK     int
		wantErrors int
		wantByCode map[string]int
	}{
		{
			name:       "no samples",
			samples:    nil,
			wantByCode: map[string]int{},
		},
		{
			name: "all ok",
			samples: []sample{
				mkSample(0, 0, 1, codes.OK, "a", ""),
				mkSample(0, 0, 2, codes.OK, "a", ""),
			},
			wantOK:     2,
			wantByCode: map[string]int{},
		},
		{
			name: "mixed codes",
			samples: []sample{
				mkSample(0, 0, 1, codes.OK, "a", ""),
				mkSample(0, 0, 2, codes.Unavailable, "", ""),
				mkSample(0, 0, 3, codes.Unavailable, "", ""),
				mkSample(0, 0, 4, codes.DeadlineExceeded, "", ""),
				mkSample(0, 0, 5, codes.ResourceExhausted, "", ""),
				mkSample(0, 0, 6, codes.OK, "b", ""),
			},
			wantOK:     2,
			wantErrors: 4,
			wantByCode: map[string]int{
				"Unavailable":       2,
				"DeadlineExceeded":  1,
				"ResourceExhausted": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := buildReport(tt.samples, runParams{SLO: time.Second})
			if rep.OK != tt.wantOK || rep.Errors != tt.wantErrors {
				t.Errorf("ok=%d errors=%d, want ok=%d errors=%d", rep.OK, rep.Errors, tt.wantOK, tt.wantErrors)
			}
			if rep.Total != tt.wantOK+tt.wantErrors {
				t.Errorf("total = %d, want %d", rep.Total, tt.wantOK+tt.wantErrors)
			}
			if len(rep.ErrorsByCode) != len(tt.wantByCode) {
				t.Fatalf("errors_by_code = %v, want %v", rep.ErrorsByCode, tt.wantByCode)
			}
			for code, want := range tt.wantByCode {
				if rep.ErrorsByCode[code] != want {
					t.Errorf("errors_by_code[%s] = %d, want %d", code, rep.ErrorsByCode[code], want)
				}
			}
		})
	}
}

func TestBuildReportStickiness(t *testing.T) {
	tests := []struct {
		name         string
		samples      []sample
		wantKeys     int
		wantMulti    int
		wantBackends map[string]int
	}{
		{
			name: "every key sticks to one backend",
			samples: []sample{
				mkSample(0, 0, 1, codes.OK, "backend-a", "key-0"),
				mkSample(0, 0, 1, codes.OK, "backend-a", "key-0"),
				mkSample(0, 0, 1, codes.OK, "backend-b", "key-1"),
			},
			wantKeys:     2,
			wantMulti:    0,
			wantBackends: map[string]int{"backend-a": 2, "backend-b": 1},
		},
		{
			name: "one key saw two backends",
			samples: []sample{
				mkSample(0, 0, 1, codes.OK, "backend-a", "key-0"),
				mkSample(0, 0, 1, codes.OK, "backend-b", "key-0"),
				mkSample(0, 0, 1, codes.OK, "backend-b", "key-0"),
				mkSample(0, 0, 1, codes.OK, "backend-c", "key-1"),
			},
			wantKeys:     2,
			wantMulti:    1,
			wantBackends: map[string]int{"backend-a": 1, "backend-b": 2, "backend-c": 1},
		},
		{
			name: "two keys each saw two backends",
			samples: []sample{
				mkSample(0, 0, 1, codes.OK, "backend-a", "key-0"),
				mkSample(0, 0, 1, codes.OK, "backend-b", "key-0"),
				mkSample(0, 0, 1, codes.OK, "backend-a", "key-1"),
				mkSample(0, 0, 1, codes.OK, "backend-c", "key-1"),
			},
			wantKeys:     2,
			wantMulti:    2,
			wantBackends: map[string]int{"backend-a": 2, "backend-b": 1, "backend-c": 1},
		},
		{
			name: "a failed request carries no backend and cannot break stickiness",
			samples: []sample{
				mkSample(0, 0, 1, codes.OK, "backend-a", "key-0"),
				mkSample(0, 0, 1, codes.Unavailable, "", "key-0"),
			},
			wantKeys:     1,
			wantMulti:    0,
			wantBackends: map[string]int{"backend-a": 1},
		},
		{
			name: "no affinity header means no keys",
			samples: []sample{
				mkSample(0, 0, 1, codes.OK, "backend-a", ""),
				mkSample(0, 0, 1, codes.OK, "backend-b", ""),
			},
			wantKeys:     0,
			wantMulti:    0,
			wantBackends: map[string]int{"backend-a": 1, "backend-b": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := buildReport(tt.samples, runParams{SLO: time.Second})
			if rep.DistinctKeys != tt.wantKeys {
				t.Errorf("distinct_keys = %d, want %d", rep.DistinctKeys, tt.wantKeys)
			}
			if rep.KeysWithMultipleBackends != tt.wantMulti {
				t.Errorf("keys_with_multiple_backends = %d, want %d", rep.KeysWithMultipleBackends, tt.wantMulti)
			}
			if len(rep.BackendDistribution) != len(tt.wantBackends) {
				t.Fatalf("backend_distribution = %v, want %v", rep.BackendDistribution, tt.wantBackends)
			}
			for id, want := range tt.wantBackends {
				if rep.BackendDistribution[id] != want {
					t.Errorf("backend_distribution[%s] = %d, want %d", id, rep.BackendDistribution[id], want)
				}
			}
		})
	}
}

// fourSamples has response times 11, 20, 40 and 300 ms and service times 10, 20, 40 and 295 ms.
func fourSamples() []sample {
	return []sample{
		mkSample(0, 1, 11, codes.OK, "backend-a", "key-0"),
		mkSample(0, 0, 20, codes.OK, "backend-a", "key-0"),
		mkSample(0, 5, 300, codes.Unavailable, "", "key-1"),
		mkSample(0, 0, 40, codes.OK, "backend-b", "key-1"),
	}
}

func TestBuildReportTimersAndSLO(t *testing.T) {
	rep := buildReport(fourSamples(), runParams{
		Label:        "bench",
		Target:       "127.0.0.1:8080",
		Mode:         modeOpen,
		Method:       methodUnary,
		RPSRequested: 100,
		Elapsed:      2 * time.Second,
		SLO:          100 * time.Millisecond,
		Conns:        2,
		Concurrency:  4,
		PayloadBytes: 16,
		DelayMs:      3,
	})

	// Latency covers successful requests only: service times 10, 20, 40 and response times
	// 11, 20, 40. The failed request's 295/300 ms belongs to error_latency, not here — pooling it
	// in would let a run that failed fast report a better p99 than one that served traffic.
	wantService := latencyStats{P50: 20, P90: 40, P95: 40, P99: 40, P999: 40, Max: 40, Mean: 23.333}
	wantResponse := latencyStats{P50: 20, P90: 40, P95: 40, P99: 40, P999: 40, Max: 40, Mean: 23.667}
	wantErrors := latencyStats{P50: 300, P90: 300, P95: 300, P99: 300, P999: 300, Max: 300, Mean: 300}
	if rep.ServiceTime != wantService {
		t.Errorf("service_time = %+v, want %+v", rep.ServiceTime, wantService)
	}
	if rep.ResponseTime != wantResponse {
		t.Errorf("response_time = %+v, want %+v", rep.ResponseTime, wantResponse)
	}
	if rep.ErrorLatency != wantErrors {
		t.Errorf("error_latency = %+v, want %+v", rep.ErrorLatency, wantErrors)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"total", rep.Total, 4},
		{"ok", rep.OK, 3},
		{"errors", rep.Errors, 1},
		{"slo_ms", rep.SLOMs, 100.0},
		{"slo_violations", rep.SLOViolations, 0},
		{"slo_fraction_under", rep.SLOFractionUnder, 1.0},
		{"error_ratio", rep.ErrorRatio, 0.25},
		{"rps_requested", rep.RPSRequested, 100.0},
		{"rps_achieved", rep.RPSAchieved, 2.0},
		{"duration_s", rep.DurationS, 2.0},
		{"distinct_keys", rep.DistinctKeys, 2},
		{"keys_with_multiple_backends", rep.KeysWithMultipleBackends, 0},
		{"conns", rep.Conns, 2},
		{"concurrency", rep.Concurrency, 4},
		{"payload_bytes", rep.PayloadBytes, 16},
		{"delay_ms", rep.DelayMs, 3},
		{"label", rep.Label, "bench"},
		{"target", rep.Target, "127.0.0.1:8080"},
		{"mode", rep.Mode, modeOpen},
		{"method", rep.Method, methodUnary},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestBuildReportSLOFractionKeepsResolution(t *testing.T) {
	// One violation in 5000 requests is 0.0002 of the run: rounding the fraction to three
	// decimals would report a perfect score for a run that missed the SLO.
	samples := make([]sample, 0, 5000)
	for i := range 4999 {
		samples = append(samples, mkSample(0, 0, 1+i%5, codes.OK, "backend-a", ""))
	}
	samples = append(samples, mkSample(0, 0, 500, codes.OK, "backend-a", ""))

	rep := buildReport(samples, runParams{SLO: 100 * time.Millisecond, Elapsed: time.Second})
	if rep.SLOViolations != 1 {
		t.Fatalf("slo_violations = %d, want 1", rep.SLOViolations)
	}
	if rep.SLOFractionUnder != 0.9998 {
		t.Errorf("slo_fraction_under = %v, want 0.9998", rep.SLOFractionUnder)
	}
	if !rep.sloPass() {
		t.Error("p99 response time is under the SLO, so the verdict must be PASS")
	}
}

func TestReportVerdict(t *testing.T) {
	tests := []struct {
		name    string
		samples []sample
		slo     time.Duration
		want    string
	}{
		// Thresholds are anchored to the SUCCESSFUL samples (11/20/40 ms, so p99 = 40 ms); the
		// 300 ms failure is deliberately not part of the latency distribution.
		{"p99 over the SLO", fourSamples(), 30 * time.Millisecond, "SLO: FAIL"},
		{"p99 under the SLO", fourSamples(), 500 * time.Millisecond, "SLO: PASS"},
		{"p99 exactly at the SLO", fourSamples(), 40 * time.Millisecond, "SLO: PASS"},
		{"no samples never passes", nil, time.Second, "SLO: FAIL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// ErrorBudget 1 isolates the latency dimension under test; the error-ratio rule has
			// its own coverage in verdict_regression_test.go.
			rep := buildReport(tt.samples, runParams{
				SLO: tt.slo, Elapsed: time.Second, Method: methodUnary, ErrorBudget: 1,
			})
			if got := rep.verdict(); !strings.HasPrefix(got, tt.want) {
				t.Errorf("verdict = %q, want it to start with %q", got, tt.want)
			}
			table := rep.Table()
			if !strings.Contains(table, tt.want) {
				t.Errorf("table does not state the verdict %q:\n%s", tt.want, table)
			}
			other := "SLO: PASS"
			if tt.want == other {
				other = "SLO: FAIL"
			}
			if strings.Contains(table, other) {
				t.Errorf("table states both verdicts:\n%s", table)
			}
		})
	}
}

func TestReportEmptyRunIsSafe(t *testing.T) {
	rep := buildReport(nil, runParams{Label: "empty", SLO: 200 * time.Millisecond, Method: methodStream, StreamCount: 5})

	if rep.Total != 0 || rep.OK != 0 || rep.Errors != 0 {
		t.Errorf("counts = %d/%d/%d, want zeros", rep.Total, rep.OK, rep.Errors)
	}
	if rep.RPSAchieved != 0 || rep.SLOFractionUnder != 0 {
		t.Errorf("rates = %v/%v, want zeros", rep.RPSAchieved, rep.SLOFractionUnder)
	}
	for name, v := range map[string]float64{
		"service p99":  rep.ServiceTime.P99,
		"response p99": rep.ResponseTime.P99,
		"service mean": rep.ServiceTime.Mean,
	} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Errorf("%s = %v, want a finite number", name, v)
		}
	}
	if rep.ErrorsByCode == nil || rep.BackendDistribution == nil {
		t.Error("maps must be non-nil so the JSON report has {} rather than null")
	}
	if table := rep.Table(); !strings.Contains(table, "no samples") {
		t.Errorf("table must say the run recorded nothing:\n%s", table)
	}
}

func TestReportJSONShape(t *testing.T) {
	rep := buildReport(fourSamples(), runParams{SLO: 100 * time.Millisecond, Elapsed: time.Second})
	b, err := rep.JSON()
	if err != nil {
		t.Fatalf("JSON() error: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("report is not valid JSON: %v", err)
	}
	wantFields := []string{
		"label", "target", "mode", "method", "stream_count", "rps_requested", "rps_achieved",
		"duration_s", "scheduled", "interrupted",
		"total", "ok", "errors", "errors_by_code", "service_time", "response_time",
		"error_latency", "slo_ms", "slo_violations", "slo_fraction_under", "error_budget",
		"error_ratio", "backend_distribution", "distinct_keys",
		"keys_with_multiple_backends", "conns", "concurrency", "payload_bytes", "delay_ms",
	}
	for _, f := range wantFields {
		if _, ok := decoded[f]; !ok {
			t.Errorf("report is missing field %q", f)
		}
	}
	if len(decoded) != len(wantFields) {
		t.Errorf("report has %d fields, want %d", len(decoded), len(wantFields))
	}

	for _, timer := range []string{"service_time", "response_time"} {
		var lat map[string]float64
		if err := json.Unmarshal(decoded[timer], &lat); err != nil {
			t.Fatalf("%s is not an object of numbers: %v", timer, err)
		}
		for _, f := range []string{"p50", "p90", "p95", "p99", "p999", "max", "mean"} {
			if _, ok := lat[f]; !ok {
				t.Errorf("%s is missing field %q", timer, f)
			}
		}
		if len(lat) != 7 {
			t.Errorf("%s has %d fields, want 7", timer, len(lat))
		}
	}
}

func TestRoundToThreeDecimals(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want float64
	}{
		{"whole millisecond", 5 * time.Millisecond, 5},
		{"rounds up", 1234567 * time.Nanosecond, 1.235},
		{"rounds down", 1234400 * time.Nanosecond, 1.234},
		{"sub-microsecond", 400 * time.Nanosecond, 0},
		{"microsecond", 1500 * time.Nanosecond, 0.002},
		{"zero", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := msRound(tt.in); got != tt.want {
				t.Errorf("msRound(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
	if got := round(math.NaN(), 3); got != 0 {
		t.Errorf("round(NaN) = %v, want 0", got)
	}
	if got := round(math.Inf(1), 3); got != 0 {
		t.Errorf("round(+Inf) = %v, want 0", got)
	}
}

func TestFormatCountsIsOrdered(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]int
		want string
	}{
		{"empty", nil, "none"},
		{"single", map[string]int{"backend-a": 3}, "backend-a=3"},
		{
			name: "sorted by key",
			in:   map[string]int{"backend-c": 1, "backend-a": 3, "backend-b": 2},
			want: "backend-a=3  backend-b=2  backend-c=1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 5 {
				if got := formatCounts(tt.in); got != tt.want {
					t.Fatalf("formatCounts() = %q, want %q", got, tt.want)
				}
			}
		})
	}
}
