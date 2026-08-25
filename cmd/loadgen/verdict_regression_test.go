package main

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

// failFast builds n samples that all failed in negligible time, which is what a load balancer
// shedding traffic (circuit open, rate limited) actually produces.
func failFast(n int, code codes.Code) []sample {
	base := time.Now()
	out := make([]sample, 0, n)
	for i := range n {
		at := base.Add(time.Duration(i) * time.Millisecond)
		out = append(out, sample{
			scheduledAt: at,
			sentAt:      at,
			completedAt: at.Add(50 * time.Microsecond),
			code:        code,
		})
	}
	return out
}

func okSamples(n int, d time.Duration) []sample {
	base := time.Now()
	out := make([]sample, 0, n)
	for i := range n {
		at := base.Add(time.Duration(i) * time.Millisecond)
		out = append(out, sample{
			scheduledAt: at,
			sentAt:      at,
			completedAt: at.Add(d),
			code:        codes.OK,
		})
	}
	return out
}

// A run in which nothing succeeded must never report PASS. Fast failures are still failures, and
// pooling them into the latency distribution used to invert the verdict: rejecting every request
// in 50us produced a sub-millisecond p99 and printed "SLO: PASS".
func TestAllErrorsNeverPasses(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted} {
		rep := buildReport(failFast(1000, code), runParams{
			SLO: 200 * time.Millisecond, ErrorBudget: 0.001, Elapsed: time.Second,
		})
		if rep.sloPass() {
			t.Errorf("%v: sloPass() = true for a run with zero successful requests", code)
		}
		if got := rep.verdict(); !strings.Contains(got, "FAIL") {
			t.Errorf("%v: verdict() = %q, want a FAIL verdict", code, got)
		}
		if rep.ResponseTime.P99 != 0 {
			t.Errorf("%v: failed requests leaked into the latency distribution (p99 = %v)", code, rep.ResponseTime.P99)
		}
		if rep.ErrorLatency.P99 == 0 {
			t.Errorf("%v: error latency was not recorded, so failures are invisible", code)
		}
	}
}

// Errors beyond the budget fail the run even when the requests that did succeed were fast.
func TestErrorBudgetGovernsVerdict(t *testing.T) {
	samples := append(okSamples(900, time.Millisecond), failFast(100, codes.Unavailable)...)
	rep := buildReport(samples, runParams{
		SLO: 200 * time.Millisecond, ErrorBudget: 0.001, Elapsed: time.Second,
	})
	if rep.sloPass() {
		t.Fatal("sloPass() = true with a 10% error ratio against a 0.1% budget")
	}
	if got := rep.verdict(); !strings.Contains(got, "error ratio") {
		t.Errorf("verdict() = %q, want it to name the error ratio as the cause", got)
	}

	// The same latencies inside the budget pass.
	within := append(okSamples(1000, time.Millisecond), failFast(1, codes.Unavailable)...)
	repOK := buildReport(within, runParams{
		SLO: 200 * time.Millisecond, ErrorBudget: 0.01, Elapsed: time.Second,
	})
	if !repOK.sloPass() {
		t.Errorf("sloPass() = false within the error budget: %s", repOK.verdict())
	}
}

// Slow successes still fail on latency, and the reason says so.
func TestSlowSuccessesFailOnLatency(t *testing.T) {
	rep := buildReport(okSamples(1000, 500*time.Millisecond), runParams{
		SLO: 200 * time.Millisecond, ErrorBudget: 0.001, Elapsed: time.Second,
	})
	if rep.sloPass() {
		t.Fatal("sloPass() = true with a 500ms p99 against a 200ms SLO")
	}
	if got := rep.verdict(); !strings.Contains(got, "p99 response") {
		t.Errorf("verdict() = %q, want it to name the latency as the cause", got)
	}
}

// A run cut short must not present itself as a completed one. The JSON artifact outlives the
// terminal, so "interrupted" has to survive into the file and into the verdict.
func TestInterruptedRunIsMarkedAndCannotPass(t *testing.T) {
	rep := buildReport(okSamples(100, time.Millisecond), runParams{
		SLO: 200 * time.Millisecond, ErrorBudget: 0.001, Elapsed: time.Second,
		Scheduled: 60000, Interrupted: true,
	})
	if rep.sloPass() {
		t.Error("sloPass() = true for a run that was interrupted after 100 of 60000 requests")
	}
	if got := rep.verdict(); !strings.Contains(got, "interrupted") {
		t.Errorf("verdict() = %q, want it to name the interruption", got)
	}
	if table := rep.Table(); !strings.Contains(table, "INTERRUPTED") {
		t.Errorf("table does not disclose the interruption:\n%s", table)
	}

	b, err := rep.JSON()
	if err != nil {
		t.Fatalf("JSON(): %v", err)
	}
	for _, want := range []string{`"interrupted": true`, `"scheduled": 60000`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("JSON report is missing %s", want)
		}
	}
}
