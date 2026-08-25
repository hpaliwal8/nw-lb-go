// Package circuit implements a rolling-window circuit breaker with three states.
package circuit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type State int32

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("unknown(%d)", int32(s))
	}
}

type Settings struct {
	Window       time.Duration
	Buckets      int
	MinRequests  int
	FailureRatio float64
	OpenTimeout  time.Duration
	HalfOpenMax  int
	// MaxEntries bounds how many distinct breakers a Registry will create. A gRPC proxy forwards
	// whatever method name arrives on the wire, so an unauthenticated client can otherwise mint
	// unbounded breakers by calling /x/1, /x/2, ... Defaults to DefaultMaxEntries.
	MaxEntries    int
	Now           func() time.Time                  // nil => time.Now
	OnStateChange func(name string, from, to State) // may be nil
}

// normalize makes a zero or partially filled Settings usable. OpenTimeout is deliberately not
// defaulted: zero means an open breaker becomes half-open on the next call, which is a legitimate
// configuration and is what callers who leave it unset asked for.
func (s Settings) normalize() Settings {
	if s.Window <= 0 {
		s.Window = time.Second
	}
	if s.Buckets < 1 {
		s.Buckets = 1
	}
	if s.MinRequests < 1 {
		s.MinRequests = 1
	}
	if s.FailureRatio <= 0 {
		s.FailureRatio = 0.5
	}
	if s.HalfOpenMax < 1 {
		s.HalfOpenMax = 1
	}
	if s.Now == nil {
		s.Now = time.Now
	}
	return s
}

type bucket struct {
	requests int
	failures int
}

type transition struct {
	from State
	to   State
}

type Breaker struct {
	name      string
	set       Settings
	bucketDur time.Duration
	onChange  func(name string, from, to State)

	// fastState mirrors state for the lock-free read path. Written only under mu. A closed
	// breaker always admits, so Allow/State can answer from it without touching the mutex.
	fastState atomic.Int32

	mu        sync.Mutex
	state     State
	buckets   []bucket
	idx       int       // index of the bucket currently being filled
	start     time.Time // instant buckets[idx] began
	total     bucket    // running sum over all live buckets
	openedAt  time.Time
	probes    int    // half-open probes admitted and not yet resolved
	successes int    // half-open successes since entering half-open
	gen       uint64 // incremented on every entry into half-open; identifies the episode
}

// Probe identifies one admitted request so its outcome can be attributed to the half-open episode
// that admitted it — or to no episode at all. A breaker forwards long-lived streams, so an outcome
// can easily arrive from a request that was admitted while the breaker was still closed, long
// before it tripped. Letting such an outcome resolve a probe would close the breaker on evidence
// that predates the trip, or reopen it while the genuine probes were succeeding.
//
// The zero Probe is safe and does nothing.
type Probe struct {
	b     *Breaker
	gen   uint64
	probe bool // admitted as a half-open probe in generation gen
}

// IsProbe reports whether this request was admitted as a half-open trial.
func (p Probe) IsProbe() bool { return p.probe }

// Success records a successful outcome for the request this Probe was issued for.
func (p Probe) Success() {
	if p.b != nil {
		p.b.record(false, p)
	}
}

// Failure records a failed outcome for the request this Probe was issued for.
func (p Probe) Failure() {
	if p.b != nil {
		p.b.record(true, p)
	}
}

// Release resolves an admission that never became a request — the caller reserved capacity and then
// abandoned the attempt without ever contacting the backend. The rolling window is left untouched
// because there is no outcome to judge, but the reserved slot must still come back: an admitted
// probe that is neither resolved nor released leaks, and HalfOpenMax such leaks wedge the breaker
// half-open forever. Releasing a non-probe is a no-op.
func (p Probe) Release() {
	if p.b == nil || !p.probe {
		return
	}
	p.b.release(p)
}

func (b *Breaker) release(p Probe) {
	b.mu.Lock()
	if p.gen == b.gen && b.state == StateHalfOpen && b.probes > 0 {
		b.probes--
	}
	b.mu.Unlock()
}

func NewBreaker(name string, s Settings) *Breaker {
	s = s.normalize()
	d := s.Window / time.Duration(s.Buckets)
	if d <= 0 {
		d = time.Nanosecond
	}
	return &Breaker{
		name:      name,
		set:       s,
		bucketDur: d,
		onChange:  s.OnStateChange,
		buckets:   make([]bucket, s.Buckets),
		start:     s.Now(),
	}
}

func (b *Breaker) Name() string { return b.name }

// State reports the current state, performing the open -> half-open transition itself so a caller
// that only polls State observes it.
func (b *Breaker) State() State {
	if s := State(b.fastState.Load()); s == StateClosed {
		return s
	}
	b.mu.Lock()
	trs := b.refreshLocked(b.set.Now(), nil)
	s := b.state
	b.mu.Unlock()
	b.fire(trs)
	return s
}

// Ready reports whether a request would be admitted, without reserving anything. Use it for
// speculative checks such as filtering candidate backends; a caller that only considers a backend
// and then discards it must not burn a half-open probe slot, because nothing would ever resolve it
// and the breaker would wedge half-open forever.
func (b *Breaker) Ready() bool {
	if State(b.fastState.Load()) == StateClosed {
		return true
	}
	b.mu.Lock()
	trs := b.refreshLocked(b.set.Now(), nil)
	ready := b.state == StateClosed || (b.state == StateHalfOpen && b.probes < b.set.HalfOpenMax)
	b.mu.Unlock()
	b.fire(trs)
	return ready
}

// Admit reserves capacity for a request the caller is about to make and returns the Probe that
// identifies it. In half-open it admits at most HalfOpenMax probes outstanding at once, and every
// admitted probe MUST be resolved by calling Success or Failure ON THE RETURNED PROBE — resolving
// through the breaker instead leaves the slot reserved forever and eventually wedges it.
//
// The returned Probe is always bound to the breaker, even when the bool is false, so a caller that
// proceeds anyway (a last-resort attempt when every backend is open) still books its outcome into
// the rolling window. Callers that are merely testing a backend want Ready instead.
func (b *Breaker) Admit() (Probe, bool) {
	if State(b.fastState.Load()) == StateClosed {
		return Probe{b: b}, true
	}
	b.mu.Lock()
	trs := b.refreshLocked(b.set.Now(), nil)
	p := Probe{b: b}
	allowed := false
	switch b.state {
	case StateClosed:
		allowed = true
	case StateHalfOpen:
		if b.probes < b.set.HalfOpenMax {
			b.probes++
			p = Probe{b: b, gen: b.gen, probe: true}
			allowed = true
		}
	}
	b.mu.Unlock()
	b.fire(trs)
	return p, allowed
}

// Unattributed returns a Probe that books outcomes into the rolling window without ever resolving a
// half-open probe. Use it where an outcome must be counted but no admission was taken — for example
// when circuit breaking is configured off but the breaker still reports state.
func (b *Breaker) Unattributed() Probe { return Probe{b: b} }

// Success and Failure record an outcome that was NOT admitted through Admit. Such an outcome counts
// into the rolling window (and so can trip a closed breaker) but must never resolve a half-open
// probe or move the breaker out of half-open: it belongs to a request the current episode never
// authorised, and judging a recovering backend on it is how a breaker closes without evidence.
func (b *Breaker) Success() { b.record(false, Probe{}) }

func (b *Breaker) Failure() { b.record(true, Probe{}) }

func (b *Breaker) record(failed bool, p Probe) {
	b.mu.Lock()
	now := b.set.Now()
	trs := b.refreshLocked(now, nil)
	b.countLocked(failed)

	// Only a probe from the episode still running may resolve a slot or steer the state. A probe
	// from an earlier episode is stale: leaving half-open already zeroed probes and successes, so
	// there is nothing of its to release and nothing it may decide.
	current := p.probe && p.gen == b.gen && b.state == StateHalfOpen

	switch b.state {
	case StateClosed:
		if b.total.requests >= b.set.MinRequests &&
			float64(b.total.failures)/float64(b.total.requests) >= b.set.FailureRatio {
			trs = b.setStateLocked(trs, StateOpen, now)
		}
	case StateHalfOpen:
		if !current {
			// Counted in the window above, but this request was never admitted as a trial of the
			// recovering backend, so it gets no say in whether the backend is judged recovered.
			break
		}
		if b.probes > 0 {
			b.probes--
		}
		if failed {
			trs = b.setStateLocked(trs, StateOpen, now)
		} else {
			b.successes++
			if b.successes >= b.set.HalfOpenMax {
				trs = b.setStateLocked(trs, StateClosed, now)
			}
		}
	case StateOpen:
		// Outcome of a request that was admitted before the trip; counted, but the open timer owns
		// the state.
	}
	b.mu.Unlock()
	b.fire(trs)
}

// Counts returns the requests and failures currently inside the rolling window.
func (b *Breaker) Counts() (requests, failures int) {
	b.mu.Lock()
	b.advanceLocked(b.set.Now())
	requests, failures = b.total.requests, b.total.failures
	b.mu.Unlock()
	return requests, failures
}

// Reset forces the breaker closed and empties the rolling window.
func (b *Breaker) Reset() {
	b.mu.Lock()
	now := b.set.Now()
	trs := b.setStateLocked(nil, StateClosed, now)
	b.clearLocked(now)
	b.probes, b.successes = 0, 0
	b.mu.Unlock()
	b.fire(trs)
}

// refreshLocked retires expired buckets and applies the open -> half-open expiry.
func (b *Breaker) refreshLocked(now time.Time, trs []transition) []transition {
	b.advanceLocked(now)
	if b.state == StateOpen && now.Sub(b.openedAt) >= b.set.OpenTimeout {
		trs = b.setStateLocked(trs, StateHalfOpen, now)
	}
	return trs
}

// advanceLocked rolls the window forward to now. A gap of at least Window drops everything.
func (b *Breaker) advanceLocked(now time.Time) {
	elapsed := now.Sub(b.start)
	if elapsed < b.bucketDur {
		return
	}
	n := int(elapsed / b.bucketDur)
	if n >= len(b.buckets) {
		b.clearLocked(now)
		return
	}
	for range n {
		b.idx = (b.idx + 1) % len(b.buckets)
		b.total.requests -= b.buckets[b.idx].requests
		b.total.failures -= b.buckets[b.idx].failures
		b.buckets[b.idx] = bucket{}
	}
	b.start = b.start.Add(time.Duration(n) * b.bucketDur)
}

func (b *Breaker) clearLocked(now time.Time) {
	clear(b.buckets)
	b.total = bucket{}
	b.idx = 0
	b.start = now
}

func (b *Breaker) countLocked(failed bool) {
	b.buckets[b.idx].requests++
	b.total.requests++
	if failed {
		b.buckets[b.idx].failures++
		b.total.failures++
	}
}

// setStateLocked appends the transition instead of invoking the callback: callbacks run after the
// mutex is released so a callback that touches this breaker cannot deadlock.
func (b *Breaker) setStateLocked(trs []transition, to State, now time.Time) []transition {
	from := b.state
	if from == to {
		return trs
	}
	b.state = to
	b.fastState.Store(int32(to))
	b.probes, b.successes = 0, 0
	switch to {
	case StateHalfOpen:
		// A new episode. Probes issued by the previous one can no longer resolve anything here,
		// which is what keeps a slow request admitted before the trip from deciding this recovery.
		b.gen++
	case StateOpen:
		b.openedAt = now
	case StateClosed:
		// Closing after half-open probes succeeded: drop the failures that tripped the breaker so
		// it does not retrip immediately on stale counts.
		b.clearLocked(now)
	}
	return append(trs, transition{from: from, to: to})
}

func (b *Breaker) fire(trs []transition) {
	if b.onChange == nil {
		return
	}
	for _, t := range trs {
		b.onChange(b.name, t.from, t.to)
	}
}

// DefaultMaxEntries bounds a Registry that does not set MaxEntries.
const DefaultMaxEntries = 1024

// OverflowName is the breaker every caller shares once a Registry is at MaxEntries. Traffic that
// lands here is still protected, just not isolated per name.
const OverflowName = "__overflow__"

type Registry struct {
	set      Settings
	breakers sync.Map // name -> *Breaker
	n        atomic.Int64
	overflow *Breaker
}

func NewRegistry(s Settings) *Registry {
	if s.MaxEntries < 1 {
		s.MaxEntries = DefaultMaxEntries
	}
	return &Registry{set: s, overflow: NewBreaker(OverflowName, s)}
}

// Get returns the breaker for name, creating it on miss. Concurrent creates collapse to one
// instance: the loser of the LoadOrStore race discards its breaker. Once MaxEntries distinct names
// exist every further name shares the overflow breaker, so a flood of junk method names costs a
// bounded amount of memory instead of growing until the process dies. The cap is approximate under
// concurrent creates, which is fine — it is a bound, not a quota.
func (r *Registry) Get(name string) *Breaker {
	if v, ok := r.breakers.Load(name); ok {
		return v.(*Breaker)
	}
	if r.n.Load() >= int64(r.set.MaxEntries) {
		return r.overflow
	}
	actual, loaded := r.breakers.LoadOrStore(name, NewBreaker(name, r.set))
	if !loaded {
		r.n.Add(1)
	}
	return actual.(*Breaker)
}
