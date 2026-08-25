// Package ratelimit provides a global token bucket plus optional per-client buckets held in a
// sharded map, sized for the request hot path of the load balancer.
package ratelimit

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/time/rate"
)

// shardCount must stay a power of two only for cheap distribution reasoning; the index itself is
// a modulo so any value >= 16 is correct.
const shardCount = 32

// defaultTTL is the idle lifetime of a per-client bucket when Config.TTL is zero.
const defaultTTL = 5 * time.Minute

type Config struct {
	Enabled        bool
	RPS            float64
	Burst          int
	PerClient      bool
	PerClientRPS   float64
	PerClientBurst int
	TTL            time.Duration // idle per-client entry eviction, default 5m
}

type entry struct {
	lim  *rate.Limiter
	last atomic.Int64 // unix nanos of the most recent access
}

type shard struct {
	mu sync.RWMutex
	m  map[string]*entry
}

type Limiter struct {
	enabled     bool
	perClient   bool
	ttl         time.Duration
	now         func() time.Time
	global      *rate.Limiter
	clientRate  rate.Limit
	clientBurst int

	shards [shardCount]shard

	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func New(c Config) *Limiter {
	return newWithClock(c, time.Now)
}

func newWithClock(c Config, now func() time.Time) *Limiter {
	ttl := c.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}
	globalRate, globalBurst := bucketParams(c.RPS, c.Burst)
	clientRate, clientBurst := bucketParams(c.PerClientRPS, c.PerClientBurst)

	l := &Limiter{
		enabled:     c.Enabled,
		perClient:   c.PerClient,
		ttl:         ttl,
		now:         now,
		global:      rate.NewLimiter(globalRate, globalBurst),
		clientRate:  clientRate,
		clientBurst: clientBurst,
		stop:        make(chan struct{}),
	}
	for i := range l.shards {
		l.shards[i].m = make(map[string]*entry)
	}
	if l.enabled && l.perClient {
		l.wg.Add(1)
		go l.janitor()
	}
	return l
}

// bucketParams keeps a misconfigured (zero or negative) rate from black-holing every request: an
// unset limit means "no limit here", not "deny everything".
func bucketParams(rps float64, burst int) (rate.Limit, int) {
	if rps <= 0 || math.IsNaN(rps) {
		return rate.Inf, 0
	}
	if burst <= 0 {
		burst = int(math.Ceil(rps))
		if burst < 1 {
			burst = 1
		}
	}
	return rate.Limit(rps), burst
}

// Allow reports whether a request from clientKey may proceed. The global bucket is consulted
// first so a rejected request never allocates per-client state.
func (l *Limiter) Allow(clientKey string) bool {
	if !l.enabled {
		return true
	}
	if !l.global.Allow() {
		return false
	}
	if !l.perClient {
		return true
	}
	return l.client(clientKey).Allow()
}

func (l *Limiter) client(key string) *rate.Limiter {
	sh := &l.shards[xxhash.Sum64String(key)%shardCount]

	sh.mu.RLock()
	e := sh.m[key]
	sh.mu.RUnlock()

	if e == nil {
		sh.mu.Lock()
		// Re-check: another goroutine may have created the bucket between the two locks, and two
		// buckets for one key would double the effective limit.
		if e = sh.m[key]; e == nil {
			e = &entry{lim: rate.NewLimiter(l.clientRate, l.clientBurst)}
			sh.m[key] = e
		}
		sh.mu.Unlock()
	}

	e.last.Store(l.now().UnixNano())
	return e.lim
}

// Clients returns the number of live per-client buckets.
// PerClient reports whether Allow will look at the client key at all. Callers use it to skip
// building a key that would be discarded — resolving one means a peer lookup and a string split.
func (l *Limiter) PerClient() bool { return l.enabled && l.perClient }

func (l *Limiter) Clients() int {
	n := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

// Close stops the janitor. It is safe to call more than once and on a disabled limiter.
func (l *Limiter) Close() {
	l.closeOnce.Do(func() {
		close(l.stop)
		l.wg.Wait()
	})
}

func (l *Limiter) janitor() {
	defer l.wg.Done()
	t := time.NewTicker(l.ttl / 2)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.sweep(l.now().Add(-l.ttl).UnixNano())
		}
	}
}

// sweep evicts every entry last touched before cutoff. Shards are locked one at a time so a sweep
// never blocks the whole hot path, and Close never waits on a lock the sweep holds.
func (l *Limiter) sweep(cutoff int64) {
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for k, e := range sh.m {
			if e.last.Load() < cutoff {
				delete(sh.m, k)
			}
		}
		sh.mu.Unlock()
	}
}
