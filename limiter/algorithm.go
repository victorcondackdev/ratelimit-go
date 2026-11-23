package limiter

import (
	"math/bits"
	"time"
)

// State is the per-key state of an algorithm. Its meaning depends on the
// algorithm and is opaque to callers. The zero State always means "idle":
// a key that was never seen, or one whose state has fully decayed, so a
// store may delete a key once its expiry has passed.
type State [3]int64

// Algorithm is a pure, deterministic rate-limiting state machine. Times are
// int64 nanoseconds on any monotonic-enough clock; algorithms never read a
// clock themselves, which is what makes them testable against reference
// models with a fake clock.
type Algorithm interface {
	// Name identifies the algorithm in logs and metrics.
	Name() string
	// Capacity is the largest cost a single request may have under l.
	Capacity(l Limit) int64
	// Apply evaluates a request of the given cost at time now. When the
	// request is allowed, next is the state after consuming it; otherwise
	// next equals s. expireAt is the time at which next decays to the zero
	// State. A cost of zero queries the state without changing it.
	Apply(s State, now int64, l Limit, cost int64) (next State, r Result, expireAt int64)
	// Refund gives back cost previously consumed from s, as far as the
	// algorithm can tell where it went. It never makes s more permissive
	// than the idle state.
	Refund(s State, now int64, l Limit, cost int64) (next State, expireAt int64)
}

// The algorithms provided by this package.
var (
	// GCRA is the generic cell rate algorithm. See [GCRAStep].
	GCRA Algorithm = gcra{}
	// TokenBucket is a token bucket of Burst tokens refilled at Rate per
	// Period. It admits exactly the same requests as GCRA; it is provided for
	// comparison and for readers who think in tokens.
	TokenBucket Algorithm = tokenBucket{}
	// SlidingWindow approximates a sliding log with two fixed windows: the
	// previous window's count is weighted by how much of it still overlaps
	// the sliding window.
	SlidingWindow Algorithm = slidingWindow{}
)

// GCRAOutcome is the result of one GCRA step in caller-chosen time units.
type GCRAOutcome struct {
	// TAT is the theoretical arrival time to store: advanced by the cost when
	// allowed, max(previous TAT, now) otherwise.
	TAT int64
	// Allowed reports whether the request conforms.
	Allowed bool
	// Remaining is the number of unit-cost requests that would still conform
	// right now (after this request when it was allowed).
	Remaining int64
	// RetryAfter is the wait until the request would conform; zero when
	// allowed.
	RetryAfter int64
	// ResetAfter is the wait until TAT is reached, that is, until the full
	// burst is available again.
	ResetAfter int64
}

// GCRAStep is the generic cell rate algorithm in its virtual-scheduling
// form, independent of time units.
//
// interval is the emission interval T (Period/Rate) and burst the number of
// unit-cost requests admitted back to back, so the delay variation
// tolerance is tau = T*burst. The state is a single number, the
// theoretical arrival time (TAT), which is the time at which the key would
// be idle again; a missing key is TAT = 0.
//
// A request of cost c at time now conforms iff
//
//	max(TAT, now) + c*T - tau <= now
//
// and, when it does, TAT becomes max(TAT, now) + c*T. Nothing else is
// stored: no counters, no window boundaries and no refill timestamps.
//
// The same arithmetic runs in the Redis Lua script with microseconds; the
// fake Redis server used in tests calls this function to emulate it.
func GCRAStep(tat, now, interval, burst, cost int64) GCRAOutcome {
	if tat < now {
		tat = now
	}
	tau := interval * burst
	newTAT := tat + interval*cost
	allowAt := newTAT - tau
	if now < allowAt {
		return GCRAOutcome{
			TAT:        tat,
			Remaining:  clampRemaining((tau - (tat - now)) / interval),
			RetryAfter: allowAt - now,
			ResetAfter: tat - now,
		}
	}
	return GCRAOutcome{
		TAT:        newTAT,
		Allowed:    true,
		Remaining:  clampRemaining((tau - (newTAT - now)) / interval),
		ResetAfter: newTAT - now,
	}
}

func clampRemaining(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

type gcra struct{}

func (gcra) Name() string             { return "gcra" }
func (gcra) Capacity(l Limit) int64   { return l.EffectiveBurst() }
func (g gcra) interval(l Limit) int64 { return int64(l.Interval()) }

func (g gcra) Apply(s State, now int64, l Limit, cost int64) (State, Result, int64) {
	o := GCRAStep(s[0], now, g.interval(l), l.EffectiveBurst(), cost)
	r := Result{
		Allowed:    o.Allowed,
		Limit:      l,
		Remaining:  o.Remaining,
		RetryAfter: time.Duration(o.RetryAfter),
		ResetAfter: time.Duration(o.ResetAfter),
	}
	if !o.Allowed {
		return s, r, s[0]
	}
	return State{o.TAT}, r, o.TAT
}

func (g gcra) Refund(s State, now int64, l Limit, cost int64) (State, int64) {
	tat := s[0] - g.interval(l)*cost
	if tat <= now {
		return State{}, 0
	}
	return State{tat}, tat
}

// tokenBucket stores the bucket as a deficit: the time, in nanoseconds of
// refill, that it would take to fill the bucket again, plus the time of the
// last update. Tokens = Burst - deficit/T. The zero State is a full bucket.
type tokenBucket struct{}

func (tokenBucket) Name() string           { return "token_bucket" }
func (tokenBucket) Capacity(l Limit) int64 { return l.EffectiveBurst() }

func (tokenBucket) refill(s State, now int64) int64 {
	deficit, last := s[0], s[1]
	if elapsed := now - last; elapsed > 0 {
		deficit -= elapsed
	}
	if deficit < 0 {
		deficit = 0
	}
	return deficit
}

func (tb tokenBucket) Apply(s State, now int64, l Limit, cost int64) (State, Result, int64) {
	iv := int64(l.Interval())
	capacity := iv * l.EffectiveBurst()
	deficit := tb.refill(s, now)
	need := deficit + cost*iv
	r := Result{Limit: l}
	if need > capacity {
		r.Remaining = clampRemaining((capacity - deficit) / iv)
		r.RetryAfter = time.Duration(need - capacity)
		r.ResetAfter = time.Duration(deficit)
		return s, r, now + deficit
	}
	r.Allowed = true
	r.Remaining = (capacity - need) / iv
	r.ResetAfter = time.Duration(need)
	if need == 0 {
		return State{}, r, 0
	}
	return State{need, now}, r, now + need
}

func (tb tokenBucket) Refund(s State, now int64, l Limit, cost int64) (State, int64) {
	deficit := tb.refill(s, now) - cost*int64(l.Interval())
	if deficit <= 0 {
		return State{}, 0
	}
	return State{deficit, now}, now + deficit
}

// slidingWindow state: [0] start of the current fixed window, [1] count of
// the previous window, [2] count of the current window. Windows are aligned
// to multiples of Period since the clock's epoch.
type slidingWindow struct{}

func (slidingWindow) Name() string           { return "sliding_window" }
func (slidingWindow) Capacity(l Limit) int64 { return l.Rate }

// roll advances s to the window containing now and returns the elapsed time
// into that window.
func (slidingWindow) roll(s State, now int64, w int64) (State, int64) {
	ws := now - now%w
	switch s[0] {
	case ws:
	case ws - w:
		s = State{ws, s[2], 0}
	default:
		s = State{ws, 0, 0}
	}
	return s, now - ws
}

// Apply admits the request iff
//
//	prev*(W-e)/W + curr + cost <= Rate
//
// where e is the time elapsed in the current window. The comparison is done
// in 128-bit integer arithmetic, so there is no floating-point rounding.
func (sw slidingWindow) Apply(s State, now int64, l Limit, cost int64) (State, Result, int64) {
	w := int64(l.Period)
	orig := s
	s, e := sw.roll(s, now, w)
	ws, prev, curr := s[0], uint64(s[1]), uint64(s[2])
	weightLeft := uint64(w - e)
	rate := uint64(l.Rate)
	r := Result{Limit: l}

	if leq128(prev, weightLeft, curr+uint64(cost), uint64(w), rate, uint64(w)) {
		curr += uint64(cost)
		s[2] = int64(curr)
		r.Allowed = true
	}
	weighted := ceilMulDiv(prev, weightLeft, uint64(w))
	if used := weighted + curr; used < rate {
		r.Remaining = int64(rate - used)
	}
	switch {
	case curr > 0:
		r.ResetAfter = time.Duration(ws + 2*w - now)
	case prev > 0:
		r.ResetAfter = time.Duration(ws + w - now)
	}
	if !r.Allowed {
		r.RetryAfter = time.Duration(sw.retryAfter(prev, curr, uint64(cost), rate, uint64(w), uint64(e)))
		s = orig
	}
	expireAt := ws + w
	if curr > 0 {
		expireAt = ws + 2*w
	} else if prev == 0 {
		return State{}, r, 0
	}
	return s, r, expireAt
}

// retryAfter solves for the earliest time at which the request would be
// admitted, assuming no other traffic.
func (slidingWindow) retryAfter(prev, curr, cost, rate, w, e uint64) int64 {
	if curr+cost <= rate {
		// Wait in this window until the previous window's weight decays:
		// prev*(W-e') <= (rate-curr-cost)*W.
		budget := mulDiv(rate-curr-cost, w, prev)
		eMin := w - min(budget, w)
		return int64(eMin - e)
	}
	// The current window is already too full; wait for the next one, where
	// it becomes the previous window: curr*(W-e'') <= (rate-cost)*W.
	budget := mulDiv(rate-cost, w, curr)
	eMin := w - min(budget, w)
	return int64(w - e + eMin)
}

func (sw slidingWindow) Refund(s State, now int64, l Limit, cost int64) (State, int64) {
	w := int64(l.Period)
	s, _ = sw.roll(s, now, w)
	take := min(cost, s[2])
	s[2] -= take
	s[1] -= min(cost-take, s[1])
	switch {
	case s[2] > 0:
		return s, s[0] + 2*w
	case s[1] > 0:
		return s, s[0] + w
	}
	return State{}, 0
}

// leq128 reports whether a*b + c*d <= e*f without overflow.
func leq128(a, b, c, d, e, f uint64) bool {
	h1, l1 := bits.Mul64(a, b)
	h2, l2 := bits.Mul64(c, d)
	lo, carry := bits.Add64(l1, l2, 0)
	hi, overflow := bits.Add64(h1, h2, carry)
	if overflow != 0 {
		return false
	}
	h3, l3 := bits.Mul64(e, f)
	return hi < h3 || (hi == h3 && lo <= l3)
}

// mulDiv returns floor(a*b/c), saturating at MaxUint64.
func mulDiv(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	if hi >= c {
		return ^uint64(0)
	}
	q, _ := bits.Div64(hi, lo, c)
	return q
}

// ceilMulDiv returns ceil(a*b/c), saturating at MaxUint64.
func ceilMulDiv(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	if hi >= c {
		return ^uint64(0)
	}
	q, rem := bits.Div64(hi, lo, c)
	if rem != 0 {
		q++
	}
	return q
}
