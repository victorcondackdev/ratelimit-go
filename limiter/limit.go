package limiter

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"
)

// Errors returned for requests that can never be satisfied. They are
// programming or configuration errors, not rate-limit denials, and stores
// return them without touching any state.
var (
	// ErrInvalidLimit reports a Limit that fails [Limit.Validate].
	ErrInvalidLimit = errors.New("limiter: invalid limit")
	// ErrNegativeCost reports a request with a cost below zero.
	ErrNegativeCost = errors.New("limiter: negative cost")
	// ErrCostExceedsBurst reports a request whose cost is larger than the
	// limit's burst (or, for the sliding window, its rate). Such a request
	// would be denied forever, so it is rejected up front instead of being
	// given a Retry-After that never comes true.
	ErrCostExceedsBurst = errors.New("limiter: cost exceeds burst")
)

// MinInterval is the smallest emission interval (Period/Rate) a Limit may
// have. The Redis store computes in microseconds so that its Lua arithmetic
// stays exact in IEEE-754 doubles; the same floor is applied everywhere so a
// Limit behaves identically on every store.
const MinInterval = time.Microsecond

// Limit describes a rate: Rate units of cost per Period, with up to Burst
// units admitted back to back from an idle state.
//
// For GCRA and the token bucket, the sustained rate is Rate/Period and the
// burst is Burst (Rate when Burst is zero). For the sliding window, Rate is
// the number of units allowed per window of length Period and Burst is
// ignored.
type Limit struct {
	Rate   int64
	Period time.Duration
	Burst  int64
}

// PerSecond returns a limit of n units per second with burst n.
func PerSecond(n int64) Limit { return Limit{Rate: n, Period: time.Second} }

// PerMinute returns a limit of n units per minute with burst n.
func PerMinute(n int64) Limit { return Limit{Rate: n, Period: time.Minute} }

// PerHour returns a limit of n units per hour with burst n.
func PerHour(n int64) Limit { return Limit{Rate: n, Period: time.Hour} }

// PerDay returns a limit of n units per 24 hours with burst n.
func PerDay(n int64) Limit { return Limit{Rate: n, Period: 24 * time.Hour} }

// WithBurst returns a copy of l with the given burst.
func (l Limit) WithBurst(burst int64) Limit {
	l.Burst = burst
	return l
}

// EffectiveBurst returns Burst, or Rate when Burst is zero.
func (l Limit) EffectiveBurst() int64 {
	if l.Burst == 0 {
		return l.Rate
	}
	return l.Burst
}

// Interval returns the emission interval Period/Rate: the time it takes to
// earn back one unit of cost. It is truncated to whole microseconds, which
// makes the effective rate at most one microsecond per unit faster than the
// nominal one.
func (l Limit) Interval() time.Duration {
	if l.Rate <= 0 {
		return 0
	}
	iv := l.Period / time.Duration(l.Rate)
	return iv - iv%MinInterval
}

// Validate reports whether l can be used by a store. It requires a positive
// rate and period, a non-negative burst, an emission interval of at least
// [MinInterval], and Burst*Interval and Rate*Period small enough not to
// overflow int64 nanoseconds.
func (l Limit) Validate() error {
	switch {
	case l.Rate <= 0:
		return fmt.Errorf("%w: rate must be positive, got %d", ErrInvalidLimit, l.Rate)
	case l.Period <= 0:
		return fmt.Errorf("%w: period must be positive, got %s", ErrInvalidLimit, l.Period)
	case l.Burst < 0:
		return fmt.Errorf("%w: burst must not be negative, got %d", ErrInvalidLimit, l.Burst)
	case l.Interval() < MinInterval:
		return fmt.Errorf("%w: %d per %s is faster than one per %s", ErrInvalidLimit, l.Rate, l.Period, MinInterval)
	}
	if hi, lo := bits.Mul64(uint64(l.EffectiveBurst()), uint64(l.Interval())); hi != 0 || lo > 1<<62 {
		return fmt.Errorf("%w: burst %d is too large for interval %s", ErrInvalidLimit, l.EffectiveBurst(), l.Interval())
	}
	if hi, lo := bits.Mul64(uint64(l.Rate), uint64(l.Period)); hi != 0 || lo > 1<<62 {
		return fmt.Errorf("%w: rate %d is too large for period %s", ErrInvalidLimit, l.Rate, l.Period)
	}
	return nil
}

// Scale returns l with Rate and Burst multiplied by f and rounded down,
// never below one. It is used to derive a per-instance local limit from a
// global one, for example Scale(1.0/replicas) for the fallback store.
func (l Limit) Scale(f float64) Limit {
	scale := func(n int64) int64 {
		v := math.Floor(float64(n) * f)
		if v < 1 {
			return 1
		}
		if v > math.MaxInt64/2 {
			return math.MaxInt64 / 2
		}
		return int64(v)
	}
	out := l
	out.Rate = scale(l.Rate)
	if l.Burst != 0 {
		out.Burst = scale(l.Burst)
	}
	return out
}

// String formats l as "100/1m0s" or "100/1m0s burst 20".
func (l Limit) String() string {
	if l.Burst == 0 || l.Burst == l.Rate {
		return fmt.Sprintf("%d/%s", l.Rate, l.Period)
	}
	return fmt.Sprintf("%d/%s burst %d", l.Rate, l.Period, l.Burst)
}

// Result is the outcome of applying a request to a limit.
type Result struct {
	// Allowed reports whether the request was admitted (and, for Take,
	// consumed).
	Allowed bool
	// Limit is the limit the request was evaluated against.
	Limit Limit
	// Remaining is how many units of cost could still be admitted right now,
	// after this request when it was allowed. It is informative only: under
	// concurrency another request may consume them first.
	Remaining int64
	// RetryAfter is how long to wait until the same request would be
	// admitted, assuming no other traffic. Zero when Allowed.
	RetryAfter time.Duration
	// ResetAfter is how long until the key is back to its idle state, that
	// is, until the full burst is available again.
	ResetAfter time.Duration
}

// Request is one unit of work for a [Store]: consume Cost units of Limit
// under Key.
type Request struct {
	Key   string
	Limit Limit
	Cost  int64
}

// validate checks the limit and cost of r against what alg can ever admit.
func (r Request) validate(capacity func(Limit) int64) error {
	if err := r.Limit.Validate(); err != nil {
		return err
	}
	if r.Cost < 0 {
		return fmt.Errorf("%w: %d", ErrNegativeCost, r.Cost)
	}
	if c := capacity(r.Limit); r.Cost > c {
		return fmt.Errorf("%w: cost %d, capacity %d", ErrCostExceedsBurst, r.Cost, c)
	}
	return nil
}

// ValidateRequests checks every request against the GCRA capacity rules
// (cost at most the effective burst). Stores that only implement GCRA, such
// as the Redis store, use it before talking to the server.
func ValidateRequests(reqs []Request) error {
	for _, r := range reqs {
		if err := r.validate(Limit.EffectiveBurst); err != nil {
			return fmt.Errorf("key %q: %w", r.Key, err)
		}
	}
	return nil
}

// AllAllowed reports whether every result is allowed. An empty batch is
// trivially allowed.
func AllAllowed(results []Result) bool {
	for _, r := range results {
		if !r.Allowed {
			return false
		}
	}
	return true
}
