package limiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrBreakerOpen is returned by a [FallbackStore] without a fallback while
// its circuit breaker is open.
var ErrBreakerOpen = errors.New("limiter: circuit breaker open")

// BreakerState is the state of a [Breaker].
type BreakerState int

// Breaker states.
const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	default:
		return "half_open"
	}
}

// BreakerOptions configures a [Breaker].
type BreakerOptions struct {
	// FailureThreshold is the number of consecutive failures that opens the
	// breaker. Defaults to 5.
	FailureThreshold int
	// OpenTimeout is how long the breaker stays open before letting a single
	// probe through. Defaults to 5s.
	OpenTimeout time.Duration
	// Clock defaults to SystemClock.
	Clock Clock
	// OnStateChange, if set, is called (without locks held) on every
	// transition.
	OnStateChange func(from, to BreakerState)
}

// Breaker is a consecutive-failure circuit breaker with a single half-open
// probe. It is safe for concurrent use.
type Breaker struct {
	opts     BreakerOptions
	mu       sync.Mutex
	state    BreakerState
	failures int
	openedAt time.Time
	probing  bool
}

// NewBreaker returns a closed Breaker.
func NewBreaker(opts BreakerOptions) *Breaker {
	if opts.FailureThreshold <= 0 {
		opts.FailureThreshold = 5
	}
	if opts.OpenTimeout <= 0 {
		opts.OpenTimeout = 5 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	return &Breaker{opts: opts}
}

// State returns the current state, moving from open to half-open if the
// open timeout has elapsed.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerOpen && b.opts.Clock.Now().Sub(b.openedAt) >= b.opts.OpenTimeout {
		return BreakerHalfOpen
	}
	return b.state
}

// Allow reports whether a call to the protected dependency may proceed. In
// the half-open state only one caller at a time gets true; it must report
// the outcome with Success or Failure.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	var from, to BreakerState
	changed := false
	ok := false
	switch b.state {
	case BreakerClosed:
		ok = true
	case BreakerOpen:
		if b.opts.Clock.Now().Sub(b.openedAt) >= b.opts.OpenTimeout {
			from, to, changed = b.state, BreakerHalfOpen, true
			b.state = BreakerHalfOpen
			b.probing = true
			ok = true
		}
	case BreakerHalfOpen:
		if !b.probing {
			b.probing = true
			ok = true
		}
	}
	b.mu.Unlock()
	if changed {
		b.notify(from, to)
	}
	return ok
}

// Success records a successful call.
func (b *Breaker) Success() {
	b.mu.Lock()
	from := b.state
	b.failures = 0
	b.probing = false
	b.state = BreakerClosed
	b.mu.Unlock()
	if from != BreakerClosed {
		b.notify(from, BreakerClosed)
	}
}

// Failure records a failed call.
func (b *Breaker) Failure() {
	b.mu.Lock()
	from := b.state
	b.failures++
	b.probing = false
	if b.state == BreakerHalfOpen || b.failures >= b.opts.FailureThreshold {
		b.state = BreakerOpen
		b.openedAt = b.opts.Clock.Now()
	}
	to := b.state
	b.mu.Unlock()
	if from != to {
		b.notify(from, to)
	}
}

func (b *Breaker) notify(from, to BreakerState) {
	if b.opts.OnStateChange != nil {
		b.opts.OnStateChange(from, to)
	}
}

// FallbackOptions configures a [FallbackStore].
type FallbackOptions struct {
	// Fallback serves requests while the primary is failing. Nil means no
	// fallback: calls fail with the primary's error or ErrBreakerOpen, and
	// the caller decides whether to fail open or closed.
	Fallback Store
	// Scale multiplies limits sent to the fallback (see [Limit.Scale]). A
	// global limit of 1000/min enforced by 4 replicas falling back to local
	// memory should use 0.25, so the fleet stays near the global budget.
	// Zero means 1.
	Scale float64
	// Breaker guards the primary. Defaults to NewBreaker(BreakerOptions{}).
	Breaker *Breaker
	// OnFallback, if set, is called whenever a call is served by the
	// fallback (or would have been, when there is none). reason is
	// "error" when the primary just failed and "open" when the breaker
	// skipped it.
	OnFallback func(op, reason string, err error)
}

// FallbackStore degrades gracefully: it sends requests to a primary store
// (typically Redis) and, when the primary fails or its circuit breaker is
// open, to a local fallback with scaled-down limits.
//
// While degraded, limits are enforced per instance rather than globally,
// which is the point: a shared-store outage turns into somewhat looser or
// tighter limits instead of either no limits or an outage of the API.
// Request errors (invalid limits or costs) are returned as is and do not
// count as primary failures, and neither does a cancelled caller context.
type FallbackStore struct {
	primary Store
	opts    FallbackOptions
}

// NewFallbackStore wraps primary.
func NewFallbackStore(primary Store, opts FallbackOptions) *FallbackStore {
	if opts.Breaker == nil {
		opts.Breaker = NewBreaker(BreakerOptions{})
	}
	if opts.Scale == 0 {
		opts.Scale = 1
	}
	return &FallbackStore{primary: primary, opts: opts}
}

// Breaker returns the store's circuit breaker.
func (f *FallbackStore) Breaker() *Breaker { return f.opts.Breaker }

// Take implements [Store].
func (f *FallbackStore) Take(ctx context.Context, reqs ...Request) ([]Result, error) {
	return f.do(ctx, "take", reqs, Store.Take, Store.Take)
}

// Peek implements [Store].
func (f *FallbackStore) Peek(ctx context.Context, reqs ...Request) ([]Result, error) {
	return f.do(ctx, "peek", reqs, Store.Peek, Store.Peek)
}

// Refund implements [Store]. Refunds go to whichever store is currently
// serving; a refund for capacity taken from the other one is lost, which
// errs on the restrictive side.
func (f *FallbackStore) Refund(ctx context.Context, reqs ...Request) error {
	refund := func(s Store, ctx context.Context, reqs ...Request) ([]Result, error) {
		return nil, s.Refund(ctx, reqs...)
	}
	_, err := f.do(ctx, "refund", reqs, refund, refund)
	return err
}

type storeFunc func(Store, context.Context, ...Request) ([]Result, error)

func (f *FallbackStore) do(ctx context.Context, op string, reqs []Request, primary, fallback storeFunc) ([]Result, error) {
	b := f.opts.Breaker
	if !b.Allow() {
		return f.fallback(ctx, op, "open", ErrBreakerOpen, reqs, fallback)
	}
	res, err := primary(f.primary, ctx, reqs...)
	switch {
	case err == nil:
		b.Success()
		return res, nil
	case IsRequestError(err):
		b.Success()
		return nil, err
	case ctx.Err() != nil:
		// The caller gave up; that says nothing about the primary. Release
		// a half-open probe slot without judging it.
		b.mu.Lock()
		b.probing = false
		b.mu.Unlock()
		return nil, err
	}
	b.Failure()
	return f.fallback(ctx, op, "error", err, reqs, fallback)
}

func (f *FallbackStore) fallback(ctx context.Context, op, reason string, cause error, reqs []Request, fn storeFunc) ([]Result, error) {
	if f.opts.OnFallback != nil {
		f.opts.OnFallback(op, reason, cause)
	}
	if f.opts.Fallback == nil {
		return nil, cause
	}
	scaled := reqs
	if f.opts.Scale != 1 {
		scaled = make([]Request, len(reqs))
		for i, r := range reqs {
			r.Limit = r.Limit.Scale(f.opts.Scale)
			if c := r.Limit.EffectiveBurst(); r.Cost > c {
				// A cost that fit the global burst may not fit the scaled
				// one; cap it so the request is judged rather than rejected.
				r.Cost = c
			}
			scaled[i] = r
		}
	}
	res, err := fn(f.opts.Fallback, ctx, scaled...)
	if err != nil {
		return nil, errors.Join(cause, err)
	}
	// Report the limits the caller asked for, not the scaled ones, so
	// headers stay stable while degraded.
	for i := range res {
		res[i].Limit = reqs[i].Limit
	}
	return res, nil
}
