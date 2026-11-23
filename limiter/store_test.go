package limiter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// flakyStore wraps a Store and can fail or be raced on demand.
type flakyStore struct {
	Store
	mu        sync.Mutex
	err       error
	takeHook  func() // runs before Take, to simulate a concurrent caller
	refundErr error
	calls     map[string]int
}

func (f *flakyStore) count(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[op]++
}

func (f *flakyStore) failure() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

func (f *flakyStore) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *flakyStore) Take(ctx context.Context, reqs ...Request) ([]Result, error) {
	f.count("take")
	if f.takeHook != nil {
		f.takeHook()
	}
	if err := f.failure(); err != nil {
		return nil, err
	}
	return f.Store.Take(ctx, reqs...)
}

func (f *flakyStore) Peek(ctx context.Context, reqs ...Request) ([]Result, error) {
	f.count("peek")
	if err := f.failure(); err != nil {
		return nil, err
	}
	return f.Store.Peek(ctx, reqs...)
}

func (f *flakyStore) Refund(ctx context.Context, reqs ...Request) error {
	f.count("refund")
	if f.refundErr != nil {
		return f.refundErr
	}
	if err := f.failure(); err != nil {
		return err
	}
	return f.Store.Refund(ctx, reqs...)
}

func remaining(t *testing.T, s Store, key string, l Limit) int64 {
	t.Helper()
	res, err := s.Peek(context.Background(), Request{Key: key, Limit: l})
	if err != nil {
		t.Fatal(err)
	}
	return res[0].Remaining
}

func TestTakeAllCheckThenCommit(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(epoch)
	l := PerMinute(5)
	a := &flakyStore{Store: newTestStore(t, GCRA, clock)}
	b := &flakyStore{Store: newTestStore(t, GCRA, clock)}

	batches := []Batch{
		{Store: a, Requests: []Request{{Key: "merchant", Limit: l, Cost: 2}}},
		{Store: b, Requests: []Request{{Key: "payer", Limit: l, Cost: 2}}},
	}
	if _, ok, err := TakeAll(ctx, batches...); !ok || err != nil {
		t.Fatalf("first: ok=%v err=%v", ok, err)
	}

	// Check phase denies: nothing is consumed anywhere.
	_, _ = b.Store.Take(ctx, Request{Key: "payer", Limit: l, Cost: 2})
	res, ok, err := TakeAll(ctx, batches...)
	if ok || err != nil || !res[0][0].Allowed || res[1][0].Allowed {
		t.Fatalf("check denial: %+v ok=%v err=%v", res, ok, err)
	}
	if got := remaining(t, a, "merchant", l); got != 3 {
		t.Fatalf("merchant charged on check denial: remaining %d", got)
	}

	// Commit phase race: b is drained between check and commit, so a's
	// commit must be refunded.
	_ = b.Store.Refund(ctx, Request{Key: "payer", Limit: l, Cost: 5})
	b.takeHook = func() {
		b.takeHook = nil
		_, _ = b.Store.Take(ctx, Request{Key: "payer", Limit: l, Cost: 5})
	}
	_, ok, err = TakeAll(ctx, batches...)
	if ok || err != nil {
		t.Fatalf("commit race: ok=%v err=%v", ok, err)
	}
	if got := remaining(t, a, "merchant", l); got != 3 {
		t.Fatalf("merchant not refunded after commit race: remaining %d", got)
	}

	// A store error during commit refunds and reports the error.
	_ = b.Store.Refund(ctx, Request{Key: "payer", Limit: l, Cost: 5})
	boom := errors.New("boom")
	b.takeHook = func() { b.takeHook = nil; b.setErr(boom) }
	if _, _, err := TakeAll(ctx, batches...); !errors.Is(err, boom) {
		t.Fatalf("commit error: %v", err)
	}
	if got := remaining(t, a, "merchant", l); got != 3 {
		t.Fatalf("merchant not refunded after commit error: remaining %d", got)
	}
	// Errors in the check phase are returned as is.
	if _, _, err := TakeAll(ctx, batches...); !errors.Is(err, boom) {
		t.Fatalf("check error: %v", err)
	}
	// Single batch goes straight to Take.
	if _, _, err := TakeAll(ctx, batches[1]); !errors.Is(err, boom) {
		t.Fatalf("single batch error: %v", err)
	}
	b.setErr(nil)
	if _, ok, err := TakeAll(ctx, batches[1]); !ok || err != nil {
		t.Fatalf("single batch: %v %v", ok, err)
	}
}

func TestTakeAllRefundFailureStillDenies(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(epoch)
	l := PerMinute(5)
	a := &flakyStore{Store: newTestStore(t, GCRA, clock), refundErr: errors.New("refund failed")}
	b := &flakyStore{Store: newTestStore(t, GCRA, clock)}
	b.takeHook = func() {
		b.takeHook = nil
		_, _ = b.Store.Take(ctx, Request{Key: "p", Limit: l, Cost: 5})
	}
	_, ok, err := TakeAll(ctx,
		Batch{Store: a, Requests: []Request{{Key: "m", Limit: l, Cost: 1}}},
		Batch{Store: b, Requests: []Request{{Key: "p", Limit: l, Cost: 1}}})
	if ok || err != nil {
		t.Fatalf("a failed refund must still be a plain denial: ok=%v err=%v", ok, err)
	}
}

func TestAllow(t *testing.T) {
	s := newTestStore(t, GCRA, NewManualClock(epoch))
	if _, err := Allow(context.Background(), s, "k", Limit{}, 1); err == nil {
		t.Fatal("want error")
	}
}

func TestBreaker(t *testing.T) {
	clock := NewManualClock(epoch)
	var transitions []string
	b := NewBreaker(BreakerOptions{FailureThreshold: 2, OpenTimeout: time.Second, Clock: clock,
		OnStateChange: func(from, to BreakerState) { transitions = append(transitions, from.String()+">"+to.String()) }})
	if !b.Allow() || b.State() != BreakerClosed {
		t.Fatal("closed breaker must allow")
	}
	b.Failure()
	if b.State() != BreakerClosed {
		t.Fatal("one failure must not open")
	}
	b.Failure()
	if b.State() != BreakerOpen || b.Allow() {
		t.Fatal("two failures must open")
	}
	clock.Advance(time.Second)
	if b.State() != BreakerHalfOpen {
		t.Fatal("timeout must half-open")
	}
	if !b.Allow() {
		t.Fatal("first probe must pass")
	}
	if b.Allow() {
		t.Fatal("second concurrent probe must not pass")
	}
	b.Failure()
	if b.State() != BreakerOpen {
		t.Fatal("failed probe must reopen")
	}
	clock.Advance(time.Second)
	if !b.Allow() {
		t.Fatal("probe after reopen")
	}
	b.Success()
	if b.State() != BreakerClosed {
		t.Fatal("successful probe must close")
	}
	want := []string{"closed>open", "open>half_open", "half_open>open", "open>half_open", "half_open>closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions %v", transitions)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions %v, want %v", transitions, want)
		}
	}
	d := NewBreaker(BreakerOptions{})
	if d.opts.FailureThreshold != 5 || d.opts.OpenTimeout != 5*time.Second {
		t.Fatal("defaults")
	}
}

func TestFallbackStore(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(epoch)
	primary := &flakyStore{Store: newTestStore(t, GCRA, clock)}
	local := newTestStore(t, GCRA, clock)
	var events []string
	f := NewFallbackStore(primary, FallbackOptions{
		Fallback: local,
		Scale:    0.5,
		Breaker:  NewBreaker(BreakerOptions{FailureThreshold: 1, OpenTimeout: time.Second, Clock: clock}),
		OnFallback: func(op, reason string, err error) {
			events = append(events, op+":"+reason)
		},
	})
	l := PerMinute(10)
	if r, err := Allow(ctx, f, "k", l, 1); err != nil || r.Remaining != 9 {
		t.Fatalf("primary: %+v %v", r, err)
	}
	primary.setErr(errors.New("connection refused"))
	r, err := Allow(ctx, f, "k", l, 1)
	if err != nil || !r.Allowed || r.Remaining != 4 || r.Limit != l {
		t.Fatalf("fallback with scaled limit: %+v %v", r, err)
	}
	if f.Breaker().State() != BreakerOpen {
		t.Fatal("breaker should be open")
	}
	// Open breaker: primary is not even called.
	before := primary.calls["take"]
	if _, err := f.Peek(ctx, Request{Key: "k", Limit: l}); err != nil {
		t.Fatal(err)
	}
	if err := f.Refund(ctx, Request{Key: "k", Limit: l, Cost: 1}); err != nil {
		t.Fatal(err)
	}
	// Costs above the scaled burst are capped, not rejected.
	if r, err := Allow(ctx, f, "big", l, 8); err != nil || !r.Allowed {
		t.Fatalf("capped cost: %+v %v", r, err)
	}
	if primary.calls["take"] != before {
		t.Fatal("primary called while breaker open")
	}
	// Recovery after the open timeout.
	primary.setErr(nil)
	clock.Advance(time.Second)
	if r, err := Allow(ctx, f, "k", l, 1); err != nil || r.Remaining != 8 {
		t.Fatalf("recovered: %+v %v", r, err)
	}
	if f.Breaker().State() != BreakerClosed {
		t.Fatal("breaker should close after a good probe")
	}
	want := []string{"take:error", "peek:open", "refund:open", "take:open"}
	if len(events) != len(want) {
		t.Fatalf("events %v", events)
	}
	// Request errors pass through and do not trip the breaker.
	if _, err := Allow(ctx, f, "k", l, 11); !errors.Is(err, ErrCostExceedsBurst) {
		t.Fatalf("request error: %v", err)
	}
	if f.Breaker().State() != BreakerClosed {
		t.Fatal("request error tripped the breaker")
	}
}

func TestFallbackStoreWithoutFallback(t *testing.T) {
	clock := NewManualClock(epoch)
	boom := errors.New("down")
	primary := &flakyStore{Store: newTestStore(t, GCRA, clock), err: boom}
	f := NewFallbackStore(primary, FallbackOptions{Breaker: NewBreaker(BreakerOptions{FailureThreshold: 1, Clock: clock})})
	if _, err := Allow(context.Background(), f, "k", PerSecond(1), 1); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	if _, err := Allow(context.Background(), f, "k", PerSecond(1), 1); !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("got %v", err)
	}
	// A failing fallback joins both errors.
	failing := &flakyStore{Store: newTestStore(t, GCRA, clock), err: errors.New("local broken")}
	f2 := NewFallbackStore(primary, FallbackOptions{Fallback: failing, Breaker: NewBreaker(BreakerOptions{Clock: clock})})
	if _, err := Allow(context.Background(), f2, "k", PerSecond(1), 1); !errors.Is(err, boom) || !errors.Is(err, failing.err) {
		t.Fatalf("got %v", err)
	}
}

func TestFallbackStoreCancelledContext(t *testing.T) {
	clock := NewManualClock(epoch)
	primary := &flakyStore{Store: newTestStore(t, GCRA, clock)}
	b := NewBreaker(BreakerOptions{FailureThreshold: 1, Clock: clock})
	f := NewFallbackStore(primary, FallbackOptions{Breaker: b})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	primary.setErr(context.Canceled)
	if _, err := Allow(ctx, f, "k", PerSecond(1), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if b.State() != BreakerClosed {
		t.Fatal("caller cancellation tripped the breaker")
	}
}
