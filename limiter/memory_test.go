package limiter

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestStore(t testing.TB, alg Algorithm, clock Clock, opts ...func(*MemoryOptions)) *MemoryStore {
	o := MemoryOptions{Algorithm: alg, Clock: clock, SweepInterval: -1}
	for _, f := range opts {
		f(&o)
	}
	s := NewMemoryStore(o)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestMemoryStoreMatchesReference(t *testing.T) {
	ctx := context.Background()
	for _, alg := range []Algorithm{GCRA, TokenBucket} {
		for seed := int64(0); seed < 20; seed++ {
			t.Run(fmt.Sprintf("%s/%d", alg.Name(), seed), func(t *testing.T) {
				clock := NewManualClock(epoch)
				s := newTestStore(t, alg, clock)
				l := propertyLimits[int(seed)%len(propertyLimits)]
				rng := rand.New(rand.NewSource(seed))
				deltas, costs := randomSteps(rng, l, 200)
				ref := newRefMeter(l)
				for i := range deltas {
					clock.Advance(time.Duration(deltas[i]))
					got, err := Allow(ctx, s, "k", l, costs[i])
					if err != nil {
						t.Fatal(err)
					}
					want := ref.take(clock.Now().UnixNano(), costs[i])
					want.Limit = l
					if got != want {
						t.Fatalf("step %d: got %+v, want %+v", i, got, want)
					}
				}
			})
		}
	}
}

func TestMemoryStoreBatchAllOrNothing(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(epoch)
	s := newTestStore(t, GCRA, clock)
	merchant := Request{Key: "merchant:1", Limit: PerMinute(10), Cost: 3}
	payer := Request{Key: "payer:1", Limit: PerMinute(4), Cost: 3}

	res, err := s.Take(ctx, merchant, payer)
	if err != nil || !AllAllowed(res) {
		t.Fatalf("first batch: %+v %v", res, err)
	}
	// payer has 1 left: the batch is denied and merchant must not be charged.
	res, err = s.Take(ctx, merchant, payer)
	if err != nil {
		t.Fatal(err)
	}
	if AllAllowed(res) || !res[0].Allowed || res[1].Allowed {
		t.Fatalf("second batch: %+v", res)
	}
	if res[0].Remaining != 7 {
		t.Fatalf("merchant remaining reported %d, want 7 (nothing consumed)", res[0].Remaining)
	}
	peek, _ := s.Peek(ctx, Request{Key: "merchant:1", Limit: PerMinute(10)})
	if peek[0].Remaining != 7 {
		t.Fatalf("merchant was charged by a denied batch: remaining %d", peek[0].Remaining)
	}
}

func TestMemoryStoreDuplicateKeysInBatch(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, GCRA, NewManualClock(epoch))
	r := Request{Key: "same", Limit: PerSecond(5), Cost: 3}
	res, err := s.Take(ctx, r, r)
	if err != nil {
		t.Fatal(err)
	}
	if AllAllowed(res) || res[1].Allowed {
		t.Fatalf("3+3 on a burst of 5 must be denied: %+v", res)
	}
	if res[0].Remaining != 5 || !res[0].Allowed {
		t.Fatalf("first twin reported against stored state: %+v", res[0])
	}
	r.Cost = 2
	res, _ = s.Take(ctx, r, r)
	if !AllAllowed(res) || res[1].Remaining != 1 {
		t.Fatalf("2+2 on 5: %+v", res)
	}
}

func TestMemoryStoreRefundAndPeek(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(epoch)
	for _, alg := range []Algorithm{GCRA, TokenBucket, SlidingWindow} {
		s := newTestStore(t, alg, clock)
		a := Request{Key: "a", Limit: PerMinute(10), Cost: 4}
		b := Request{Key: "b", Limit: PerMinute(10), Cost: 2}
		if res, _ := s.Take(ctx, a, b); !AllAllowed(res) {
			t.Fatalf("%s: take denied", alg.Name())
		}
		if res, _ := s.Peek(ctx, a, b); res[0].Remaining != 2 || res[1].Remaining != 6 {
			t.Fatalf("%s: peek reports remaining after the hypothetical take: %+v", alg.Name(), res)
		}
		if res, _ := s.Peek(ctx, Request{Key: "a", Limit: a.Limit}, Request{Key: "b", Limit: b.Limit}); res[0].Remaining != 6 || res[1].Remaining != 8 {
			t.Fatalf("%s: peek %+v", alg.Name(), res)
		}
		if err := s.Refund(ctx, a, b); err != nil {
			t.Fatal(err)
		}
		if res, _ := s.Peek(ctx, a); res[0].Remaining != 6 {
			t.Fatalf("%s: after batch refund %+v", alg.Name(), res)
		}
		if err := s.Refund(ctx, a); err != nil {
			t.Fatal(err)
		}
		if st := s.Stats(); st.Keys != 0 {
			t.Fatalf("%s: idle keys should be deleted, have %d", alg.Name(), st.Keys)
		}
	}
}

func TestMemoryStoreErrors(t *testing.T) {
	s := newTestStore(t, GCRA, nil)
	ctx := context.Background()
	if _, err := s.Take(ctx, Request{Key: "k", Limit: PerSecond(2), Cost: 3}); !errors.Is(err, ErrCostExceedsBurst) {
		t.Errorf("cost > burst: %v", err)
	}
	if _, err := s.Take(ctx, Request{Key: "k", Limit: PerSecond(2), Cost: -1}); !errors.Is(err, ErrNegativeCost) {
		t.Errorf("negative cost: %v", err)
	}
	if _, err := s.Take(ctx, Request{Key: "k", Limit: Limit{}}); !errors.Is(err, ErrInvalidLimit) {
		t.Errorf("invalid limit: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Take(cctx, Request{Key: "k", Limit: PerSecond(2), Cost: 1}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	sw := newTestStore(t, SlidingWindow, nil)
	if _, err := sw.Take(ctx, Request{Key: "k", Limit: PerSecond(2).WithBurst(10), Cost: 3}); !errors.Is(err, ErrCostExceedsBurst) {
		t.Errorf("sliding window capacity is the rate: %v", err)
	}
	if sw.Algorithm() != SlidingWindow {
		t.Error("Algorithm()")
	}
	if got := NewMemoryStore(MemoryOptions{SweepInterval: -1}); got.Algorithm() != GCRA {
		t.Error("default algorithm")
	}
}

func TestMemoryStoreExpiryAndSweep(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(epoch)
	s := newTestStore(t, GCRA, clock)
	for i := 0; i < 100; i++ {
		if _, err := Allow(ctx, s, fmt.Sprint("k", i), PerSecond(10), 1); err != nil {
			t.Fatal(err)
		}
	}
	if st := s.Stats(); st.Keys != 100 {
		t.Fatalf("keys = %d", st.Keys)
	}
	clock.Advance(99 * time.Millisecond)
	if n := s.Sweep(); n != 0 {
		t.Fatalf("swept %d live keys", n)
	}
	clock.Advance(time.Millisecond)
	if n := s.Sweep(); n != 100 {
		t.Fatalf("swept %d, want 100", n)
	}
	if st := s.Stats(); st.Keys != 0 || st.Expired != 100 {
		t.Fatalf("stats %+v", st)
	}
}

func TestMemoryStoreSweeperGoroutine(t *testing.T) {
	clock := NewManualClock(epoch)
	s := NewMemoryStore(MemoryOptions{Clock: clock, SweepInterval: time.Millisecond})
	defer s.Close()
	_, _ = Allow(context.Background(), s, "k", PerSecond(1), 1)
	clock.Advance(2 * time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for s.Stats().Keys != 0 {
		if time.Now().After(deadline) {
			t.Fatal("sweeper did not run")
		}
		time.Sleep(time.Millisecond)
	}
	s.Close() // idempotent
}

func TestMemoryStoreBoundedMemory(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(epoch)
	s := newTestStore(t, GCRA, clock, func(o *MemoryOptions) { o.Shards = 3; o.MaxKeys = 40 })
	if len(s.shards) != 4 {
		t.Fatalf("shards rounded to %d, want 4", len(s.shards))
	}
	for i := 0; i < 1000; i++ {
		clock.Advance(time.Microsecond)
		if _, err := Allow(ctx, s, fmt.Sprint("key-", i), PerMinute(1), 1); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.Keys > 40 {
		t.Fatalf("keys = %d, bound is 40", st.Keys)
	}
	if st.Evictions == 0 {
		t.Fatal("expected evictions")
	}
	// Expired keys are reclaimed before live ones are evicted.
	clock.Advance(2 * time.Minute)
	before := s.Stats().Evictions
	for i := 0; i < 10; i++ {
		_, _ = Allow(ctx, s, fmt.Sprint("fresh-", i), PerMinute(1), 1)
	}
	if after := s.Stats().Evictions; after != before {
		t.Fatalf("evicted %d live keys while expired ones were available", after-before)
	}
	unbounded := newTestStore(t, GCRA, clock, func(o *MemoryOptions) { o.MaxKeys = -1 })
	for i := 0; i < 2000; i++ {
		_, _ = Allow(ctx, unbounded, fmt.Sprint(i), PerMinute(1), 1)
	}
	if unbounded.Stats().Keys != 2000 {
		t.Fatal("unbounded store evicted")
	}
}

// TestMemoryStoreGCRAExactUnderContention checks that racing goroutines
// get exactly the burst plus what the elapsed time earns, never more.
func TestMemoryStoreGCRAExactUnderContention(t *testing.T) {
	for _, alg := range []Algorithm{GCRA, TokenBucket, SlidingWindow} {
		t.Run(alg.Name(), func(t *testing.T) {
			clock := NewManualClock(epoch.Truncate(time.Minute))
			s := newTestStore(t, alg, clock, func(o *MemoryOptions) { o.Shards = 4 })
			l := Limit{Rate: 100, Period: time.Second, Burst: 50}
			if alg == SlidingWindow {
				l.Burst = 0
			}
			const workers, perWorker = 16, 500
			var allowed atomic.Int64
			var wg sync.WaitGroup
			ctx := context.Background()
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						if w == 0 && i%50 == 0 {
							clock.Advance(10 * time.Millisecond) // earns one unit
						}
						r, err := Allow(ctx, s, "hot", l, 1)
						if err != nil {
							t.Error(err)
							return
						}
						if r.Allowed {
							allowed.Add(1)
						}
					}
				}(w)
			}
			wg.Wait()
			elapsed := clock.Now().Sub(epoch.Truncate(time.Minute))
			var want int64
			if alg == SlidingWindow {
				want = 100 // 100ms elapsed: still the first window
			} else {
				want = 50 + int64(elapsed/(10*time.Millisecond))
			}
			got := allowed.Load()
			// Clock advances interleave with requests, so the last earned
			// units may not have been claimed yet; admissions can be lower
			// than the bound but never higher.
			if got > want || got < want-10 {
				t.Fatalf("allowed %d, want at most %d (elapsed %s)", got, want, elapsed)
			}
		})
	}
}

func TestMemoryStoreExactFrozenClock(t *testing.T) {
	clock := NewManualClock(epoch)
	s := newTestStore(t, GCRA, clock)
	l := PerSecond(1000).WithBurst(137)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// Mix in multi-key batches that contend on the same key.
				res, _ := s.Take(context.Background(),
					Request{Key: "hot", Limit: l, Cost: 1},
					Request{Key: fmt.Sprint("side-", i%7), Limit: PerSecond(1000).WithBurst(1 << 20), Cost: 1})
				if AllAllowed(res) {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 137 {
		t.Fatalf("allowed %d, want exactly 137", got)
	}
}
