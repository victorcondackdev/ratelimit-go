package limiter_test

import (
	"context"
	"fmt"
	"time"

	"github.com/victorcondackdev/ratelimit-go/limiter"
)

func ExampleGCRAStep() {
	// 1 request per 10 time units, bursts of 3. The whole state is one
	// number, the theoretical arrival time.
	var tat int64
	for now := int64(0); now <= 10; now += 2 {
		o := limiter.GCRAStep(tat, now, 10, 3, 1)
		if o.Allowed {
			tat = o.TAT
		}
		fmt.Printf("t=%2d allowed=%-5v remaining=%d retry=%d\n", now, o.Allowed, o.Remaining, o.RetryAfter)
	}
	// Output:
	// t= 0 allowed=true  remaining=2 retry=0
	// t= 2 allowed=true  remaining=1 retry=0
	// t= 4 allowed=true  remaining=0 retry=0
	// t= 6 allowed=false remaining=0 retry=4
	// t= 8 allowed=false remaining=0 retry=2
	// t=10 allowed=true  remaining=0 retry=0
}

func ExampleMemoryStore() {
	clock := limiter.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := limiter.NewMemoryStore(limiter.MemoryOptions{Clock: clock})
	defer store.Close()

	ctx := context.Background()
	pix := limiter.PerMinute(60).WithBurst(5) // one per second, bursts of five

	for i := 0; i < 6; i++ {
		r, _ := limiter.Allow(ctx, store, "merchant:42", pix, 1)
		fmt.Println(r.Allowed, r.Remaining, r.RetryAfter)
	}
	clock.Advance(time.Second)
	r, _ := limiter.Allow(ctx, store, "merchant:42", pix, 1)
	fmt.Println(r.Allowed, r.Remaining)
	// Output:
	// true 4 0s
	// true 3 0s
	// true 2 0s
	// true 1 0s
	// true 0 0s
	// false 0 1s
	// true 0
}

func ExampleMemoryStore_Take() {
	store := limiter.NewMemoryStore(limiter.MemoryOptions{})
	defer store.Close()
	ctx := context.Background()

	// A bulk payout of 30 items costs 30 against the merchant and the
	// payer. Both must pass; when one denies, neither is charged.
	merchant := limiter.Request{Key: "merchant:42", Limit: limiter.PerMinute(100), Cost: 30}
	payer := limiter.Request{Key: "payer:abc", Limit: limiter.PerMinute(50), Cost: 30}

	res, _ := store.Take(ctx, merchant, payer)
	fmt.Println(limiter.AllAllowed(res), res[0].Remaining, res[1].Remaining)
	res, _ = store.Take(ctx, merchant, payer)
	fmt.Println(limiter.AllAllowed(res), res[0].Remaining, res[1].Remaining)
	// Output:
	// true 70 20
	// false 70 20
}

func ExampleSlidingWindow() {
	clock := limiter.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := limiter.NewMemoryStore(limiter.MemoryOptions{Algorithm: limiter.SlidingWindow, Clock: clock})
	defer store.Close()
	ctx := context.Background()
	l := limiter.PerMinute(10)

	r, _ := limiter.Allow(ctx, store, "k", l, 10)
	fmt.Println(r.Allowed, r.Remaining)
	// 45s into the next window, 25% of the previous window still counts:
	// 2.5 + 7 fits in 10. One more unit fits once the previous window's
	// weight drops to 2, 48s into the window.
	clock.Advance(105 * time.Second)
	r, _ = limiter.Allow(ctx, store, "k", l, 7)
	fmt.Println(r.Allowed, r.Remaining)
	r, _ = limiter.Allow(ctx, store, "k", l, 1)
	fmt.Println(r.Allowed, r.RetryAfter)
	// Output:
	// true 0
	// true 0
	// false 3s
}

func ExampleNewFallbackStore() {
	primary := limiter.NewMemoryStore(limiter.MemoryOptions{}) // stands in for Redis
	local := limiter.NewMemoryStore(limiter.MemoryOptions{})
	defer primary.Close()
	defer local.Close()

	store := limiter.NewFallbackStore(primary, limiter.FallbackOptions{
		Fallback: local,
		Scale:    1.0 / 4, // four replicas share the global budget while degraded
		OnFallback: func(op, reason string, err error) {
			fmt.Println("degraded:", op, reason, err)
		},
	})
	r, err := limiter.Allow(context.Background(), store, "merchant:42", limiter.PerMinute(100), 1)
	fmt.Println(r.Allowed, r.Remaining, err)
	// Output:
	// true 99 <nil>
}
