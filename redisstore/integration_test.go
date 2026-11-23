//go:build integration

package redisstore_test

// Integration tests against a real Redis, which runs the actual Lua script.
// Run with:
//
//	REDIS_URL=redis://localhost:6379/0 go test -tags integration ./redisstore
//
// The tests use database 15 by default and flush it.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/redisstore"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

func realRedis(t *testing.T, protocol int) (*resp.Client, *redisstore.Store) {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set")
	}
	opts, err := resp.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	if opts.DB == 0 {
		opts.DB = 15
	}
	opts.Protocol = protocol
	c := resp.NewClient(opts)
	t.Cleanup(func() { c.Close() })
	if _, err := c.Do(context.Background(), "FLUSHDB"); err != nil {
		t.Fatal(err)
	}
	return c, redisstore.New(c, redisstore.Options{Prefix: fmt.Sprintf("it%d:", time.Now().UnixNano())})
}

// TestLuaMatchesGo runs the real script and replays every step through
// limiter.GCRAStep using the time the script itself read from Redis.
func TestLuaMatchesGo(t *testing.T) {
	for _, proto := range []int{2, 3} {
		t.Run(fmt.Sprint("RESP", proto), func(t *testing.T) {
			c, s := realRedis(t, proto)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(1))
			key := s.Key("lua")
			const iv, burst = 2000, 5 // 2ms interval
			var tat int64
			for i := 0; i < 400; i++ {
				if rng.Intn(3) == 0 {
					time.Sleep(time.Duration(rng.Intn(3000)) * time.Microsecond)
				}
				cost := rng.Int63n(burst + 1)
				mode := []string{"take", "take", "take", "peek", "refund"}[rng.Intn(5)]
				v, err := c.Run(ctx, redisstore.Script, []string{key}, mode, strconv.Itoa(iv), strconv.Itoa(burst), strconv.FormatInt(cost, 10))
				if err != nil {
					t.Fatal(err)
				}
				nums := make([]int64, len(v.Elems))
				for j, e := range v.Elems {
					if nums[j], err = e.Int64(); err != nil {
						t.Fatal(err)
					}
				}
				now := nums[1]
				if mode == "refund" {
					tat -= iv * cost
					if tat <= now {
						tat = 0
					}
				} else {
					o := limiter.GCRAStep(tat, now, iv, burst, cost)
					want := []int64{0, now, 0, o.Remaining, o.RetryAfter, o.ResetAfter}
					if o.Allowed {
						want[0], want[2] = 1, 1
						if mode == "take" && cost > 0 {
							tat = o.TAT
						}
					}
					for j := range want {
						if nums[j] != want[j] {
							t.Fatalf("step %d %s cost %d: lua %v, go %v", i, mode, cost, nums, want)
						}
					}
				}
				// TIME is read after GET, so a TAT beyond it was live at GET.
				vs, err := c.Pipeline(ctx, []string{"GET", key}, []string{"TIME"})
				if err != nil {
					t.Fatal(err)
				}
				got := vs[0]
				sec, _ := vs[1].Elems[0].Int64()
				usec, _ := vs[1].Elems[1].Int64()
				if tat <= sec*1_000_000+usec {
					continue // idle or about to expire
				}
				if s, _ := got.Text(); s != strconv.FormatInt(tat, 10) {
					t.Fatalf("step %d: stored %v, want %d", i, got, tat)
				}
			}
		})
	}
}

func TestIntegrationBurstAndTTL(t *testing.T) {
	c, s := realRedis(t, 2)
	ctx := context.Background()
	l := limiter.PerHour(3600).WithBurst(10) // 1 per second
	for i := 0; i < 10; i++ {
		if r, err := limiter.Allow(ctx, s, "burst", l, 1); err != nil || !r.Allowed {
			t.Fatalf("request %d: %+v %v", i, r, err)
		}
	}
	r, err := limiter.Allow(ctx, s, "burst", l, 1)
	if err != nil || r.Allowed {
		t.Fatalf("11th: %+v %v", r, err)
	}
	if r.RetryAfter <= 900*time.Millisecond || r.RetryAfter > time.Second {
		t.Fatalf("RetryAfter %s", r.RetryAfter)
	}
	pttl, err := c.Do(ctx, "PTTL", s.Key("burst"))
	if err != nil || pttl.Int < 9000 || pttl.Int > 10000 {
		t.Fatalf("PTTL %v %v", pttl, err)
	}
	// The stored TAT must be a plain integer, not Lua's %.14g rendering.
	v, _ := c.Do(ctx, "GET", s.Key("burst"))
	if _, err := strconv.ParseInt(v.Str, 10, 64); err != nil {
		t.Fatalf("stored TAT %q is not an integer", v.Str)
	}
}

func TestIntegrationBatchAndRefund(t *testing.T) {
	_, s := realRedis(t, 3)
	ctx := context.Background()
	merchant := limiter.Request{Key: "merchant:1", Limit: limiter.PerHour(10), Cost: 3}
	payer := limiter.Request{Key: "payer:1", Limit: limiter.PerHour(4), Cost: 3}
	if res, err := s.Take(ctx, merchant, payer); err != nil || !limiter.AllAllowed(res) {
		t.Fatalf("%+v %v", res, err)
	}
	res, err := s.Take(ctx, merchant, payer)
	if err != nil || limiter.AllAllowed(res) || res[0].Remaining != 7 {
		t.Fatalf("%+v %v", res, err)
	}
	if err := s.Refund(ctx, merchant, payer); err != nil {
		t.Fatal(err)
	}
	res, err = s.Peek(ctx, limiter.Request{Key: "merchant:1", Limit: merchant.Limit})
	if err != nil || res[0].Remaining != 10 {
		t.Fatalf("after refund %+v %v", res, err)
	}
}

func TestIntegrationNoScript(t *testing.T) {
	c, s := realRedis(t, 2)
	ctx := context.Background()
	if _, err := c.Do(ctx, "SCRIPT", "FLUSH"); err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Allow(ctx, s, "k", limiter.PerSecond(5), 1); err != nil {
		t.Fatal(err)
	}
	v, err := c.Do(ctx, "SCRIPT", "EXISTS", redisstore.Script.SHA())
	if err != nil || v.Elems[0].Int != 1 {
		t.Fatalf("script not cached after fallback: %v %v", v, err)
	}
}

func TestIntegrationExactUnderContention(t *testing.T) {
	_, s := realRedis(t, 2)
	l := limiter.PerDay(1).WithBurst(250)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				r, err := limiter.Allow(context.Background(), s, "hot", l, 1)
				if err != nil {
					t.Error(err)
					return
				}
				if r.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 250 {
		t.Fatalf("allowed %d, want exactly 250", got)
	}
}

func BenchmarkIntegrationTake(b *testing.B) {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		b.Skip("REDIS_URL not set")
	}
	opts, err := resp.ParseURL(url)
	if err != nil {
		b.Fatal(err)
	}
	c := resp.NewClient(opts)
	defer c.Close()
	s := redisstore.New(c, redisstore.Options{Prefix: "bench:"})
	l := limiter.PerSecond(500_000).WithBurst(1 << 30)
	ctx := context.Background()
	b.Run("one-key", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := s.Take(ctx, limiter.Request{Key: "k", Limit: l, Cost: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("three-keys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := s.Take(ctx,
				limiter.Request{Key: "merchant", Limit: l, Cost: 1},
				limiter.Request{Key: "payer", Limit: l, Cost: 1},
				limiter.Request{Key: "ip", Limit: l, Cost: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("one-key-parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := s.Take(ctx, limiter.Request{Key: "k", Limit: l, Cost: 1}); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
