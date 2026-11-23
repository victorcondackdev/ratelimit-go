package limiter

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkGCRAStep(b *testing.B) {
	var tat int64
	now := epoch.UnixNano()
	for i := 0; i < b.N; i++ {
		o := GCRAStep(tat, now, int64(time.Millisecond), 100, 1)
		if o.Allowed {
			tat = o.TAT
		}
		now += int64(time.Millisecond)
	}
}

func benchStore(b *testing.B, alg Algorithm) *MemoryStore {
	s := NewMemoryStore(MemoryOptions{Algorithm: alg, SweepInterval: -1})
	b.Cleanup(func() { s.Close() })
	return s
}

// BenchmarkMemoryStoreParallel measures Take from GOMAXPROCS goroutines,
// on one contended key and spread over 10k keys.
func BenchmarkMemoryStoreParallel(b *testing.B) {
	l := PerSecond(1_000_000 / 2).WithBurst(1 << 20)
	keys := make([]string, 10_000)
	for i := range keys {
		keys[i] = "merchant:" + strconv.Itoa(i)
	}
	for _, alg := range []Algorithm{GCRA, TokenBucket, SlidingWindow} {
		for _, mode := range []string{"hot-key", "10k-keys"} {
			b.Run(alg.Name()+"/"+mode, func(b *testing.B) {
				s := benchStore(b, alg)
				ctx := context.Background()
				var n atomic.Uint64
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := int(n.Add(1)) * 7919
					for pb.Next() {
						key := "hot"
						if mode == "10k-keys" {
							key = keys[i%len(keys)]
							i++
						}
						if _, err := s.Take(ctx, Request{Key: key, Limit: l, Cost: 1}); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

// BenchmarkMemoryStoreBatch measures an atomic three-limit decision
// (merchant, payer, IP), the shape of a payment request.
func BenchmarkMemoryStoreBatch(b *testing.B) {
	s := benchStore(b, GCRA)
	ctx := context.Background()
	l := PerSecond(500_000).WithBurst(1 << 20)
	var n atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := n.Add(1) * 104729
		for pb.Next() {
			i++
			m := strconv.FormatUint(i%1000, 10)
			if _, err := s.Take(ctx,
				Request{Key: "merchant:" + m, Limit: l, Cost: 3},
				Request{Key: "payer:" + strconv.FormatUint(i%100_000, 10), Limit: l, Cost: 3},
				Request{Key: "ip:" + m, Limit: l, Cost: 1},
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}
