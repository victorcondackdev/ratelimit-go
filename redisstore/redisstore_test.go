package redisstore_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/internal/redistest"
	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/redisstore"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

var epoch = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

type fixture struct {
	srv   *redistest.Server
	clock *limiter.ManualClock
	c     *resp.Client
	store *redisstore.Store
}

func setup(t testing.TB, opts redisstore.Options, sopts redistest.Options) *fixture {
	t.Helper()
	clock := limiter.NewManualClock(epoch)
	sopts.Clock = clock.Now
	srv, err := redistest.Start(sopts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	c := resp.NewClient(resp.Options{Addr: srv.Addr()})
	t.Cleanup(func() { c.Close() })
	return &fixture{srv: srv, clock: clock, c: c, store: redisstore.New(c, opts)}
}

func TestScriptMarker(t *testing.T) {
	if !strings.HasPrefix(redisstore.Script.Source(), redistest.GCRAMarker+"\n") {
		t.Fatal("gcra.lua must start with the marker the fake server recognizes")
	}
}

func TestKey(t *testing.T) {
	s := redisstore.New(nil, redisstore.Options{})
	if got := s.Key("merchant:42"); got != "rl:{merchant:42}" {
		t.Errorf("Key = %q", got)
	}
	if got := s.Key("pix:{merchant:42}:payer"); got != "rl:pix:{merchant:42}:payer" {
		t.Errorf("Key with own hash tag = %q", got)
	}
	if got := s.Key("odd{}key"); got != "rl:{odd{}key}" {
		t.Errorf("Key with empty tag = %q", got)
	}
	p := redisstore.New(nil, redisstore.Options{Prefix: "app:"})
	if got := p.Key("k"); got != "app:{k}" {
		t.Errorf("Key with prefix = %q", got)
	}
}

// TestMatchesMemoryStore drives the Redis store (through the fake server's
// emulation of the script) and the memory store with the same clock and the
// same random traffic, and requires identical results.
func TestMatchesMemoryStore(t *testing.T) {
	ctx := context.Background()
	limits := []limiter.Limit{
		limiter.PerSecond(10).WithBurst(5),
		{Rate: 3, Period: time.Second, Burst: 4}, // interval truncated to 333333us
		limiter.PerMinute(100),
	}
	for seed := int64(0); seed < 10; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			f := setup(t, redisstore.Options{}, redistest.Options{})
			mem := limiter.NewMemoryStore(limiter.MemoryOptions{Clock: f.clock, SweepInterval: -1})
			defer mem.Close()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 300; i++ {
				f.clock.Advance(time.Duration(rng.Int63n(200_000)) * time.Microsecond)
				var reqs []limiter.Request
				for n := 1 + rng.Intn(3); n > 0; n-- {
					l := limits[rng.Intn(len(limits))]
					reqs = append(reqs, limiter.Request{
						Key:   fmt.Sprint("k", rng.Intn(3)),
						Limit: l,
						Cost:  rng.Int63n(l.EffectiveBurst() + 1),
					})
				}
				var got, want []limiter.Result
				var gerr, werr error
				switch op := rng.Intn(10); {
				case op < 7:
					got, gerr = f.store.Take(ctx, reqs...)
					want, werr = mem.Take(ctx, reqs...)
				case op < 9:
					got, gerr = f.store.Peek(ctx, reqs...)
					want, werr = mem.Peek(ctx, reqs...)
				default:
					gerr, werr = f.store.Refund(ctx, reqs...), mem.Refund(ctx, reqs...)
				}
				if gerr != nil || werr != nil {
					t.Fatalf("step %d: redis err %v, memory err %v", i, gerr, werr)
				}
				if dupKeys(reqs) {
					// Duplicate keys in a denied batch are reported slightly
					// differently (see parseReply); compare decisions only.
					if limiter.AllAllowed(got) != limiter.AllAllowed(want) {
						t.Fatalf("step %d: decisions differ for %+v", i, reqs)
					}
					continue
				}
				for j := range want {
					if got[j] != want[j] {
						t.Fatalf("step %d req %d %+v:\nredis  %+v\nmemory %+v", i, j, reqs[j], got[j], want[j])
					}
				}
			}
		})
	}
}

func dupKeys(reqs []limiter.Request) bool {
	seen := map[string]bool{}
	for _, r := range reqs {
		if seen[r.Key] {
			return true
		}
		seen[r.Key] = true
	}
	return false
}

func TestBurstRetryAndExpiry(t *testing.T) {
	ctx := context.Background()
	f := setup(t, redisstore.Options{}, redistest.Options{})
	l := limiter.PerMinute(60).WithBurst(3)
	for i := 0; i < 3; i++ {
		r, err := limiter.Allow(ctx, f.store, "m", l, 1)
		if err != nil || !r.Allowed || r.Remaining != int64(2-i) {
			t.Fatalf("request %d: %+v %v", i, r, err)
		}
	}
	r, _ := limiter.Allow(ctx, f.store, "m", l, 1)
	if r.Allowed || r.RetryAfter != time.Second || r.ResetAfter != 3*time.Second {
		t.Fatalf("denied: %+v", r)
	}
	// The key is one integer, the TAT in microseconds, expiring when idle.
	v, ok := f.srv.Get("rl:{m}")
	if !ok || v != fmt.Sprint(epoch.Add(3*time.Second).UnixMicro()) {
		t.Fatalf("stored TAT %q", v)
	}
	if ttl := f.srv.TTL("rl:{m}"); ttl != 3*time.Second {
		t.Fatalf("TTL %s", ttl)
	}
	f.clock.Advance(3 * time.Second)
	if f.srv.Keys() != 0 {
		t.Fatal("idle key not expired")
	}
}

func TestBatchAllOrNothing(t *testing.T) {
	ctx := context.Background()
	f := setup(t, redisstore.Options{}, redistest.Options{})
	merchant := limiter.Request{Key: "merchant:1", Limit: limiter.PerMinute(10), Cost: 3}
	payer := limiter.Request{Key: "payer:1", Limit: limiter.PerMinute(4), Cost: 3}
	if res, err := f.store.Take(ctx, merchant, payer); err != nil || !limiter.AllAllowed(res) {
		t.Fatalf("%+v %v", res, err)
	}
	res, err := f.store.Take(ctx, merchant, payer)
	if err != nil || limiter.AllAllowed(res) || !res[0].Allowed || res[0].Remaining != 7 || res[1].Allowed {
		t.Fatalf("%+v %v", res, err)
	}
	if res[0].ResetAfter != 18*time.Second {
		t.Fatalf("unconsumed reset %s", res[0].ResetAfter)
	}
	if err := f.store.Refund(ctx, merchant, payer); err != nil {
		t.Fatal(err)
	}
	if f.srv.Keys() != 0 {
		t.Fatal("refund to idle should delete keys")
	}
	if res, err := f.store.Take(ctx); res != nil || err != nil {
		t.Fatal("empty batch")
	}
}

func TestRequestErrorsNeverReachRedis(t *testing.T) {
	f := setup(t, redisstore.Options{}, redistest.Options{})
	_, err := limiter.Allow(context.Background(), f.store, "k", limiter.PerSecond(1), 2)
	if !errors.Is(err, limiter.ErrCostExceedsBurst) {
		t.Fatal(err)
	}
	if f.srv.Count("EVALSHA") != 0 {
		t.Fatal("invalid request sent to Redis")
	}
}

func TestNoScriptRecovery(t *testing.T) {
	ctx := context.Background()
	f := setup(t, redisstore.Options{}, redistest.Options{})
	l := limiter.PerSecond(5)
	for i := 0; i < 3; i++ {
		f.srv.FlushScripts() // a restart or failover drops the script cache
		if _, err := limiter.Allow(ctx, f.store, "k", l, 1); err != nil {
			t.Fatal(err)
		}
	}
	if f.srv.Count("EVAL") != 3 || f.srv.Count("EVALSHA") != 3 {
		t.Fatalf("EVAL %d EVALSHA %d", f.srv.Count("EVAL"), f.srv.Count("EVALSHA"))
	}
}

func TestServerErrors(t *testing.T) {
	ctx := context.Background()
	f := setup(t, redisstore.Options{}, redistest.Options{})
	f.srv.FailNext("EVALSHA", "ERR boom")
	if _, err := limiter.Allow(ctx, f.store, "k", limiter.PerSecond(1), 1); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("got %v", err)
	}
	f.srv.SetDown(true)
	if _, err := limiter.Allow(ctx, f.store, "k", limiter.PerSecond(1), 1); err == nil || limiter.IsRequestError(err) {
		t.Fatalf("got %v", err)
	}
}

func TestObserve(t *testing.T) {
	var ops []string
	f := setup(t, redisstore.Options{Observe: func(op string, d time.Duration, err error) {
		ops = append(ops, op)
	}}, redistest.Options{})
	ctx := context.Background()
	r := limiter.Request{Key: "k", Limit: limiter.PerSecond(1), Cost: 1}
	_, _ = f.store.Take(ctx, r)
	_, _ = f.store.Peek(ctx, r)
	_ = f.store.Refund(ctx, r)
	if strings.Join(ops, ",") != "take,peek,refund" {
		t.Fatalf("observed %v", ops)
	}
}

func TestClusterMode(t *testing.T) {
	ctx := context.Background()
	f := setup(t, redisstore.Options{Cluster: true}, redistest.Options{Cluster: true})
	unclustered := redisstore.New(f.c, redisstore.Options{})
	a := limiter.Request{Key: "merchant:1", Limit: limiter.PerMinute(10), Cost: 2}
	b := limiter.Request{Key: "payer:1", Limit: limiter.PerMinute(3), Cost: 2}
	if resp.Slot(f.store.Key(a.Key)) == resp.Slot(f.store.Key(b.Key)) {
		t.Fatal("test keys must hash to different slots")
	}
	// Without cluster mode the server refuses the cross-slot script.
	if _, err := unclustered.Take(ctx, a, b); err == nil || !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Fatalf("want CROSSSLOT, got %v", err)
	}
	res, err := f.store.Take(ctx, a, b)
	if err != nil || !limiter.AllAllowed(res) || res[0].Remaining != 8 || res[1].Remaining != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	// b is now short; the check phase denies and a is untouched.
	res, err = f.store.Take(ctx, a, b)
	if err != nil || limiter.AllAllowed(res) || res[0].Remaining != 8 {
		t.Fatalf("%+v %v", res, err)
	}
	peek, err := f.store.Peek(ctx, a, b)
	if err != nil || limiter.AllAllowed(peek) || peek[0].Remaining != 8 {
		t.Fatalf("peek %+v %v", peek, err)
	}
	if res, _ := f.store.Peek(ctx, limiter.Request{Key: a.Key, Limit: a.Limit}, limiter.Request{Key: "other", Limit: a.Limit}); !limiter.AllAllowed(res) {
		t.Fatal("peek of idle keys")
	}
	if err := f.store.Refund(ctx, a, b); err != nil {
		t.Fatal(err)
	}
	if f.srv.Keys() != 0 {
		t.Fatalf("keys left after refund: %d", f.srv.Keys())
	}
	// Keys sharing a hash tag go in one atomic script.
	before := f.srv.Count("EVALSHA")
	co := []limiter.Request{
		{Key: "{merchant:1}:pix", Limit: limiter.PerMinute(10), Cost: 1},
		{Key: "{merchant:1}:payout", Limit: limiter.PerMinute(10), Cost: 1},
	}
	if res, err := f.store.Take(ctx, co...); err != nil || !limiter.AllAllowed(res) {
		t.Fatalf("%+v %v", res, err)
	}
	if f.srv.Count("EVALSHA")-before != 1 {
		t.Fatal("co-located keys should take one round trip")
	}
	// Errors in any slot group surface.
	f.srv.FailNext("EVALSHA", "ERR boom")
	if _, err := f.store.Take(ctx, a, b); err == nil {
		t.Fatal("take error")
	}
	f.srv.FailNext("EVALSHA", "ERR boom")
	if _, err := f.store.Peek(ctx, a, b); err == nil {
		t.Fatal("peek error")
	}
	f.srv.FailNext("EVALSHA", "ERR boom")
	if err := f.store.Refund(ctx, a, b); err == nil {
		t.Fatal("refund error")
	}
}

func TestExactUnderContention(t *testing.T) {
	f := setup(t, redisstore.Options{}, redistest.Options{})
	l := limiter.PerHour(1).WithBurst(100) // nothing replenishes during the test
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				r, err := limiter.Allow(context.Background(), f.store, "hot", l, 1)
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
	if got := allowed.Load(); got != 100 {
		t.Fatalf("allowed %d, want exactly 100", got)
	}
}

func TestScriptStillMissing(t *testing.T) {
	f := setup(t, redisstore.Options{}, redistest.Options{})
	f.srv.FailNext("EVALSHA", "NOSCRIPT No matching script")
	f.srv.FailNext("EVAL", "NOSCRIPT still missing")
	if _, err := limiter.Allow(context.Background(), f.store, "k", limiter.PerSecond(1), 1); !resp.IsNoScript(err) {
		t.Fatalf("got %v", err)
	}
}

func BenchmarkTakeFakeServer(b *testing.B) {
	f := setup(b, redisstore.Options{}, redistest.Options{})
	l := limiter.PerSecond(500_000).WithBurst(1 << 30)
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := f.store.Take(ctx, limiter.Request{Key: "k", Limit: l, Cost: 1}); err != nil {
			b.Fatal(err)
		}
	}
}
