package limiter

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var epoch = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

func TestGCRAStepTable(t *testing.T) {
	// interval 10, burst 3: tau = 30.
	tests := []struct {
		name           string
		tat, now, cost int64
		want           GCRAOutcome
	}{
		{"idle key", 0, 100, 1, GCRAOutcome{TAT: 110, Allowed: true, Remaining: 2, ResetAfter: 10}},
		{"full burst at once", 0, 100, 3, GCRAOutcome{TAT: 130, Allowed: true, Remaining: 0, ResetAfter: 30}},
		{"burst exhausted", 130, 100, 1, GCRAOutcome{TAT: 130, Remaining: 0, RetryAfter: 10, ResetAfter: 30}},
		{"one interval later", 130, 110, 1, GCRAOutcome{TAT: 140, Allowed: true, Remaining: 0, ResetAfter: 30}},
		{"partial interval", 130, 105, 1, GCRAOutcome{TAT: 130, Remaining: 0, RetryAfter: 5, ResetAfter: 25}},
		{"weighted cost denied keeps remaining", 120, 100, 2, GCRAOutcome{TAT: 120, Remaining: 1, RetryAfter: 10, ResetAfter: 20}},
		{"cost zero queries", 120, 100, 0, GCRAOutcome{TAT: 120, Allowed: true, Remaining: 1, ResetAfter: 20}},
		{"stale tat clamps to now", 50, 100, 1, GCRAOutcome{TAT: 110, Allowed: true, Remaining: 2, ResetAfter: 10}},
		{"over-full state from a lowered limit", 200, 100, 0, GCRAOutcome{TAT: 200, Remaining: 0, RetryAfter: 70, ResetAfter: 100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GCRAStep(tt.tat, tt.now, 10, 3, tt.cost); got != tt.want {
				t.Errorf("GCRAStep(%d, %d, 10, 3, %d) = %+v, want %+v", tt.tat, tt.now, tt.cost, got, tt.want)
			}
		})
	}
}

// propertyLimits have intervals that are whole microseconds, so the
// reference meter's drain rate is exactly the algorithms' interval.
var propertyLimits = []Limit{
	{Rate: 10, Period: 10 * time.Millisecond},              // T = 1ms, burst 10
	{Rate: 5, Period: 15 * time.Microsecond, Burst: 2},     // T = 3us, burst 2
	{Rate: 100, Period: time.Second, Burst: 1},             // no burst
	{Rate: 3, Period: 3 * time.Millisecond, Burst: 50},     // burst above rate
	{Rate: 1000, Period: 50 * time.Millisecond, Burst: 25}, // T = 50us
}

func randomSteps(rng *rand.Rand, l Limit, n int) (deltas, costs []int64) {
	iv := int64(l.Interval())
	capacity := l.EffectiveBurst()
	for i := 0; i < n; i++ {
		switch rng.Intn(4) {
		case 0:
			deltas = append(deltas, 0) // simultaneous requests
		case 1:
			deltas = append(deltas, rng.Int63n(iv+1))
		case 2:
			deltas = append(deltas, rng.Int63n(3*iv+1))
		default:
			deltas = append(deltas, rng.Int63n(iv*capacity*2+1)) // long idle
		}
		c := rng.Int63n(capacity + 1)
		if rng.Intn(3) == 0 {
			c = 1
		}
		costs = append(costs, c)
	}
	return deltas, costs
}

func TestPropertyMeterEquivalence(t *testing.T) {
	for _, alg := range []Algorithm{GCRA, TokenBucket} {
		for li, l := range propertyLimits {
			for seed := int64(0); seed < 40; seed++ {
				t.Run(fmt.Sprintf("%s/limit%d/seed%d", alg.Name(), li, seed), func(t *testing.T) {
					rng := rand.New(rand.NewSource(seed))
					deltas, costs := randomSteps(rng, l, 300)
					ref := newRefMeter(l)
					var s State
					now := epoch.UnixNano()
					for i := range deltas {
						now += deltas[i]
						next, got, exp := alg.Apply(s, now, l, costs[i])
						want := ref.take(now, costs[i])
						want.Limit = l
						if got != want {
							t.Fatalf("step %d (now=+%d cost=%d): got %+v, want %+v", i, now-epoch.UnixNano(), costs[i], got, want)
						}
						if got.Allowed {
							s = next
						}
						if exp > 0 && exp-now != int64(got.ResetAfter) {
							t.Fatalf("step %d: expireAt-now = %d, ResetAfter = %d", i, exp-now, got.ResetAfter)
						}
					}
				})
			}
		}
	}
}

func TestPropertyTokenBucketEqualsGCRA(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	l := Limit{Rate: 7, Period: 7 * time.Millisecond, Burst: 4}
	var sg, st State
	now := epoch.UnixNano()
	for i := 0; i < 20000; i++ {
		now += rng.Int63n(3 * int64(time.Millisecond))
		cost := rng.Int63n(5)
		ng, rg, _ := GCRA.Apply(sg, now, l, cost)
		nt, rt, _ := TokenBucket.Apply(st, now, l, cost)
		if rg != rt {
			t.Fatalf("step %d: gcra %+v, token bucket %+v", i, rg, rt)
		}
		sg, st = ng, nt
		if rng.Intn(10) == 0 {
			sg, _ = GCRA.Refund(sg, now, l, 1)
			st, _ = TokenBucket.Refund(st, now, l, 1)
		}
	}
}

func TestPropertySlidingWindow(t *testing.T) {
	limits := []Limit{
		{Rate: 10, Period: 50},
		{Rate: 3, Period: 40},
		{Rate: 1, Period: 17},
		{Rate: 25, Period: 64},
	}
	for li, l := range limits {
		for seed := int64(0); seed < 40; seed++ {
			t.Run(fmt.Sprintf("limit%d/seed%d", li, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				w := int64(l.Period)
				ref := &refWindowLog{window: w, rate: l.Rate}
				var s State
				now := int64(1_000_000)
				for i := 0; i < 250; i++ {
					switch rng.Intn(4) {
					case 0:
					case 1:
						now += rng.Int63n(w / 4)
					case 2:
						now += rng.Int63n(w)
					default:
						now += rng.Int63n(3 * w)
					}
					cost := rng.Int63n(l.Rate + 1)
					if rng.Intn(2) == 0 {
						cost = 1
					}
					next, got, _ := SlidingWindow.Apply(s, now, l, cost)
					want := ref.take(now, cost)
					want.Limit = l
					if got != want {
						t.Fatalf("step %d (now=%d cost=%d state=%v): got %+v, want %+v", i, now, cost, s, got, want)
					}
					if got.Allowed {
						s = next
					}
					// Trim the log; entries older than two windows never count.
					keep := ref.log[:0]
					for _, e := range ref.log {
						if e.at >= now-2*w {
							keep = append(keep, e)
						}
					}
					ref.log = keep
				}
			})
		}
	}
}

func TestSlidingWindowWeighting(t *testing.T) {
	l := Limit{Rate: 10, Period: time.Second}
	start := epoch.Truncate(time.Second).UnixNano()
	var s State
	for i := 0; i < 10; i++ {
		var r Result
		s, r, _ = SlidingWindow.Apply(s, start+int64(i), l, 1)
		if !r.Allowed {
			t.Fatalf("request %d denied", i)
		}
	}
	// Halfway into the next window the previous ten count as five.
	mid := start + int64(1500*time.Millisecond)
	_, r, _ := SlidingWindow.Apply(s, mid, l, 5)
	if !r.Allowed || r.Remaining != 0 {
		t.Fatalf("cost 5 at mid-window: %+v", r)
	}
	_, r, _ = SlidingWindow.Apply(s, mid, l, 6)
	if r.Allowed || r.RetryAfter != 100*time.Millisecond {
		t.Fatalf("cost 6 at mid-window: %+v", r)
	}
}

func TestRefund(t *testing.T) {
	l := Limit{Rate: 10, Period: 10 * time.Second} // T = 1s
	now := epoch.UnixNano()
	for _, alg := range []Algorithm{GCRA, TokenBucket, SlidingWindow} {
		t.Run(alg.Name(), func(t *testing.T) {
			s, r, _ := alg.Apply(State{}, now, l, 4)
			if !r.Allowed || r.Remaining != 6 {
				t.Fatalf("take: %+v", r)
			}
			s, _ = alg.Refund(s, now, l, 3)
			_, r, _ = alg.Apply(s, now, l, 0)
			if r.Remaining != 9 {
				t.Fatalf("after refund remaining = %d, want 9", r.Remaining)
			}
			s, exp := alg.Refund(s, now, l, 5)
			if s != (State{}) || exp != 0 {
				t.Fatalf("over-refund should reset to idle, got %v exp %d", s, exp)
			}
		})
	}
	t.Run("sliding_window previous window", func(t *testing.T) {
		w := Limit{Rate: 10, Period: time.Second}
		start := epoch.Truncate(time.Second).UnixNano()
		s, _, _ := SlidingWindow.Apply(State{}, start, w, 4)
		s, exp := SlidingWindow.Refund(s, start+int64(1100*time.Millisecond), w, 1)
		if s[1] != 3 || s[2] != 0 || exp != start+2*int64(time.Second) {
			t.Fatalf("state %v exp %d", s, exp)
		}
	})
}

func TestLimitValidate(t *testing.T) {
	tests := []struct {
		l  Limit
		ok bool
	}{
		{PerSecond(100), true},
		{PerMinute(60).WithBurst(10), true},
		{PerHour(1), true},
		{PerDay(1), true},
		{Limit{Rate: 0, Period: time.Second}, false},
		{Limit{Rate: 1, Period: 0}, false},
		{Limit{Rate: 1, Period: time.Second, Burst: -1}, false},
		{Limit{Rate: 2_000_000, Period: time.Second}, false},
		{Limit{Rate: 1, Period: 1 << 50, Burst: 1 << 20}, false},
		{Limit{Rate: 1 << 40, Period: 1 << 40}, false},
	}
	for _, tt := range tests {
		err := tt.l.Validate()
		if (err == nil) != tt.ok {
			t.Errorf("%v.Validate() = %v, want ok=%v", tt.l, err, tt.ok)
		}
	}
}

func TestLimitHelpers(t *testing.T) {
	l := Limit{Rate: 3, Period: time.Second}
	if got := l.Interval(); got != 333333*time.Microsecond {
		t.Errorf("Interval = %s", got)
	}
	if (Limit{}).Interval() != 0 {
		t.Error("zero limit interval")
	}
	if got := PerMinute(100).String(); got != "100/1m0s" {
		t.Errorf("String = %q", got)
	}
	if got := PerMinute(100).WithBurst(20).String(); got != "100/1m0s burst 20" {
		t.Errorf("String = %q", got)
	}
	s := PerMinute(100).WithBurst(20).Scale(0.25)
	if s.Rate != 25 || s.Burst != 5 {
		t.Errorf("Scale = %+v", s)
	}
	if s := PerMinute(3).Scale(0.1); s.Rate != 1 || s.Burst != 0 {
		t.Errorf("Scale floor = %+v", s)
	}
	if s := PerMinute(1 << 62).Scale(8); s.Rate <= 0 {
		t.Errorf("Scale overflow = %+v", s)
	}
}

func TestValidateRequests(t *testing.T) {
	ok := []Request{{Key: "a", Limit: PerSecond(5), Cost: 5}}
	if err := ValidateRequests(ok); err != nil {
		t.Fatal(err)
	}
	for _, r := range []Request{
		{Key: "a", Limit: PerSecond(5), Cost: 6},
		{Key: "a", Limit: PerSecond(5), Cost: -1},
		{Key: "a", Limit: Limit{}, Cost: 1},
	} {
		err := ValidateRequests([]Request{r})
		if err == nil || !IsRequestError(err) {
			t.Errorf("ValidateRequests(%+v) = %v", r, err)
		}
	}
	if IsRequestError(context.Canceled) {
		t.Error("context error is not a request error")
	}
}
