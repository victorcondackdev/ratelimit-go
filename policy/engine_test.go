package policy

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/limiter"
)

var epoch = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

func memStore(t *testing.T, clock limiter.Clock) *limiter.MemoryStore {
	s := limiter.NewMemoryStore(limiter.MemoryOptions{Clock: clock, SweepInterval: -1})
	t.Cleanup(func() { s.Close() })
	return s
}

func pixRules() []Rule {
	return []Rule{
		{Name: "merchant-pix", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(100), Endpoints: []string{"POST /pix", "POST /payouts"}},
		{Name: "payer-pix", Dimensions: []Dimension{Payer}, Limit: limiter.PerHour(10), Endpoints: []string{"POST /pix", "POST /payouts"}},
		{Name: "ip", Dimensions: []Dimension{IP}, Limit: limiter.PerSecond(20), Unweighted: true},
	}
}

func mustEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func remainingOf(d *Decision, rule string) int64 {
	for _, o := range d.Outcomes {
		if o.Rule == rule {
			return o.Result.Remaining
		}
	}
	return -1
}

func TestEngineCombinedLimits(t *testing.T) {
	ctx := context.Background()
	clock := limiter.NewManualClock(epoch)
	e := mustEngine(t, Config{Rules: pixRules(), Store: memStore(t, clock)})
	req := Request{Merchant: "m1", Payer: "123.456.789-09", Endpoint: "POST /pix", IP: "10.0.0.1"}

	d, err := e.Allow(ctx, req)
	if err != nil || !d.Allowed || len(d.Outcomes) != 3 {
		t.Fatalf("%+v %v", d, err)
	}
	if remainingOf(d, "merchant-pix") != 99 || remainingOf(d, "payer-pix") != 9 || remainingOf(d, "ip") != 19 {
		t.Fatalf("remaining %+v", d.Outcomes)
	}
	// A bulk payout of 9 for the same payer: weighted rules charge 9, the IP
	// rule charges 1.
	bulk := req
	bulk.Endpoint, bulk.Cost = "POST /payouts", 9
	d, _ = e.Allow(ctx, bulk)
	if !d.Allowed || remainingOf(d, "merchant-pix") != 90 || remainingOf(d, "payer-pix") != 0 || remainingOf(d, "ip") != 18 {
		t.Fatalf("bulk %+v", d.Outcomes)
	}
	// The payer is exhausted: denied, and the merchant is not charged.
	d, _ = e.Allow(ctx, req)
	if d.Allowed {
		t.Fatal("payer limit not enforced")
	}
	if o, _ := d.DeniedBy(); o.Rule != "payer-pix" {
		t.Fatalf("denied by %q", o.Rule)
	}
	if o, ok := d.Tightest(); !ok || o.Rule != "payer-pix" || o.Result.RetryAfter != 6*time.Minute {
		t.Fatalf("tightest %+v", o)
	}
	if w, ok := d.RetryAfter(); !ok || w != 6*time.Minute {
		t.Fatalf("retry after %s", w)
	}
	other := Request{Merchant: "m1", Payer: "98765432100", Endpoint: "POST /pix", IP: "10.0.0.2"}
	d, _ = e.Allow(ctx, other)
	if !d.Allowed || remainingOf(d, "merchant-pix") != 89 {
		t.Fatalf("merchant charged by a denied request: %+v", d.Outcomes)
	}
	// Formatting does not create a new payer.
	unformatted := req
	unformatted.Payer = "12345678909"
	if d, _ := e.Allow(ctx, unformatted); d.Allowed {
		t.Fatal("same payer, different formatting, must share the limit")
	}
	// Rules restricted to endpoints, and rules missing a dimension, are skipped.
	d, _ = e.Allow(ctx, Request{Endpoint: "GET /balance", IP: "10.0.0.1"})
	if !d.Allowed || len(d.Outcomes) != 1 || d.Outcomes[0].Rule != "ip" {
		t.Fatalf("outcomes %+v", d.Outcomes)
	}
	d.Release()
	d, _ = e.Allow(ctx, Request{Endpoint: "GET /balance"})
	if !d.Allowed || len(d.Outcomes) != 0 {
		t.Fatal("no applicable rule must allow")
	}
	if _, ok := d.Tightest(); ok {
		t.Fatal("no rate outcome")
	}
}

func TestEngineCostExceedsBurst(t *testing.T) {
	clock := limiter.NewManualClock(epoch)
	store := memStore(t, clock)
	e := mustEngine(t, Config{Rules: pixRules(), Store: store})
	d, err := e.Allow(context.Background(), Request{Merchant: "m", Payer: "12345678909", Endpoint: "POST /payouts", IP: "1.1.1.1", Cost: 11})
	if err != nil || d.Allowed {
		t.Fatalf("%+v %v", d, err)
	}
	o, _ := d.DeniedBy()
	if o.Rule != "payer-pix" || !o.CostExceeded {
		t.Fatalf("%+v", o)
	}
	if _, ok := d.RetryAfter(); ok {
		t.Fatal("waiting cannot help an oversized request")
	}
	if store.Stats().Keys != 0 {
		t.Fatal("an impossible request consumed capacity")
	}
}

func TestEnginePlans(t *testing.T) {
	ctx := context.Background()
	clock := limiter.NewManualClock(epoch)
	rules := []Rule{
		{Name: "merchant", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(100)},
		{Name: "endpoint", Dimensions: []Dimension{Merchant, Endpoint}, Limit: limiter.PerMinute(10)},
		{Name: "payouts", Dimensions: []Dimension{Merchant}, MaxInFlight: 2},
	}
	e := mustEngine(t, Config{
		Rules: rules,
		Store: memStore(t, clock),
		Plans: []Plan{
			{Name: "starter"},
			{Name: "enterprise", Scale: 10, Limits: map[string]limiter.Limit{"endpoint": limiter.PerMinute(50)}, MaxInFlight: map[string]int{"payouts": 8}},
			{Name: "growth", Scale: 2},
		},
		DefaultPlan: "starter",
	})
	tests := []struct {
		plan               string
		merchant, endpoint int64
		maxInFlight        int
	}{
		{"", 99, 9, 2},
		{"unknown", 99, 9, 2},
		{"starter", 99, 9, 2},
		{"enterprise", 999, 49, 8},
		{"growth", 199, 19, 4},
	}
	for i, tt := range tests {
		d, err := e.Allow(ctx, Request{Merchant: string(rune('a' + i)), Endpoint: "POST /pix", Plan: tt.plan})
		if err != nil || !d.Allowed {
			t.Fatalf("%s: %+v %v", tt.plan, d, err)
		}
		if remainingOf(d, "merchant") != tt.merchant || remainingOf(d, "endpoint") != tt.endpoint || d.Outcomes[2].MaxInFlight != tt.maxInFlight {
			t.Errorf("%s: %+v", tt.plan, d.Outcomes)
		}
		d.Release()
	}
}

func TestEngineConcurrency(t *testing.T) {
	ctx := context.Background()
	clock := limiter.NewManualClock(epoch)
	store := memStore(t, clock)
	e := mustEngine(t, Config{Store: store, Rules: []Rule{
		{Name: "payouts-inflight", Dimensions: []Dimension{Merchant}, MaxInFlight: 2, Endpoints: []string{"POST /payouts"}},
		{Name: "merchant", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(100)},
	}})
	req := Request{Merchant: "m", Endpoint: "POST /payouts"}
	d1, _ := e.Allow(ctx, req)
	d2, _ := e.Allow(ctx, req)
	d3, _ := e.Allow(ctx, req)
	if !d1.Allowed || !d2.Allowed || d3.Allowed {
		t.Fatal("third concurrent payout must be denied")
	}
	if o, _ := d3.DeniedBy(); !o.Concurrency || o.Rule != "payouts-inflight" {
		t.Fatalf("%+v", o)
	}
	if _, ok := d3.RetryAfter(); ok {
		t.Fatal("concurrency denials have no rate wait")
	}
	if o, ok := d3.Tightest(); ok {
		t.Fatalf("concurrency denial evaluated no rate rule, got %+v", o)
	}
	d3.Release() // no-op
	if remaining := remainingOf(d2, "merchant"); remaining != 98 {
		t.Fatalf("remaining %d", remaining)
	}
	// The concurrency denial did not consume the rate limit.
	peek, _ := store.Peek(ctx, limiter.Request{Key: d2.Outcomes[1].Key, Limit: limiter.PerMinute(100)})
	if peek[0].Remaining != 98 {
		t.Fatalf("rate consumed by a concurrency denial: %d", peek[0].Remaining)
	}
	d1.Release()
	d1.Release()
	if e.Concurrency().InFlight(d1.Outcomes[0].Key) != 1 {
		t.Fatal("release not idempotent")
	}
	d4, _ := e.Allow(ctx, req)
	if !d4.Allowed {
		t.Fatal("slot not freed")
	}
	d2.Release()
	d4.Release()
	if e.Concurrency().InFlight(d1.Outcomes[0].Key) != 0 {
		t.Fatal("slots leaked")
	}
	var nilDecision *Decision
	nilDecision.Release()
}

func TestEngineRateDenialReleasesSlots(t *testing.T) {
	ctx := context.Background()
	clock := limiter.NewManualClock(epoch)
	e := mustEngine(t, Config{Store: memStore(t, clock), Rules: []Rule{
		{Name: "inflight", Dimensions: []Dimension{Merchant}, MaxInFlight: 5},
		{Name: "rate", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(1)},
	}})
	d, _ := e.Allow(ctx, Request{Merchant: "m"})
	d.Release()
	d, _ = e.Allow(ctx, Request{Merchant: "m"})
	if d.Allowed {
		t.Fatal("rate not enforced")
	}
	if e.Concurrency().InFlight(d.Outcomes[0].Key) != 0 {
		t.Fatal("rate denial kept a concurrency slot")
	}
}

type failingStore struct{ limiter.Store }

var errDown = errors.New("store down")

func (failingStore) Take(context.Context, ...limiter.Request) ([]limiter.Result, error) {
	return nil, errDown
}

func TestEngineStoreError(t *testing.T) {
	e := mustEngine(t, Config{Store: failingStore{}, Rules: []Rule{
		{Name: "inflight", Dimensions: []Dimension{Merchant}, MaxInFlight: 1},
		{Name: "rate", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(10)},
	}})
	if _, err := e.Allow(context.Background(), Request{Merchant: "m"}); !errors.Is(err, errDown) {
		t.Fatalf("got %v", err)
	}
	if e.Concurrency().InFlight("inflight:merchant=m") != 0 {
		t.Fatal("store error kept a concurrency slot")
	}
}

func TestEngineMultipleStores(t *testing.T) {
	ctx := context.Background()
	clock := limiter.NewManualClock(epoch)
	shared := memStore(t, clock) // stands in for Redis
	local := memStore(t, clock)
	e := mustEngine(t, Config{Store: shared, Rules: []Rule{
		{Name: "merchant", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(10)},
		{Name: "ip", Dimensions: []Dimension{IP}, Limit: limiter.PerMinute(2), Store: local},
	}})
	for i := 0; i < 2; i++ {
		if d, _ := e.Allow(ctx, Request{Merchant: "m", IP: "1.2.3.4"}); !d.Allowed {
			t.Fatal("denied early")
		}
	}
	d, _ := e.Allow(ctx, Request{Merchant: "m", IP: "1.2.3.4"})
	if d.Allowed {
		t.Fatal("local ip rule not enforced")
	}
	if got := remainingOf(d, "merchant"); got != 8 {
		t.Fatalf("merchant remaining %d, want 8 (unconsumed)", got)
	}
	d, _ = e.Allow(ctx, Request{Merchant: "m", IP: "5.6.7.8"})
	if !d.Allowed || remainingOf(d, "merchant") != 7 {
		t.Fatalf("%+v", d.Outcomes)
	}
}

func TestEngineKeys(t *testing.T) {
	clock := limiter.NewManualClock(epoch)
	rules := []Rule{{Name: "r", Dimensions: []Dimension{APIKey, Payer, Merchant}, Limit: limiter.PerMinute(10)}}
	req := Request{APIKey: "sk_live_abcdefghijkl", Payer: "123.456.789-09", Merchant: "acme:{br}"}
	plain := mustEngine(t, Config{Rules: rules, Store: memStore(t, clock), KeyPrefix: "pay:"})
	keyed := mustEngine(t, Config{Rules: rules, Store: memStore(t, clock), KeySecret: []byte("k1")})
	d1, _ := plain.Allow(context.Background(), req)
	d2, _ := keyed.Allow(context.Background(), req)
	k1, k2 := d1.Outcomes[0].Key, d2.Outcomes[0].Key
	for _, k := range []string{k1, k2} {
		if strings.Contains(k, "sk_live") || strings.Contains(k, "456") || strings.ContainsAny(k, "{}") {
			t.Errorf("key leaks data or hash tags: %q", k)
		}
	}
	if !strings.HasPrefix(k1, "pay:r:api_key=") || !strings.HasSuffix(k1, ":merchant=acme%3A%7Bbr%7D") {
		t.Errorf("key %q", k1)
	}
	if strings.Split(k1, ":")[3] == strings.Split(k2, ":")[2] {
		t.Error("the key secret must change pseudonyms")
	}
}

func TestConfigValidation(t *testing.T) {
	store := limiter.NewMemoryStore(limiter.MemoryOptions{SweepInterval: -1})
	ok := Rule{Name: "r", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(1)}
	bad := []Config{
		{Rules: []Rule{{Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(1)}}, Store: store},
		{Rules: []Rule{{Name: "a:b", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(1)}}, Store: store},
		{Rules: []Rule{ok, ok}, Store: store},
		{Rules: []Rule{{Name: "r", Limit: limiter.PerMinute(1)}}, Store: store},
		{Rules: []Rule{{Name: "r", Dimensions: []Dimension{"country"}, Limit: limiter.PerMinute(1)}}, Store: store},
		{Rules: []Rule{{Name: "r", Dimensions: []Dimension{Merchant}, Limit: limiter.PerMinute(1), MaxInFlight: 1}}, Store: store},
		{Rules: []Rule{{Name: "r", Dimensions: []Dimension{Merchant}}}, Store: store},
		{Rules: []Rule{ok}},
		{Rules: []Rule{ok}, Store: store, Plans: []Plan{{}}},
		{Rules: []Rule{ok}, Store: store, Plans: []Plan{{Name: "p", Scale: -1}}},
		{Rules: []Rule{ok}, Store: store, Plans: []Plan{{Name: "p", Limits: map[string]limiter.Limit{"x": limiter.PerMinute(1)}}}},
		{Rules: []Rule{ok}, Store: store, Plans: []Plan{{Name: "p", Limits: map[string]limiter.Limit{"r": {}}}}},
		{Rules: []Rule{ok}, Store: store, Plans: []Plan{{Name: "p", MaxInFlight: map[string]int{"r": 1}}}},
		{Rules: []Rule{ok}, Store: store, DefaultPlan: "missing"},
	}
	for i, cfg := range bad {
		if _, err := New(cfg); !errors.Is(err, ErrConfig) {
			t.Errorf("config %d: got %v", i, err)
		}
	}
	e, err := New(Config{Rules: []Rule{ok, {Name: "c", Dimensions: []Dimension{IP}, MaxInFlight: 3}}, Store: store})
	if err != nil || len(e.Rules()) != 2 {
		t.Fatal(err)
	}
}

func TestMasking(t *testing.T) {
	tests := map[string]string{
		"":                   "",
		"123.456.789-09":     "***.456.789-**",
		"12345678909":        "***.456.789-**",
		"12.345.678/0001-95": "**.345.678/****-**",
		"12.ABC.345/01DE-35": "**.ABC.345/****-**",
		"9":                  "*",
		"12345":              "***45",
	}
	for in, want := range tests {
		if got := MaskDocument(in); got != want {
			t.Errorf("MaskDocument(%q) = %q, want %q", in, got, want)
		}
	}
	if MaskSecret("") != "" || MaskSecret("short") != "****" || MaskSecret("sk_live_abcdefgh") != "sk_l…(16)" {
		t.Error("MaskSecret")
	}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	log.Info("x", "req", Request{APIKey: "sk_live_abcdefgh", Payer: "123.456.789-09", Merchant: "m", Cost: 0})
	out := buf.String()
	if strings.Contains(out, "abcdefgh") || strings.Contains(out, "123.456") || !strings.Contains(out, "req.payer=***.456.789-**") || !strings.Contains(out, "req.cost=1") {
		t.Errorf("log leaks or misses fields: %s", out)
	}
	if (Request{}).Value("other") != "" {
		t.Error("unknown dimension value")
	}
}

func TestConcurrencyRace(t *testing.T) {
	c := NewConcurrency()
	var wg sync.WaitGroup
	var mu sync.Mutex
	peak, cur := 0, 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				release, denied := c.Acquire(Slot{Key: "k", Max: 3}, Slot{Key: "k2", Max: 100})
				if denied >= 0 {
					continue
				}
				mu.Lock()
				cur++
				peak = max(peak, cur)
				mu.Unlock()
				mu.Lock()
				cur--
				mu.Unlock()
				release()
			}
		}()
	}
	wg.Wait()
	if peak > 3 || c.InFlight("k") != 0 || c.InFlight("k2") != 0 {
		t.Fatalf("peak %d, in flight %d", peak, c.InFlight("k"))
	}
	// Duplicate slots count twice.
	if _, denied := c.Acquire(Slot{Key: "d", Max: 1}, Slot{Key: "d", Max: 1}); denied != 1 {
		t.Fatalf("duplicate slot: denied %d", denied)
	}
}
