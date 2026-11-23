package httplimit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/internal/redistest"
	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/metrics"
	"github.com/victorcondackdev/ratelimit-go/policy"
	"github.com/victorcondackdev/ratelimit-go/redisstore"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

var epoch = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

func newEngine(t *testing.T, store limiter.Store, rules ...policy.Rule) *policy.Engine {
	t.Helper()
	e, err := policy.New(policy.Config{Store: store, Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func memStore(t *testing.T, clock limiter.Clock) *limiter.MemoryStore {
	s := limiter.NewMemoryStore(limiter.MemoryOptions{Clock: clock, SweepInterval: -1})
	t.Cleanup(func() { s.Close() })
	return s
}

var ok = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })

func do(h http.Handler, method, path string, headers ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func problem(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if ct := w.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type %q", ct)
	}
	var p map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestHeadersAndDenial(t *testing.T) {
	clock := limiter.NewManualClock(epoch)
	e := newEngine(t, memStore(t, clock),
		policy.Rule{Name: "merchant", Dimensions: []policy.Dimension{policy.Merchant}, Limit: limiter.PerMinute(60).WithBurst(2)},
		policy.Rule{Name: "ip", Dimensions: []policy.Dimension{policy.IP}, Limit: limiter.PerMinute(100), Unweighted: true},
	)
	reg := metrics.New()
	h := Middleware(e, Options{Metrics: reg, Logger: slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))})(ok)

	w := do(h, "POST", "/pix", "X-Merchant-ID", "m1")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status %d", w.Code)
	}
	want := map[string]string{
		"RateLimit-Policy":      `"merchant";q=2;w=2, "ip";q=100;w=60`,
		"RateLimit":             `"merchant";r=1;t=1, "ip";r=99;t=1`,
		"X-RateLimit-Limit":     "2",
		"X-RateLimit-Remaining": "1",
		"X-RateLimit-Reset":     "1",
	}
	for k, v := range want {
		if got := w.Header().Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	do(h, "POST", "/pix", "X-Merchant-ID", "m1")
	clock.Advance(250 * time.Millisecond)
	w = do(h, "POST", "/pix", "X-Merchant-ID", "m1")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d", w.Code)
	}
	if w.Header().Get("Retry-After") != "1" || w.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("headers %v", w.Header())
	}
	p := problem(t, w)
	if p["status"].(float64) != 429 || p["policy"] != "merchant" || p["type"] != "about:blank" || !strings.Contains(p["detail"].(string), "750ms") {
		t.Fatalf("problem %v", p)
	}
	var out bytes.Buffer
	_, _ = reg.WriteTo(&out)
	for _, line := range []string{
		`ratelimit_decisions_total{policy="merchant",decision="allowed"} 2`,
		`ratelimit_decisions_total{policy="merchant",decision="denied"} 1`,
		`ratelimit_decisions_total{policy="ip",decision="allowed"} 3`,
	} {
		if !strings.Contains(out.String(), line) {
			t.Errorf("metrics missing %q:\n%s", line, out.String())
		}
	}

	quiet := Middleware(e, Options{DisableDraftHeaders: true, DisableLegacyHeaders: true})(ok)
	if w := do(quiet, "GET", "/", "X-Merchant-ID", "m2"); len(w.Header()) != 0 {
		t.Fatalf("headers sent while disabled: %v", w.Header())
	}
}

func TestCostAndConcurrencyDenials(t *testing.T) {
	clock := limiter.NewManualClock(epoch)
	e := newEngine(t, memStore(t, clock),
		policy.Rule{Name: "payer", Dimensions: []policy.Dimension{policy.Payer}, Limit: limiter.PerHour(10)},
		policy.Rule{Name: "inflight", Dimensions: []policy.Dimension{policy.Merchant}, MaxInFlight: 1},
	)
	extract := func(r *http.Request) (policy.Request, error) {
		req, _ := DefaultExtract(r)
		if r.URL.Query().Get("n") != "" {
			req.Cost = int64(len(r.URL.Query().Get("n")))
		}
		return req, nil
	}
	h := Middleware(e, Options{Extract: extract, ProblemType: "https://example.com/problems/rate-limited"})(ok)

	w := do(h, "POST", "/payouts?n=xxxxxxxxxxx", "X-Payer-Document", "12345678909")
	if w.Code != 429 || w.Header().Get("Retry-After") != "" {
		t.Fatalf("oversized: %d %v", w.Code, w.Header())
	}
	p := problem(t, w)
	if p["max_cost"].(float64) != 10 || p["cost"].(float64) != 11 || p["type"] != "https://example.com/problems/rate-limited" {
		t.Fatalf("problem %v", p)
	}
	if w.Header().Get("RateLimit-Policy") != `"payer";q=10;w=3600` || w.Header().Get("RateLimit") != "" {
		t.Fatalf("headers %v", w.Header())
	}

	release := make(chan struct{})
	started := make(chan struct{})
	slow := Middleware(e, Options{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
	}))
	done := make(chan struct{})
	go func() { do(slow, "POST", "/payouts", "X-Merchant-ID", "m"); close(done) }()
	<-started
	w = do(slow, "POST", "/payouts", "X-Merchant-ID", "m")
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("concurrent: %d %v", w.Code, w.Header())
	}
	if p := problem(t, w); p["policy"] != "inflight" {
		t.Fatalf("%v", p)
	}
	close(release)
	<-done
	if w := do(h, "POST", "/payouts", "X-Merchant-ID", "m"); w.Code != http.StatusNoContent {
		t.Fatalf("slot not released after the handler returned: %d", w.Code)
	}
}

func TestShadowMode(t *testing.T) {
	clock := limiter.NewManualClock(epoch)
	e := newEngine(t, memStore(t, clock),
		policy.Rule{Name: "merchant", Dimensions: []policy.Dimension{policy.Merchant}, Limit: limiter.PerMinute(1)})
	var logs bytes.Buffer
	reg := metrics.New()
	h := Middleware(e, Options{Shadow: true, Metrics: reg, Logger: slog.New(slog.NewTextHandler(&logs, nil))})(ok)
	for i := 0; i < 3; i++ {
		w := do(h, "POST", "/pix", "X-Merchant-ID", "m", "X-Payer-Document", "123.456.789-09")
		if w.Code != http.StatusNoContent || w.Header().Get("RateLimit") != "" {
			t.Fatalf("shadow mode enforced or leaked headers: %d %v", w.Code, w.Header())
		}
	}
	if n := strings.Count(logs.String(), "would deny"); n != 2 {
		t.Fatalf("logged %d shadow denials:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), "123.456") {
		t.Fatal("payer document logged in clear")
	}
	var out bytes.Buffer
	_, _ = reg.WriteTo(&out)
	if !strings.Contains(out.String(), `decision="shadow_denied"} 2`) {
		t.Fatal(out.String())
	}
}

type errLimiter struct{ err error }

func (e errLimiter) Allow(context.Context, policy.Request) (*policy.Decision, error) {
	return nil, e.err
}

func TestErrorModes(t *testing.T) {
	down := errLimiter{errors.New("redis: connection refused")}
	quiet := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	reg := metrics.New()
	if w := do(Middleware(down, Options{Logger: quiet, Metrics: reg})(ok), "GET", "/"); w.Code != http.StatusNoContent {
		t.Fatalf("fail open: %d", w.Code)
	}
	w := do(Middleware(down, Options{OnError: FailClosed, Logger: quiet})(ok), "GET", "/")
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("fail closed: %d", w.Code)
	}
	problem(t, w)
	misconfigured := errLimiter{limiter.ErrCostExceedsBurst}
	if w := do(Middleware(misconfigured, Options{Logger: quiet})(ok), "GET", "/"); w.Code != http.StatusInternalServerError {
		t.Fatalf("request error must never fail open: %d", w.Code)
	}
	var out bytes.Buffer
	_, _ = reg.WriteTo(&out)
	if !strings.Contains(out.String(), `ratelimit_errors_total{kind="limiter"} 1`) {
		t.Fatal(out.String())
	}
}

func TestExtractErrors(t *testing.T) {
	e := newEngine(t, memStore(t, nil), policy.Rule{Name: "ip", Dimensions: []policy.Dimension{policy.IP}, Limit: limiter.PerMinute(1)})
	skip := Middleware(e, Options{Extract: func(*http.Request) (policy.Request, error) { return policy.Request{}, ErrSkip }})(ok)
	for i := 0; i < 3; i++ {
		if w := do(skip, "GET", "/healthz"); w.Code != http.StatusNoContent || w.Header().Get("RateLimit") != "" {
			t.Fatal("ErrSkip must bypass limiting")
		}
	}
	bad := Middleware(e, Options{Extract: func(*http.Request) (policy.Request, error) {
		return policy.Request{}, errors.New("missing merchant")
	}})(ok)
	w := do(bad, "GET", "/")
	if w.Code != http.StatusBadRequest || problem(t, w)["detail"] != "missing merchant" {
		t.Fatalf("extract error: %d", w.Code)
	}
}

// TestGracefulDegradation runs the middleware on a Redis store behind a
// FallbackStore and takes Redis down mid-traffic: limits keep being
// enforced locally, and recover to Redis when it comes back.
func TestGracefulDegradation(t *testing.T) {
	clock := limiter.NewManualClock(epoch)
	srv, err := redistest.Start(redistest.Options{Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	c := resp.NewClient(resp.Options{Addr: srv.Addr(), DialTimeout: 100 * time.Millisecond})
	defer c.Close()
	reg := metrics.New()
	breaker := limiter.NewBreaker(limiter.BreakerOptions{FailureThreshold: 2, OpenTimeout: time.Second, Clock: clock})
	store := limiter.NewFallbackStore(redisstore.New(c, redisstore.Options{
		Observe: reg.StoreLatency,
	}), limiter.FallbackOptions{
		Fallback:   memStore(t, clock),
		Scale:      0.5, // two replicas
		Breaker:    breaker,
		OnFallback: func(op, reason string, _ error) { reg.Fallback(op, reason) },
	})
	reg.Gauge("ratelimit_breaker_open", func() float64 {
		if breaker.State() == limiter.BreakerOpen {
			return 1
		}
		return 0
	})
	e := newEngine(t, store, policy.Rule{Name: "merchant", Dimensions: []policy.Dimension{policy.Merchant}, Limit: limiter.PerMinute(10)})
	h := Middleware(e, Options{OnError: FailClosed, Metrics: reg})(ok)
	hit := func() int { return do(h, "POST", "/pix", "X-Merchant-ID", "m").Code }

	for i := 0; i < 3; i++ {
		if hit() != http.StatusNoContent {
			t.Fatal("redis path")
		}
	}
	srv.SetDown(true)
	// Local fallback at half the global limit: 5 more, then 429, never 503.
	codes := map[int]int{}
	for i := 0; i < 8; i++ {
		codes[hit()]++
	}
	if codes[http.StatusNoContent] != 5 || codes[http.StatusTooManyRequests] != 3 || codes[http.StatusServiceUnavailable] != 0 {
		t.Fatalf("degraded codes %v", codes)
	}
	if breaker.State() != limiter.BreakerOpen {
		t.Fatal("breaker should be open")
	}
	srv.SetDown(false)
	clock.Advance(time.Second)
	if hit() != http.StatusNoContent || breaker.State() != limiter.BreakerClosed {
		t.Fatal("did not recover to redis")
	}
	var out bytes.Buffer
	_, _ = reg.WriteTo(&out)
	for _, want := range []string{
		`ratelimit_fallback_total{op="take",reason="error"} 2`,
		`ratelimit_fallback_total{op="take",reason="open"} 6`,
		`ratelimit_store_errors_total{op="take"} 2`,
		`ratelimit_store_duration_seconds_count{op="take"} 6`,
		"ratelimit_breaker_open 0",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in\n%s", want, out.String())
		}
	}
}

func TestClientIP(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	tests := []struct {
		remote, xff string
		trusted     []netip.Prefix
		want        string
	}{
		{"203.0.113.7:5555", "", nil, "203.0.113.7"},
		{"203.0.113.7:5555", "1.2.3.4", nil, "203.0.113.7"},                // untrusted peer: header ignored
		{"10.0.0.2:80", "198.51.100.9, 10.0.0.5", trusted, "198.51.100.9"}, // walk past trusted hops
		{"10.0.0.2:80", "6.6.6.6, 198.51.100.9", trusted, "198.51.100.9"},  // client-supplied prefix ignored
		{"10.0.0.2:80", "garbage", trusted, "10.0.0.2"},
		{"10.0.0.2:80", "", trusted, "10.0.0.2"},
		{"[::ffff:203.0.113.7]:1", "", nil, "203.0.113.7"},
		{"not-an-addr", "", nil, "not-an-addr"},
	}
	for _, tt := range tests {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = tt.remote
		if tt.xff != "" {
			r.Header.Set("X-Forwarded-For", tt.xff)
		}
		if got := ClientIP(r, tt.trusted); got != tt.want {
			t.Errorf("ClientIP(%q, %q) = %q, want %q", tt.remote, tt.xff, got, tt.want)
		}
	}
}

func TestDefaultExtract(t *testing.T) {
	r := httptest.NewRequest("POST", "/pix/transfers?x=1", nil)
	r.Header.Set("Authorization", "Bearer sk_test_123")
	r.Header.Set("X-Merchant-ID", "m1")
	r.Header.Set("X-Payer-Document", "12345678909")
	r.Header.Set("X-Plan", "growth")
	req, err := DefaultExtract(r)
	if err != nil || req.APIKey != "sk_test_123" || req.Merchant != "m1" || req.Payer != "12345678909" ||
		req.Plan != "growth" || req.Endpoint != "POST /pix/transfers" || req.IP != "192.0.2.1" || req.Cost != 1 {
		t.Fatalf("%+v %v", req, err)
	}
	r.Header.Set("Authorization", "Basic abc")
	r.Header.Set("X-API-Key", "k2")
	if req, _ := DefaultExtract(r); req.APIKey != "k2" {
		t.Fatalf("X-API-Key fallback: %q", req.APIKey)
	}
}

func TestCeilSeconds(t *testing.T) {
	for d, want := range map[time.Duration]int64{0: 1, time.Millisecond: 1, time.Second: 1, 1001 * time.Millisecond: 2, time.Minute: 60} {
		if got := ceilSeconds(d); got != want {
			t.Errorf("ceilSeconds(%s) = %d, want %d", d, got, want)
		}
	}
}
