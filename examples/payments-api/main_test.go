package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/internal/redistest"
	"github.com/victorcondackdev/ratelimit-go/limiter"
)

func testServer(t *testing.T, redisURL string, clock limiter.Clock) http.Handler {
	t.Helper()
	srv, err := newServer(config{
		RedisURL: redisURL,
		Replicas: 2,
		Secret:   []byte("test"),
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:    clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.close)
	return srv.handler
}

func post(h http.Handler, path, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("X-API-Key", key)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func pix(payer string) string {
	return `{"payer_document":"` + payer + `","amount_cents":1500}`
}

func bulk(payer string, n int) string {
	items := strings.TrimSuffix(strings.Repeat(`{"amount_cents":100},`, n), ",")
	return `{"payer_document":"` + payer + `","items":[` + items + `]}`
}

func TestPayerLimitContainsBursts(t *testing.T) {
	clock := limiter.NewManualClock(time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC))
	h := testServer(t, "", clock)
	for i := 0; i < 5; i++ {
		if w := post(h, "/pix/transfers", "demo-starter", pix("123.456.789-09")); w.Code != http.StatusCreated {
			t.Fatalf("transfer %d: %d %s", i, w.Code, w.Body)
		}
	}
	w := post(h, "/pix/transfers", "demo-starter", pix("12345678909")) // same payer, unformatted
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("6th transfer for one payer: %d", w.Code)
	}
	var p map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &p)
	if p["policy"] != "payer-pix" || w.Header().Get("Retry-After") != "180" {
		t.Fatalf("%v %v", p, w.Header())
	}
	// Another payer of the same merchant is unaffected.
	if w := post(h, "/pix/transfers", "demo-starter", pix("98765432100")); w.Code != http.StatusCreated {
		t.Fatalf("other payer: %d", w.Code)
	}
	// One payer slot comes back every 3 minutes.
	clock.Advance(3 * time.Minute)
	if w := post(h, "/pix/transfers", "demo-starter", pix("12345678909")); w.Code != http.StatusCreated {
		t.Fatalf("after replenish: %d", w.Code)
	}
}

func TestBulkPayoutCosts(t *testing.T) {
	clock := limiter.NewManualClock(time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC))
	h := testServer(t, "", clock)
	w := post(h, "/payouts/bulk", "demo-starter", bulk("12.345.678/0001-95", 4))
	if w.Code != http.StatusAccepted || !strings.Contains(w.Header().Get("RateLimit"), `"payer-pix";r=1`) {
		t.Fatalf("%d %v", w.Code, w.Header())
	}
	// 2 more items exceed the payer's remaining burst of 1.
	if w := post(h, "/payouts/bulk", "demo-starter", bulk("12.345.678/0001-95", 2)); w.Code != http.StatusTooManyRequests {
		t.Fatalf("over budget: %d", w.Code)
	}
	// 6 items can never fit a burst of 5: denied with no Retry-After.
	w = post(h, "/payouts/bulk", "demo-starter", bulk("11.222.333/0001-81", 6))
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "" || !strings.Contains(w.Body.String(), "split the request") {
		t.Fatalf("oversized: %d %v %s", w.Code, w.Header(), w.Body)
	}
	// The enterprise plan raises the payer burst to 10.
	if w := post(h, "/payouts/bulk", "demo-enterprise", bulk("11.222.333/0001-81", 6)); w.Code != http.StatusAccepted {
		t.Fatalf("enterprise: %d %s", w.Code, w.Body)
	}
	// Merchant budget is shared across payers: starter has a burst of 30.
	for i := 0; i < 6; i++ {
		post(h, "/payouts/bulk", "demo-starter", bulk("payer-"+string(rune('a'+i)), 5))
	}
	if w := post(h, "/payouts/bulk", "demo-starter", bulk("payer-z", 2)); w.Code != http.StatusTooManyRequests || !strings.Contains(w.Body.String(), "merchant-pix") {
		t.Fatalf("merchant budget: %d %s", w.Code, w.Body)
	}
}

func TestBadRequestsAndMetrics(t *testing.T) {
	h := testServer(t, "", limiter.SystemClock{})
	if w := post(h, "/pix/transfers", "nope", pix("1")); w.Code != http.StatusBadRequest {
		t.Fatalf("unknown key: %d", w.Code)
	}
	if w := post(h, "/pix/transfers", "demo-growth", "{"); w.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", w.Code)
	}
	if w := post(h, "/payouts/bulk", "demo-growth", `{"items":[]}`); w.Code != http.StatusBadRequest {
		t.Fatalf("empty bulk: %d", w.Code)
	}
	r := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("healthz: %d", w.Code)
	}
	post(h, "/pix/transfers", "demo-growth", pix("12345678909"))
	r = httptest.NewRequest("GET", "/metrics", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `ratelimit_decisions_total{policy="payer-pix",decision="allowed"} 1`) {
		t.Fatalf("metrics:\n%s", w.Body)
	}
}

func TestRedisWithFallback(t *testing.T) {
	clock := limiter.NewManualClock(time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC))
	srv, err := redistest.Start(redistest.Options{Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	h := testServer(t, srv.URL(), clock)
	for i := 0; i < 3; i++ {
		if w := post(h, "/pix/transfers", "demo-growth", pix("12345678909")); w.Code != http.StatusCreated {
			t.Fatalf("redis path: %d %s", w.Code, w.Body)
		}
	}
	if srv.Keys() == 0 {
		t.Fatal("nothing stored in redis")
	}
	srv.SetDown(true)
	// Degraded: the local fallback takes over (scaled for 2 replicas), so
	// requests are still limited rather than failing closed.
	codes := map[int]int{}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := post(h, "/pix/transfers", "demo-growth", pix("12345678909"))
			mu.Lock()
			codes[w.Code]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if codes[http.StatusServiceUnavailable] != 0 || codes[http.StatusTooManyRequests] == 0 {
		t.Fatalf("degraded codes %v", codes)
	}
	r := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if !bytes.Contains(w.Body.Bytes(), []byte("ratelimit_fallback_total")) || !bytes.Contains(w.Body.Bytes(), []byte("ratelimit_breaker_open 1")) {
		t.Fatalf("metrics:\n%s", w.Body)
	}
	if _, err := newServer(config{RedisURL: "http://bad"}); err == nil {
		t.Fatal("bad redis url accepted")
	}
}
