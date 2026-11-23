// Command payments-api is a small Pix API showing ratelimit-go in a payment
// context: per-API-key, per-merchant and per-payer limits, cost-weighted
// bulk payouts, a concurrency cap on payouts, tiered plans, Prometheus
// metrics, and Redis with a local fallback.
//
//	go run ./examples/payments-api                 # in-memory store
//	REDIS_URL=redis://localhost:6379 REPLICAS=3 go run ./examples/payments-api
//
//	curl -i -X POST localhost:8080/pix/transfers -H 'X-API-Key: demo-starter' \
//	     -d '{"payer_document":"123.456.789-09","amount_cents":1500}'
//	curl -i -X POST localhost:8080/payouts/bulk -H 'X-API-Key: demo-growth' \
//	     -d '{"payer_document":"12.345.678/0001-95","items":[{"amount_cents":100},{"amount_cents":200}]}'
//	curl localhost:8080/metrics
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/victorcondackdev/ratelimit-go/httplimit"
	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/metrics"
	"github.com/victorcondackdev/ratelimit-go/policy"
	"github.com/victorcondackdev/ratelimit-go/redisstore"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

// account is what an API key resolves to. A real service would look this
// up after authenticating the key.
type account struct {
	Merchant string
	Plan     string
}

var accounts = map[string]account{
	"demo-starter":    {Merchant: "merchant-001", Plan: "starter"},
	"demo-growth":     {Merchant: "merchant-002", Plan: "growth"},
	"demo-enterprise": {Merchant: "merchant-003", Plan: "enterprise"},
}

const (
	pixEndpoint    = "POST /pix/transfers"
	payoutEndpoint = "POST /payouts/bulk"
	maxBody        = 1 << 20
)

// Rules for a Pix API. Costs: a transfer is 1, a bulk payout of N items
// is N.
func rules() []policy.Rule {
	payments := []string{pixEndpoint, payoutEndpoint}
	return []policy.Rule{
		// Credential abuse: requests per key, whatever they cost.
		{Name: "api-key", Dimensions: []policy.Dimension{policy.APIKey}, Limit: limiter.PerMinute(600), Unweighted: true},
		// Merchant throughput: transfers per minute, bursts of 30.
		{Name: "merchant-pix", Dimensions: []policy.Dimension{policy.Merchant}, Limit: limiter.PerMinute(120).WithBurst(30), Endpoints: payments},
		// Fraud containment: a single payer document moving money in a
		// burst is the classic account-takeover signature.
		{Name: "payer-pix", Dimensions: []policy.Dimension{policy.Payer}, Limit: limiter.PerHour(20).WithBurst(5), Endpoints: payments},
		// Scraping and retries storms, per IP and endpoint.
		{Name: "ip-endpoint", Dimensions: []policy.Dimension{policy.IP, policy.Endpoint}, Limit: limiter.PerMinute(60), Unweighted: true},
		// Bulk payouts are slow; cap how many run at once per merchant.
		{Name: "payout-inflight", Dimensions: []policy.Dimension{policy.Merchant}, MaxInFlight: 2, Endpoints: []string{payoutEndpoint}},
	}
}

func plans() []policy.Plan {
	return []policy.Plan{
		{Name: "starter"},
		{Name: "growth", Scale: 3},
		{Name: "enterprise", Scale: 10, Limits: map[string]limiter.Limit{
			// Payer limits are a risk control, not a commercial one: they
			// grow far less than throughput.
			"payer-pix": limiter.PerHour(40).WithBurst(10),
		}, MaxInFlight: map[string]int{"payout-inflight": 8}},
	}
}

type paymentBody struct {
	PayerDocument string `json:"payer_document"`
	AmountCents   int64  `json:"amount_cents"`
	Items         []struct {
		AmountCents int64 `json:"amount_cents"`
	} `json:"items"`
}

// extract resolves the API key and reads the payer and cost from the body,
// which it restores for the handler.
func extract(r *http.Request) (policy.Request, error) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
		return policy.Request{}, httplimit.ErrSkip
	}
	key := httplimit.BearerToken(r)
	if key == "" {
		key = r.Header.Get("X-API-Key")
	}
	acct, ok := accounts[key]
	if !ok {
		return policy.Request{}, errors.New("unknown API key")
	}
	req := policy.Request{
		APIKey:   key,
		Merchant: acct.Merchant,
		Plan:     acct.Plan,
		Endpoint: r.Method + " " + r.URL.Path,
		IP:       httplimit.ClientIP(r, nil),
		Cost:     1,
	}
	if r.Method != http.MethodPost {
		return req, nil
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		return req, fmt.Errorf("read body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var body paymentBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return req, errors.New("body must be JSON")
	}
	req.Payer = body.PayerDocument
	if req.Endpoint == payoutEndpoint {
		req.Cost = int64(len(body.Items))
		if req.Cost == 0 {
			return req, errors.New("a bulk payout needs at least one item")
		}
	}
	return req, nil
}

// config is everything newServer needs from the environment.
type config struct {
	RedisURL string
	Replicas int
	Secret   []byte
	Logger   *slog.Logger
	Clock    limiter.Clock
}

type server struct {
	handler http.Handler
	close   func()
}

func newServer(cfg config) (*server, error) {
	reg := metrics.New()
	local := limiter.NewMemoryStore(limiter.MemoryOptions{Clock: cfg.Clock})
	reg.Gauge("ratelimit_memory_keys", func() float64 { return float64(local.Stats().Keys) })
	closers := []func(){func() { local.Close() }}

	var store limiter.Store = local
	if cfg.RedisURL != "" {
		opts, err := resp.ParseURL(cfg.RedisURL)
		if err != nil {
			return nil, err
		}
		opts.ReadTimeout, opts.WriteTimeout = 100*time.Millisecond, 100*time.Millisecond
		client := resp.NewClient(opts)
		closers = append(closers, func() { client.Close() })
		breaker := limiter.NewBreaker(limiter.BreakerOptions{
			OnStateChange: func(from, to limiter.BreakerState) {
				cfg.Logger.Warn("redis circuit breaker", "from", from.String(), "to", to.String())
			},
		})
		reg.Gauge("ratelimit_breaker_open", func() float64 {
			if breaker.State() == limiter.BreakerClosed {
				return 0
			}
			return 1
		})
		store = limiter.NewFallbackStore(
			redisstore.New(client, redisstore.Options{Prefix: "payments:rl:", Observe: reg.StoreLatency}),
			limiter.FallbackOptions{
				Fallback:   local,
				Scale:      1 / float64(max(1, cfg.Replicas)),
				Breaker:    breaker,
				OnFallback: func(op, reason string, _ error) { reg.Fallback(op, reason) },
			})
	}

	engine, err := policy.New(policy.Config{
		Rules:       rules(),
		Plans:       plans(),
		DefaultPlan: "starter",
		Store:       store,
		KeySecret:   cfg.Secret,
	})
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /pix/transfers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusCreated, map[string]any{"status": "ACCEPTED"})
	})
	mux.HandleFunc("POST /payouts/bulk", func(w http.ResponseWriter, r *http.Request) {
		var body paymentBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "PROCESSING", "items": len(body.Items)})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("GET /metrics", reg)

	limit := httplimit.Middleware(engine, httplimit.Options{
		Extract: extract,
		// Payer limits are a fraud control: if the limiter cannot decide,
		// do not move money.
		OnError: httplimit.FailClosed,
		Logger:  cfg.Logger,
		Metrics: reg,
		Shadow:  os.Getenv("RATELIMIT_SHADOW") == "1",
	})
	return &server{handler: limit(mux), close: func() {
		for _, c := range closers {
			c()
		}
	}}, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	replicas, _ := strconv.Atoi(os.Getenv("REPLICAS"))
	srv, err := newServer(config{
		RedisURL: os.Getenv("REDIS_URL"),
		Replicas: replicas,
		Secret:   []byte(os.Getenv("RATELIMIT_KEY_SECRET")),
		Logger:   logger,
	})
	if err != nil {
		logger.Error("startup", "error", err)
		os.Exit(1)
	}
	defer srv.close()

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	hs := &http.Server{Addr: addr, Handler: srv.handler, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = hs.Shutdown(shutdown)
	}()
	logger.Info("listening", "addr", addr, "redis", os.Getenv("REDIS_URL") != "")
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("serve", "error", err)
		os.Exit(1)
	}
}
