// Package httplimit is net/http middleware for [policy.Engine].
//
// For every request it extracts a [policy.Request] (API key, merchant,
// payer, endpoint, IP, plan, cost), asks the engine for a decision and
// either calls the next handler or answers 429 with an RFC 9457
// application/problem+json body.
//
// Responses carry rate-limit headers in two dialects:
//
//   - RateLimit-Policy and RateLimit, from the IETF HTTPAPI working group's
//     draft "RateLimit header fields for HTTP"
//     (draft-ietf-httpapi-ratelimit-headers). It is still an Internet-Draft
//     and may change; the structured-field form used here follows its
//     recent revisions, one list member per policy.
//   - The de facto X-RateLimit-Limit, X-RateLimit-Remaining and
//     X-RateLimit-Reset (seconds until reset), for the tightest policy.
//
// plus Retry-After on denials when waiting will help.
//
// Shadow mode evaluates and records everything but never denies, which is
// how new limits should be rolled out. When the limiter itself fails (for
// example Redis is unreachable and no fallback store is configured), the
// ErrorMode decides between admitting the request (FailOpen) and answering
// 503 (FailClosed). For graceful degradation, give the engine a
// [limiter.FallbackStore], so that failures are absorbed by a local limiter
// behind a circuit breaker before they ever reach this switch.
package httplimit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/metrics"
	"github.com/victorcondackdev/ratelimit-go/policy"
)

// Limiter decides requests; *policy.Engine implements it.
type Limiter interface {
	Allow(ctx context.Context, req policy.Request) (*policy.Decision, error)
}

// ErrorMode selects the behavior when the limiter returns an error.
type ErrorMode int

const (
	// FailOpen admits the request and logs the error. Availability first:
	// right for most read traffic and for limits that protect fairness.
	FailOpen ErrorMode = iota
	// FailClosed answers 503. Right where the limit is a safety control,
	// for example per-payer limits that contain fraud.
	FailClosed
)

// ErrSkip, returned by an Extract function, exempts the request from rate
// limiting (health checks, internal callers).
var ErrSkip = errors.New("httplimit: skip")

// Options configures the middleware.
type Options struct {
	// Extract builds the policy request. Defaults to DefaultExtract. Return
	// ErrSkip to bypass limiting; any other error answers 400.
	Extract func(*http.Request) (policy.Request, error)
	// Shadow evaluates limits and records would-be denials without
	// enforcing them, and sends no rate-limit headers.
	Shadow bool
	// OnError chooses what happens when the limiter fails. Invalid-request
	// errors (a misconfigured limit) always answer 500.
	OnError ErrorMode
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Metrics, if set, records decisions and errors.
	Metrics *metrics.Registry
	// ProblemType is the "type" member of problem responses. Defaults to
	// "about:blank", per RFC 9457.
	ProblemType string
	// DisableDraftHeaders and DisableLegacyHeaders turn off a header dialect.
	DisableDraftHeaders  bool
	DisableLegacyHeaders bool
}

// Middleware returns middleware enforcing l.
func Middleware(l Limiter, opts Options) func(http.Handler) http.Handler {
	if opts.Extract == nil {
		opts.Extract = DefaultExtract
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.ProblemType == "" {
		opts.ProblemType = "about:blank"
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			preq, err := opts.Extract(r)
			if errors.Is(err, ErrSkip) {
				next.ServeHTTP(w, r)
				return
			}
			if err != nil {
				opts.Metrics.Error("extract")
				writeProblem(w, opts.ProblemType, http.StatusBadRequest, "Bad Request", err.Error(), nil)
				return
			}
			d, err := l.Allow(r.Context(), preq)
			if err != nil {
				handleError(w, r, next, opts, preq, err)
				return
			}
			defer d.Release()
			record(opts, d)
			if d.Allowed {
				if !opts.Shadow {
					setHeaders(w.Header(), d, opts)
				}
				next.ServeHTTP(w, r)
				return
			}
			denied, _ := d.DeniedBy()
			if opts.Shadow {
				opts.Logger.LogAttrs(r.Context(), slog.LevelWarn, "rate limit would deny (shadow mode)",
					slog.String("policy", denied.Rule), slog.String("key", denied.Key), slog.Any("request", preq))
				next.ServeHTTP(w, r)
				return
			}
			opts.Logger.LogAttrs(r.Context(), slog.LevelInfo, "rate limited",
				slog.String("policy", denied.Rule), slog.String("key", denied.Key), slog.Any("request", preq))
			deny(w, d, denied, opts)
		})
	}
}

func handleError(w http.ResponseWriter, r *http.Request, next http.Handler, opts Options, preq policy.Request, err error) {
	opts.Metrics.Error("limiter")
	if limiter.IsRequestError(err) {
		opts.Logger.LogAttrs(r.Context(), slog.LevelError, "rate limiter misconfigured",
			slog.String("error", err.Error()), slog.Any("request", preq))
		writeProblem(w, opts.ProblemType, http.StatusInternalServerError, "Internal Server Error", "rate limiter misconfigured", nil)
		return
	}
	if opts.OnError == FailClosed {
		opts.Logger.LogAttrs(r.Context(), slog.LevelError, "rate limiter unavailable, failing closed",
			slog.String("error", err.Error()), slog.Any("request", preq))
		w.Header().Set("Retry-After", "1")
		writeProblem(w, opts.ProblemType, http.StatusServiceUnavailable, "Service Unavailable", "rate limiter unavailable", nil)
		return
	}
	opts.Logger.LogAttrs(r.Context(), slog.LevelWarn, "rate limiter unavailable, failing open",
		slog.String("error", err.Error()), slog.Any("request", preq))
	next.ServeHTTP(w, r)
}

func record(opts Options, d *policy.Decision) {
	if opts.Metrics == nil {
		return
	}
	for _, o := range d.Outcomes {
		switch {
		case o.Allowed:
			opts.Metrics.Decision(o.Rule, metrics.Allowed)
		case opts.Shadow:
			opts.Metrics.Decision(o.Rule, metrics.ShadowDenied)
		default:
			opts.Metrics.Decision(o.Rule, metrics.Denied)
		}
	}
}

func deny(w http.ResponseWriter, d *policy.Decision, denied policy.Outcome, opts Options) {
	setHeaders(w.Header(), d, opts)
	ext := map[string]any{"policy": denied.Rule}
	var detail string
	switch {
	case denied.CostExceeded:
		detail = fmt.Sprintf("request cost %d exceeds the maximum of %d for policy %q and can never be admitted; split the request",
			denied.Cost, denied.Limit.EffectiveBurst(), denied.Rule)
		ext["cost"] = denied.Cost
		ext["max_cost"] = denied.Limit.EffectiveBurst()
	case denied.Concurrency:
		detail = fmt.Sprintf("too many concurrent requests for policy %q (limit %d)", denied.Rule, denied.MaxInFlight)
		w.Header().Set("Retry-After", "1")
		ext["retry_after"] = 1
	default:
		wait, _ := d.RetryAfter()
		secs := ceilSeconds(wait)
		w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
		detail = fmt.Sprintf("rate limit %q exceeded; retry in %s", denied.Rule, wait.Round(time.Millisecond))
		ext["retry_after"] = secs
	}
	writeProblem(w, opts.ProblemType, http.StatusTooManyRequests, "Too Many Requests", detail, ext)
}

func writeProblem(w http.ResponseWriter, typ string, status int, title, detail string, ext map[string]any) {
	body := map[string]any{"type": typ, "title": title, "status": status, "detail": detail}
	for k, v := range ext {
		body[k] = v
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// ceilSeconds rounds up to whole seconds, at least 1, so a client never
// retries before the limit allows it.
func ceilSeconds(d time.Duration) int64 {
	s := int64(math.Ceil(d.Seconds()))
	if s < 1 {
		return 1
	}
	return s
}

// window is the time a fully drained limit takes to refill: Burst times the
// emission interval.
func window(l limiter.Limit) int64 {
	return ceilSeconds(time.Duration(l.EffectiveBurst()) * l.Interval())
}

func setHeaders(h http.Header, d *policy.Decision, opts Options) {
	var policies, states []string
	for _, o := range d.Outcomes {
		if o.Concurrency {
			continue
		}
		name := strconv.Quote(o.Rule)
		policies = append(policies, fmt.Sprintf("%s;q=%d;w=%d", name, o.Limit.EffectiveBurst(), window(o.Limit)))
		if !o.CostExceeded {
			states = append(states, fmt.Sprintf("%s;r=%d;t=%d", name, o.Result.Remaining, int64(math.Ceil(o.Result.ResetAfter.Seconds()))))
		}
	}
	if len(policies) == 0 {
		return
	}
	if !opts.DisableDraftHeaders {
		h.Set("RateLimit-Policy", strings.Join(policies, ", "))
		if len(states) > 0 {
			h.Set("RateLimit", strings.Join(states, ", "))
		}
	}
	if t, ok := d.Tightest(); ok && !opts.DisableLegacyHeaders && !t.CostExceeded {
		h.Set("X-RateLimit-Limit", strconv.FormatInt(t.Limit.EffectiveBurst(), 10))
		h.Set("X-RateLimit-Remaining", strconv.FormatInt(t.Result.Remaining, 10))
		h.Set("X-RateLimit-Reset", strconv.FormatInt(int64(math.Ceil(t.Result.ResetAfter.Seconds())), 10))
	}
}

// DefaultExtract builds a policy request from common headers:
//
//   - APIKey: the bearer token of Authorization, else X-API-Key.
//   - Merchant: X-Merchant-ID.
//   - Payer: X-Payer-Document.
//   - Plan: X-Plan. In production, derive the plan from the authenticated
//     API key instead of trusting a client header.
//   - Endpoint: "METHOD /path".
//   - IP: the connection's remote address (see ClientIP to honor trusted
//     proxies).
//   - Cost: 1.
func DefaultExtract(r *http.Request) (policy.Request, error) {
	key := BearerToken(r)
	if key == "" {
		key = r.Header.Get("X-API-Key")
	}
	return policy.Request{
		APIKey:   key,
		Merchant: r.Header.Get("X-Merchant-ID"),
		Payer:    r.Header.Get("X-Payer-Document"),
		Plan:     r.Header.Get("X-Plan"),
		Endpoint: r.Method + " " + r.URL.Path,
		IP:       ClientIP(r, nil),
		Cost:     1,
	}, nil
}

// BearerToken returns the token of an "Authorization: Bearer" header.
func BearerToken(r *http.Request) string {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

// ClientIP returns the client address. When the direct peer is in trusted
// (a load balancer or reverse proxy), X-Forwarded-For is walked from the
// right and the first address not in trusted is returned; otherwise the
// header is ignored, since any client can send it.
func ClientIP(r *http.Request, trusted []netip.Prefix) string {
	peer := parseAddr(r.RemoteAddr)
	if !peer.IsValid() {
		return r.RemoteAddr
	}
	if !inAny(peer, trusted) {
		return peer.String()
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		a := parseAddr(strings.TrimSpace(hops[i]))
		if !a.IsValid() {
			break
		}
		if !inAny(a, trusted) {
			return a.String()
		}
	}
	return peer.String()
}

func parseAddr(s string) netip.Addr {
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}
	}
	return a.Unmap()
}

func inAny(a netip.Addr, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}
