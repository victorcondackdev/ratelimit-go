// Package policy turns payment-API traffic into rate-limit decisions.
//
// A policy is a set of rules, each keyed on one or more dimensions of the
// request (API key, merchant, payer document, endpoint, client IP), with a
// rate limit or a maximum number of in-flight requests. Every matching rule
// must pass for a request to be admitted, and when one denies, nothing is
// consumed from the others:
//
//   - Concurrency rules are checked first, in process, all-or-nothing.
//   - Rate rules on the same store go in one [limiter.Store.Take] batch,
//     which is atomic on the memory store and on a single Redis node.
//   - Rate rules on different stores (for example a local per-IP limit next
//     to a Redis per-merchant one) are combined with [limiter.TakeAll]'s
//     check-then-commit protocol; see its documentation for what that does
//     and does not guarantee.
//
// Requests carry a cost. Weighted rules consume it (a bulk payout of 50
// transfers costs 50), unweighted rules count requests. Plans scale or
// override limits per customer tier.
package policy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/victorcondackdev/ratelimit-go/limiter"
)

// Rule is one limit.
type Rule struct {
	// Name identifies the rule in keys, headers, logs and metrics. It must
	// be unique and should be a short token such as "merchant-pix".
	Name string
	// Dimensions are the request attributes the rule is keyed on, in order.
	// A request missing any of them is not subject to the rule.
	Dimensions []Dimension
	// Limit is the rate limit. Set either Limit or MaxInFlight.
	Limit limiter.Limit
	// MaxInFlight makes this a concurrency rule: at most this many
	// admitted requests per key may be in flight at once.
	MaxInFlight int
	// Endpoints restricts the rule to these Request.Endpoint values. Empty
	// means every endpoint.
	Endpoints []string
	// Unweighted makes the rule count requests (cost 1) regardless of the
	// request's cost, for limits such as requests per IP.
	Unweighted bool
	// Store overrides Config.Store for this rule.
	Store limiter.Store
}

func (r Rule) concurrency() bool { return r.MaxInFlight > 0 }

// Plan is a customer tier.
type Plan struct {
	Name string
	// Scale multiplies the Rate and Burst of every rate rule without an
	// override (see [limiter.Limit.Scale]) and the MaxInFlight of every
	// concurrency rule without one. Zero means 1.
	Scale float64
	// Limits overrides rate rules by name.
	Limits map[string]limiter.Limit
	// MaxInFlight overrides concurrency rules by name.
	MaxInFlight map[string]int
}

// Config configures an Engine.
type Config struct {
	Rules []Rule
	Plans []Plan
	// DefaultPlan is used for requests with an empty or unknown Plan. Empty
	// means no plan: rules apply as written.
	DefaultPlan string
	// Store is used by rate rules without their own.
	Store limiter.Store
	// Concurrency holds in-flight counts. Defaults to a new one; share one
	// between engines to share counts.
	Concurrency *Concurrency
	// KeyPrefix is prepended to every store key.
	KeyPrefix string
	// KeySecret keys the HMAC that pseudonymizes API keys and payer
	// documents in store keys. Set it (and keep it stable across
	// instances); without it the pseudonyms are plain hashes.
	KeySecret []byte
}

// ErrConfig is wrapped by configuration errors from New.
var ErrConfig = errors.New("policy: invalid config")

// Engine evaluates requests against a policy. It is safe for concurrent use.
type Engine struct {
	cfg   Config
	plans map[string]*Plan
	conc  *Concurrency
}

// New validates cfg and returns an Engine.
func New(cfg Config) (*Engine, error) {
	e := &Engine{cfg: cfg, plans: map[string]*Plan{}, conc: cfg.Concurrency}
	if e.conc == nil {
		e.conc = NewConcurrency()
	}
	names := map[string]Rule{}
	for _, r := range cfg.Rules {
		if r.Name == "" || strings.ContainsAny(r.Name, ":{}\" ,;=") {
			return nil, fmt.Errorf("%w: rule name %q must be a non-empty token", ErrConfig, r.Name)
		}
		if _, dup := names[r.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate rule %q", ErrConfig, r.Name)
		}
		names[r.Name] = r
		if len(r.Dimensions) == 0 {
			return nil, fmt.Errorf("%w: rule %q has no dimensions", ErrConfig, r.Name)
		}
		for _, d := range r.Dimensions {
			if !d.valid() {
				return nil, fmt.Errorf("%w: rule %q: unknown dimension %q", ErrConfig, r.Name, d)
			}
		}
		if r.concurrency() {
			if r.Limit != (limiter.Limit{}) {
				return nil, fmt.Errorf("%w: rule %q sets both Limit and MaxInFlight", ErrConfig, r.Name)
			}
			continue
		}
		if err := r.Limit.Validate(); err != nil {
			return nil, fmt.Errorf("%w: rule %q: %v", ErrConfig, r.Name, err)
		}
		if r.Store == nil && cfg.Store == nil {
			return nil, fmt.Errorf("%w: rule %q has no store", ErrConfig, r.Name)
		}
	}
	for i := range cfg.Plans {
		p := &cfg.Plans[i]
		if p.Name == "" {
			return nil, fmt.Errorf("%w: plan without a name", ErrConfig)
		}
		if p.Scale < 0 {
			return nil, fmt.Errorf("%w: plan %q has a negative scale", ErrConfig, p.Name)
		}
		for name, l := range p.Limits {
			r, ok := names[name]
			if !ok || r.concurrency() {
				return nil, fmt.Errorf("%w: plan %q overrides unknown rate rule %q", ErrConfig, p.Name, name)
			}
			if err := l.Validate(); err != nil {
				return nil, fmt.Errorf("%w: plan %q rule %q: %v", ErrConfig, p.Name, name, err)
			}
		}
		for name, n := range p.MaxInFlight {
			r, ok := names[name]
			if !ok || !r.concurrency() || n <= 0 {
				return nil, fmt.Errorf("%w: plan %q: bad in-flight override for %q", ErrConfig, p.Name, name)
			}
		}
		e.plans[p.Name] = p
	}
	if cfg.DefaultPlan != "" && e.plans[cfg.DefaultPlan] == nil {
		return nil, fmt.Errorf("%w: default plan %q is not defined", ErrConfig, cfg.DefaultPlan)
	}
	return e, nil
}

// Rules returns the configured rules.
func (e *Engine) Rules() []Rule { return e.cfg.Rules }

// Concurrency returns the engine's in-flight counters.
func (e *Engine) Concurrency() *Concurrency { return e.conc }

// Outcome is one rule's verdict.
type Outcome struct {
	Rule string
	// Key is the store key, with sensitive dimensions pseudonymized. It is
	// safe to log.
	Key string
	// Allowed is this rule's own verdict.
	Allowed bool
	// Cost is what the rule charged (or would have).
	Cost int64

	// Rate rules.
	Limit  limiter.Limit
	Result limiter.Result
	// CostExceeded reports a cost larger than the rule's burst: the request
	// can never pass, so waiting will not help.
	CostExceeded bool

	// Concurrency rules.
	Concurrency bool
	MaxInFlight int
}

// Decision is the verdict for a request.
type Decision struct {
	Allowed  bool
	Outcomes []Outcome
	release  func()
	once     sync.Once
}

// Release frees the concurrency slots held by an admitted request. Call it
// when the request finishes; it is safe to call more than once and on a
// denied decision.
func (d *Decision) Release() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		if d.release != nil {
			d.release()
		}
	})
}

// DeniedBy returns the first rule that denied the request.
func (d *Decision) DeniedBy() (Outcome, bool) {
	for _, o := range d.Outcomes {
		if !o.Allowed {
			return o, true
		}
	}
	return Outcome{}, false
}

// Tightest returns the rate outcome a client should pace itself by: when
// denied by rate rules, the denying rule with the longest wait; otherwise
// the rule with the fewest remaining units. ok is false when the request
// matched no rate rule.
func (d *Decision) Tightest() (o Outcome, ok bool) {
	if !d.Allowed {
		for _, c := range d.Outcomes {
			if c.Concurrency || c.Allowed {
				continue
			}
			if !ok || c.Result.RetryAfter > o.Result.RetryAfter {
				o, ok = c, true
			}
		}
		if ok {
			return o, true
		}
	}
	for _, c := range d.Outcomes {
		if c.Concurrency {
			continue
		}
		if !ok || c.Result.Remaining < o.Result.Remaining {
			o, ok = c, true
		}
	}
	return o, ok
}

// RetryAfter is the longest wait among rate rules that denied. ok is false
// when no rate rule denied with a wait that would help: the request was
// allowed, denied by a concurrency rule, or its cost exceeds a burst.
func (d *Decision) RetryAfter() (wait time.Duration, ok bool) {
	for _, o := range d.Outcomes {
		if o.Allowed || o.Concurrency {
			continue
		}
		if o.CostExceeded {
			return 0, false
		}
		if o.Result.RetryAfter > wait {
			wait = o.Result.RetryAfter
		}
		ok = true
	}
	return wait, ok
}

func (e *Engine) plan(name string) *Plan {
	if p, ok := e.plans[name]; ok {
		return p
	}
	return e.plans[e.cfg.DefaultPlan]
}

func (e *Engine) key(r Rule, req Request) (string, bool) {
	var b strings.Builder
	b.WriteString(e.cfg.KeyPrefix)
	b.WriteString(r.Name)
	for _, d := range r.Dimensions {
		v := req.Value(d)
		if v == "" {
			return "", false
		}
		if d.sensitive() {
			v = pseudonymize(e.cfg.KeySecret, d, v)
		} else {
			v = keyEscaper.Replace(v)
		}
		b.WriteByte(':')
		b.WriteString(string(d))
		b.WriteByte('=')
		b.WriteString(v)
	}
	return b.String(), true
}

func applies(r Rule, endpoint string) bool {
	if len(r.Endpoints) == 0 {
		return true
	}
	for _, ep := range r.Endpoints {
		if ep == endpoint {
			return true
		}
	}
	return false
}

// Allow decides req. A denial is not an error; errors come from the stores
// (unavailable, or a configuration that produces invalid requests) and
// leave the decision to the caller, typically a fail-open or fail-closed
// switch. Call Release on an admitted decision when the request is done.
func (e *Engine) Allow(ctx context.Context, req Request) (*Decision, error) {
	plan := e.plan(req.Plan)
	cost := req.cost()
	d := &Decision{Allowed: true}

	type rateRule struct {
		store limiter.Store
		req   limiter.Request
		out   int // index into d.Outcomes
	}
	var rates []rateRule
	var slots []Slot
	var slotOut []int
	for _, r := range e.cfg.Rules {
		if !applies(r, req.Endpoint) {
			continue
		}
		key, ok := e.key(r, req)
		if !ok {
			continue
		}
		if r.concurrency() {
			limit := r.MaxInFlight
			if plan != nil {
				if n, ok := plan.MaxInFlight[r.Name]; ok {
					limit = n
				} else if plan.Scale > 0 {
					limit = max(1, int(float64(limit)*plan.Scale))
				}
			}
			slots = append(slots, Slot{Key: key, Max: limit})
			slotOut = append(slotOut, len(d.Outcomes))
			d.Outcomes = append(d.Outcomes, Outcome{Rule: r.Name, Key: key, Allowed: true, Cost: 1, Concurrency: true, MaxInFlight: limit})
			continue
		}
		l := r.Limit
		if plan != nil {
			if o, ok := plan.Limits[r.Name]; ok {
				l = o
			} else if plan.Scale > 0 && plan.Scale != 1 {
				l = l.Scale(plan.Scale)
			}
		}
		c := cost
		if r.Unweighted {
			c = 1
		}
		o := Outcome{Rule: r.Name, Key: key, Allowed: true, Cost: c, Limit: l}
		if c > l.EffectiveBurst() {
			o.Allowed, o.CostExceeded = false, true
			o.Result = limiter.Result{Limit: l}
			d.Allowed = false
		}
		store := r.Store
		if store == nil {
			store = e.cfg.Store
		}
		rates = append(rates, rateRule{store: store, req: limiter.Request{Key: key, Limit: l, Cost: c}, out: len(d.Outcomes)})
		d.Outcomes = append(d.Outcomes, o)
	}
	if !d.Allowed {
		// A cost above some rule's burst can never pass; deny without
		// touching any state, reporting only the rules that cannot pass.
		d.Outcomes = keep(d.Outcomes, func(o Outcome) bool { return o.CostExceeded })
		return d, nil
	}

	release, denied := e.conc.Acquire(slots...)
	if denied >= 0 {
		d.Allowed = false
		d.Outcomes[slotOut[denied]].Allowed = false
		// Rate rules were not evaluated; do not report them.
		d.Outcomes = keep(d.Outcomes, func(o Outcome) bool { return o.Concurrency })
		return d, nil
	}
	if len(rates) == 0 {
		d.release = release
		return d, nil
	}

	// One batch per store, in rule order.
	var batches []limiter.Batch
	var index [][]int
	for i, rr := range rates {
		j := 0
		for j < len(batches) && batches[j].Store != rr.store {
			j++
		}
		if j == len(batches) {
			batches = append(batches, limiter.Batch{Store: rr.store})
			index = append(index, nil)
		}
		batches[j].Requests = append(batches[j].Requests, rr.req)
		index[j] = append(index[j], i)
	}
	results, ok, err := limiter.TakeAll(ctx, batches...)
	if err != nil {
		release()
		return nil, err
	}
	for j := range batches {
		for k, i := range index[j] {
			o := &d.Outcomes[rates[i].out]
			o.Result = results[j][k]
			o.Allowed = o.Result.Allowed
		}
	}
	if !ok {
		release()
		d.Allowed = false
		if _, any := d.DeniedBy(); !any {
			// Defensive: a denial must name a rule. Blame the first rate
			// rule if the stores reported none.
			for i := range d.Outcomes {
				if !d.Outcomes[i].Concurrency {
					d.Outcomes[i].Allowed = false
					break
				}
			}
		}
		return d, nil
	}
	d.release = release
	return d, nil
}

func keep(os []Outcome, f func(Outcome) bool) []Outcome {
	out := os[:0]
	for _, o := range os {
		if f(o) {
			out = append(out, o)
		}
	}
	return out
}
