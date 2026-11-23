// Package metrics records rate-limiter metrics and exposes them in the
// Prometheus text exposition format (version 0.0.4), without a client
// library.
//
// All methods are safe on a nil *Registry, which records nothing, so
// instrumentation can be left in place when metrics are disabled.
package metrics

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Decision outcomes recorded by [Registry.Decision].
const (
	Allowed      = "allowed"
	Denied       = "denied"
	ShadowDenied = "shadow_denied" // would have been denied; shadow mode let it through
)

// DefaultBuckets are the store latency histogram bounds, in seconds: 100µs
// to 1s, which brackets a local Redis round trip through a timeout.
var DefaultBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}

// Registry holds the metrics.
type Registry struct {
	mu        sync.Mutex
	buckets   []float64
	decisions map[[2]string]uint64
	latency   map[string]*histogram
	storeErrs map[string]uint64
	fallbacks map[[2]string]uint64
	errs      map[string]uint64
	gauges    map[string]func() float64
}

type histogram struct {
	counts []uint64 // per bucket, non-cumulative; last is +Inf
	sum    float64
	n      uint64
}

// New returns an empty Registry using DefaultBuckets.
func New() *Registry {
	return &Registry{
		buckets:   DefaultBuckets,
		decisions: map[[2]string]uint64{},
		latency:   map[string]*histogram{},
		storeErrs: map[string]uint64{},
		fallbacks: map[[2]string]uint64{},
		errs:      map[string]uint64{},
		gauges:    map[string]func() float64{},
	}
}

// Decision counts one rate-limit decision for a policy (rule name).
func (r *Registry) Decision(policy, outcome string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.decisions[[2]string{policy, outcome}]++
	r.mu.Unlock()
}

// StoreLatency records the duration of a store operation and, if err is
// not nil, counts a store error.
func (r *Registry) StoreLatency(op string, d time.Duration, err error) {
	if r == nil {
		return
	}
	s := d.Seconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.latency[op]
	if !ok {
		h = &histogram{counts: make([]uint64, len(r.buckets)+1)}
		r.latency[op] = h
	}
	i := sort.SearchFloat64s(r.buckets, s)
	h.counts[i]++
	h.sum += s
	h.n++
	if err != nil {
		r.storeErrs[op]++
	}
}

// Fallback counts a store call served by (or destined for) the local
// fallback.
func (r *Registry) Fallback(op, reason string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.fallbacks[[2]string{op, reason}]++
	r.mu.Unlock()
}

// Error counts an error of the given kind, for example "store" when a
// limiter error made the middleware fail open or closed.
func (r *Registry) Error(kind string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.errs[kind]++
	r.mu.Unlock()
}

// Gauge registers a gauge whose value is read at scrape time, such as the
// circuit breaker state or the memory store's key count. name must be a
// valid metric name; registering it again replaces the function.
func (r *Registry) Gauge(name string, fn func() float64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.gauges[name] = fn
	r.mu.Unlock()
}

// ServeHTTP serves the metrics in the Prometheus text format.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = r.WriteTo(w)
}

// WriteTo writes every metric in the Prometheus text format, in a stable
// order.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	cw := &countingWriter{w: bufio.NewWriter(w)}
	if r == nil {
		return 0, nil
	}
	r.mu.Lock()
	gauges := make(map[string]func() float64, len(r.gauges))
	for k, v := range r.gauges {
		gauges[k] = v
	}

	cw.printf("# HELP ratelimit_decisions_total Rate-limit decisions by policy and outcome.\n# TYPE ratelimit_decisions_total counter\n")
	for _, k := range sortedPairs(r.decisions) {
		cw.printf("ratelimit_decisions_total{policy=%s,decision=%s} %d\n", quote(k[0]), quote(k[1]), r.decisions[k])
	}

	cw.printf("# HELP ratelimit_store_duration_seconds Latency of limiter store operations.\n# TYPE ratelimit_store_duration_seconds histogram\n")
	ops := make([]string, 0, len(r.latency))
	for op := range r.latency {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	for _, op := range ops {
		h := r.latency[op]
		var cum uint64
		for i, b := range r.buckets {
			cum += h.counts[i]
			cw.printf("ratelimit_store_duration_seconds_bucket{op=%s,le=%s} %d\n", quote(op), quote(formatFloat(b)), cum)
		}
		cum += h.counts[len(r.buckets)]
		cw.printf("ratelimit_store_duration_seconds_bucket{op=%s,le=\"+Inf\"} %d\n", quote(op), cum)
		cw.printf("ratelimit_store_duration_seconds_sum{op=%s} %s\n", quote(op), formatFloat(h.sum))
		cw.printf("ratelimit_store_duration_seconds_count{op=%s} %d\n", quote(op), h.n)
	}

	cw.printf("# HELP ratelimit_store_errors_total Failed limiter store operations.\n# TYPE ratelimit_store_errors_total counter\n")
	for _, op := range sortedKeys(r.storeErrs) {
		cw.printf("ratelimit_store_errors_total{op=%s} %d\n", quote(op), r.storeErrs[op])
	}

	cw.printf("# HELP ratelimit_fallback_total Store calls served by the local fallback.\n# TYPE ratelimit_fallback_total counter\n")
	for _, k := range sortedPairs(r.fallbacks) {
		cw.printf("ratelimit_fallback_total{op=%s,reason=%s} %d\n", quote(k[0]), quote(k[1]), r.fallbacks[k])
	}

	cw.printf("# HELP ratelimit_errors_total Errors handled by the middleware.\n# TYPE ratelimit_errors_total counter\n")
	for _, k := range sortedKeys(r.errs) {
		cw.printf("ratelimit_errors_total{kind=%s} %d\n", quote(k), r.errs[k])
	}
	r.mu.Unlock()

	// Gauge functions may take their own locks; call them unlocked.
	for _, name := range sortedKeys(gauges) {
		cw.printf("# TYPE %s gauge\n%s %s\n", name, name, formatFloat(gauges[name]()))
	}
	if err := cw.w.Flush(); err != nil && cw.err == nil {
		cw.err = err
	}
	return cw.n, cw.err
}

type countingWriter struct {
	w   *bufio.Writer
	n   int64
	err error
}

func (c *countingWriter) printf(format string, args ...any) {
	if c.err != nil {
		return
	}
	n, err := fmt.Fprintf(c.w, format, args...)
	c.n += int64(n)
	c.err = err
}

func sortedPairs(m map[[2]string]uint64) [][2]string {
	keys := make([][2]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	return keys
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func quote(s string) string { return `"` + labelEscaper.Replace(s) + `"` }

func formatFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
