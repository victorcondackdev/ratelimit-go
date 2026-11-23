package metrics_test

import (
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/metrics"
)

func Example() {
	reg := metrics.New()
	reg.Decision("merchant-pix", metrics.Allowed)
	reg.Decision("merchant-pix", metrics.Allowed)
	reg.Decision("payer-pix", metrics.Denied)
	reg.StoreLatency("take", 700*time.Microsecond, nil)
	reg.Fallback("take", "open")
	reg.Gauge("ratelimit_memory_keys", func() float64 { return 42 })
	_, _ = reg.WriteTo(os.Stdout)
	// Output:
	// # HELP ratelimit_decisions_total Rate-limit decisions by policy and outcome.
	// # TYPE ratelimit_decisions_total counter
	// ratelimit_decisions_total{policy="merchant-pix",decision="allowed"} 2
	// ratelimit_decisions_total{policy="payer-pix",decision="denied"} 1
	// # HELP ratelimit_store_duration_seconds Latency of limiter store operations.
	// # TYPE ratelimit_store_duration_seconds histogram
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.0001"} 0
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.00025"} 0
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.0005"} 0
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.001"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.0025"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.005"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.01"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.025"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.05"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.1"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.25"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="0.5"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="1"} 1
	// ratelimit_store_duration_seconds_bucket{op="take",le="+Inf"} 1
	// ratelimit_store_duration_seconds_sum{op="take"} 0.0007
	// ratelimit_store_duration_seconds_count{op="take"} 1
	// # HELP ratelimit_store_errors_total Failed limiter store operations.
	// # TYPE ratelimit_store_errors_total counter
	// # HELP ratelimit_fallback_total Store calls served by the local fallback.
	// # TYPE ratelimit_fallback_total counter
	// ratelimit_fallback_total{op="take",reason="open"} 1
	// # HELP ratelimit_errors_total Errors handled by the middleware.
	// # TYPE ratelimit_errors_total counter
	// # TYPE ratelimit_memory_keys gauge
	// ratelimit_memory_keys 42
}

func TestRegistry(t *testing.T) {
	reg := metrics.New()
	reg.StoreLatency("take", 2*time.Second, errors.New("x"))
	reg.Error("limiter")
	reg.Decision("weird\"name\n\\", metrics.ShadowDenied)
	reg.Gauge("g_inf", func() float64 { return posInf() })
	reg.Gauge("g_ninf", func() float64 { return -posInf() })
	reg.Gauge("g_nan", func() float64 { return posInf() - posInf() })
	w := httptest.NewRecorder()
	reg.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, want := range []string{
		`ratelimit_store_duration_seconds_bucket{op="take",le="1"} 0`,
		`ratelimit_store_duration_seconds_bucket{op="take",le="+Inf"} 1`,
		`ratelimit_store_errors_total{op="take"} 1`,
		`ratelimit_errors_total{kind="limiter"} 1`,
		`policy="weird\"name\n\\"`,
		"g_inf +Inf", "g_ninf -Inf", "g_nan NaN",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in\n%s", want, body)
		}
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("Content-Type %q", ct)
	}
}

func posInf() float64 {
	var zero float64
	return 1 / zero
}

func TestNilRegistry(t *testing.T) {
	var reg *metrics.Registry
	reg.Decision("p", metrics.Allowed)
	reg.StoreLatency("take", time.Millisecond, nil)
	reg.Fallback("take", "open")
	reg.Error("x")
	reg.Gauge("g", func() float64 { return 1 })
	if n, err := reg.WriteTo(&strings.Builder{}); n != 0 || err != nil {
		t.Fatal("nil registry writes nothing")
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestWriteError(t *testing.T) {
	reg := metrics.New()
	for i := 0; i < 1000; i++ {
		reg.Decision(strings.Repeat("p", i%50), metrics.Allowed)
	}
	if _, err := reg.WriteTo(failWriter{}); err == nil {
		t.Fatal("want write error")
	}
}
