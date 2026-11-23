package httplimit_test

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"

	"github.com/victorcondackdev/ratelimit-go/httplimit"
	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/policy"
)

func ExampleMiddleware() {
	store := limiter.NewMemoryStore(limiter.MemoryOptions{})
	defer store.Close()

	engine, err := policy.New(policy.Config{
		Store: store,
		Rules: []policy.Rule{
			{Name: "merchant", Dimensions: []policy.Dimension{policy.Merchant}, Limit: limiter.PerMinute(120).WithBurst(2)},
			{Name: "payer", Dimensions: []policy.Dimension{policy.Payer}, Limit: limiter.PerHour(20).WithBurst(5)},
		},
	})
	if err != nil {
		panic(err)
	}
	pix := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	h := httplimit.Middleware(engine, httplimit.Options{
		OnError: httplimit.FailClosed,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})(pix)

	for i := 0; i < 3; i++ {
		r := httptest.NewRequest("POST", "/pix/transfers", nil)
		r.Header.Set("X-Merchant-ID", "merchant-42")
		r.Header.Set("X-Payer-Document", "123.456.789-09")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		fmt.Printf("%d %s | Retry-After: %q\n", w.Code, w.Header().Get("RateLimit"), w.Header().Get("Retry-After"))
	}
	// Output:
	// 201 "merchant";r=1;t=1, "payer";r=4;t=180 | Retry-After: ""
	// 201 "merchant";r=0;t=1, "payer";r=3;t=360 | Retry-After: ""
	// 429 "merchant";r=0;t=1, "payer";r=3;t=360 | Retry-After: "1"
}
