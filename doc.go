// Package ratelimit is the root of ratelimit-go, rate limiting for payment
// APIs, local and distributed, using only the Go standard library.
//
// The module is organized in layers, each usable on its own:
//
//   - [github.com/victorcondackdev/ratelimit-go/limiter]: the algorithms
//     (GCRA, token bucket, sliding window), the Store interface, an
//     in-memory sharded store, the check-then-commit protocol for limits on
//     different stores, and a circuit-breaking fallback store.
//   - [github.com/victorcondackdev/ratelimit-go/redisstore]: a Store that
//     runs GCRA atomically in Redis with a Lua script and Redis' own clock.
//   - [github.com/victorcondackdev/ratelimit-go/resp]: the minimal RESP2/RESP3
//     Redis client the Redis store is built on.
//   - [github.com/victorcondackdev/ratelimit-go/policy]: multi-dimensional,
//     cost-weighted policies (API key, merchant, payer document, endpoint,
//     IP), tiered plans and concurrency limits.
//   - [github.com/victorcondackdev/ratelimit-go/httplimit]: net/http
//     middleware with RateLimit headers, problem+json denials, shadow mode
//     and fail-open/fail-closed handling.
//   - [github.com/victorcondackdev/ratelimit-go/metrics]: Prometheus text
//     metrics without a client library.
//
// This package contains no code.
package ratelimit
