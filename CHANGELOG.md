# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-11-22

First public release.

### Added

- `limiter`: GCRA (`GCRAStep`), token bucket and two-window sliding window
  as pure, clock-free algorithms with cost-weighted requests and refunds;
  `Limit` with burst, validation and scaling; `Store` interface with
  all-or-nothing batches; sharded `MemoryStore` with TTL eviction, a key
  bound and atomic multi-key batches; `TakeAll` check-then-commit protocol
  across stores; `FallbackStore` with a consecutive-failure circuit breaker
  and scaled local limits; `ManualClock` for tests.
- `redisstore`: GCRA in one Lua script that decides a batch atomically using
  Redis `TIME`; one integer per key with TTL equal to its reset time;
  hash-tagged keys and slot-split batches for Redis Cluster.
- `resp`: RESP2/RESP3 parser and encoder with allocation limits; pooled
  client with pipelining, per-command timeouts and context cancellation,
  AUTH/SELECT/HELLO/CLIENT SETNAME on connect, TLS, EVALSHA with NOSCRIPT
  fallback, MOVED/ASK redirects, `redis://` and `rediss://` URLs.
- `policy`: rules over API key, merchant, payer document, endpoint and IP;
  weighted and unweighted rules; endpoint scoping; tiered plans with scaling
  and overrides; in-process concurrency limits; HMAC pseudonymization of
  sensitive key parts; CPF/CNPJ masking and a masking `slog.LogValuer`.
- `httplimit`: net/http middleware with IETF draft `RateLimit-Policy` and
  `RateLimit` headers, legacy `X-RateLimit-*` and `Retry-After`, RFC 9457
  problem responses, shadow mode, fail-open/fail-closed, key extraction
  helpers including trusted-proxy aware `ClientIP`.
- `metrics`: Prometheus text exposition for decisions, store latency,
  store errors, fallbacks and gauges.
- `examples/payments-api`: Pix API with per-merchant and per-payer limits,
  cost-weighted bulk payouts, a payout concurrency cap, plans, metrics, and
  Redis with local fallback.

[Unreleased]: https://github.com/victorcondackdev/ratelimit-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/victorcondackdev/ratelimit-go/releases/tag/v0.1.0
