# ratelimit-go

[![CI](https://github.com/victorcondackdev/ratelimit-go/actions/workflows/ci.yml/badge.svg)](https://github.com/victorcondackdev/ratelimit-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/victorcondackdev/ratelimit-go.svg)](https://pkg.go.dev/github.com/victorcondackdev/ratelimit-go)

Rate limiting for payment APIs, in process or shared through Redis, using
only the Go standard library. That includes the Redis client.

A payment API needs more than "100 requests per minute". A Pix transfer and a
bulk payout of 500 transfers should not cost the same. A payer document
moving money in a burst is a fraud signal and should be limited no matter
which merchant or API key it comes through. When one limit denies, the
others must not be charged. And when Redis goes down, the API should
degrade, not stop and not turn into a free-for-all. ratelimit-go handles
all of these:

- **GCRA**, plus a token bucket and a sliding window. Each one is
  property-tested against a naive reference model with a fake clock.
- **Distributed limits on Redis.** One Lua script decides a batch of limits
  atomically, using Redis' clock, so skew between app servers has no effect.
  Each key is one integer with a TTL. Keys work with Redis Cluster.
- **Multi-dimensional, cost-weighted policies.** Rules key on API key,
  merchant, payer CPF/CNPJ, endpoint and IP. Plans set tiers, concurrency
  caps limit in-flight work, and a request is admitted only when every rule
  passes. A denial charges nothing.
- **net/http middleware.** It sends the IETF draft `RateLimit-Policy` /
  `RateLimit` headers and the legacy `X-RateLimit-*` ones, answers 429 with
  `application/problem+json`, and has a shadow mode and fail-open/fail-closed
  switch.
- **Graceful degradation.** A circuit breaker moves decisions to a local
  limiter with per-replica limits while Redis is down, and moves them back
  when it recovers.
- **Observability.** Metrics come out in Prometheus text format (no client
  library). Logs go through `log/slog`, and payer documents and API keys are
  masked whenever they are logged.
- **LGPD-aware keys.** Store keys never hold raw CPFs, CNPJs or API keys.
  They are HMAC pseudonyms.

Go 1.22+, no dependencies.

```sh
go get github.com/victorcondackdev/ratelimit-go
```

## Quick start

```go
store := limiter.NewMemoryStore(limiter.MemoryOptions{}) // or redisstore.New(...)
defer store.Close()

engine, err := policy.New(policy.Config{
	Store:     store,
	KeySecret: []byte(os.Getenv("RATELIMIT_KEY_SECRET")),
	Rules: []policy.Rule{
		// Throughput per merchant: 120 transfers/min, bursts of 30.
		{Name: "merchant-pix", Dimensions: []policy.Dimension{policy.Merchant},
			Limit: limiter.PerMinute(120).WithBurst(30)},
		// Fraud containment per payer document: 20/hour, bursts of 5.
		{Name: "payer-pix", Dimensions: []policy.Dimension{policy.Payer},
			Limit: limiter.PerHour(20).WithBurst(5)},
		// Requests per IP, regardless of cost.
		{Name: "ip", Dimensions: []policy.Dimension{policy.IP},
			Limit: limiter.PerMinute(300), Unweighted: true},
		// At most 2 bulk payouts in flight per merchant.
		{Name: "payout-inflight", Dimensions: []policy.Dimension{policy.Merchant},
			MaxInFlight: 2, Endpoints: []string{"POST /payouts/bulk"}},
	},
})
if err != nil {
	log.Fatal(err)
}

mw := httplimit.Middleware(engine, httplimit.Options{
	Extract: extract,              // API key -> merchant/plan, body -> payer and cost
	OnError: httplimit.FailClosed, // payer limits are a risk control
})
http.ListenAndServe(":8080", mw(mux))
```

With a burst of 2 per merchant and 5 per payer, three requests from the same
payer look like this (see `httplimit.ExampleMiddleware`):

```
201  RateLimit: "merchant";r=1;t=1, "payer";r=4;t=180
201  RateLimit: "merchant";r=0;t=1, "payer";r=3;t=360
429  RateLimit: "merchant";r=0;t=1, "payer";r=3;t=360   Retry-After: 1
```

```json
{
  "type": "about:blank",
  "title": "Too Many Requests",
  "status": 429,
  "detail": "rate limit \"merchant\" exceeded; retry in 500ms",
  "policy": "merchant",
  "retry_after": 1
}
```

[`examples/payments-api`](examples/payments-api) is a complete service. It
has per-key, per-merchant and per-payer Pix limits, bulk payouts that cost
one unit per item, a payout concurrency cap, starter/growth/enterprise plans,
`/metrics`, and Redis with a local fallback (`REDIS_URL`, `REPLICAS`).

## Packages

| Package | What it does |
|---|---|
| [`limiter`](limiter) | Algorithms, `Store` interface, sharded memory store, `TakeAll` (check-then-commit), `FallbackStore` + circuit breaker |
| [`redisstore`](redisstore) | GCRA in Redis via one Lua script, cluster-aware |
| [`resp`](resp) | Minimal RESP2/RESP3 client: pool, pipelining, EVALSHA/NOSCRIPT, AUTH/SELECT/HELLO, TLS, MOVED/ASK |
| [`policy`](policy) | Rules, dimensions, plans, cost weighting, concurrency limits |
| [`httplimit`](httplimit) | net/http middleware |
| [`metrics`](metrics) | Prometheus text exposition |

## Why GCRA

The generic cell rate algorithm comes from ATM networks. It is a leaky bucket
in "virtual scheduling" form. Each key stores one number: its **theoretical
arrival time** (TAT), the moment the key would be idle again if traffic
stopped. With emission interval `T = Period/Rate` and tolerance `tau = T*Burst`,
a request of cost `c` at time `now` conforms iff

```
max(TAT, now) + c*T - tau <= now
```

When it does, `TAT` becomes `max(TAT, now) + c*T`. That one comparison
yields everything a client needs:

- `Remaining = floor((tau - (TAT' - now)) / T)`
- `RetryAfter = max(TAT, now) + c*T - tau - now` when denied
- `ResetAfter = TAT' - now`

The state is one timestamp per key: no counters, no window boundaries, no
refill timestamp. In Redis that means a single integer per key. Its TTL is
`TAT - now`, so a key deletes itself exactly when it stops carrying
information. Costs slot in with no extra work, because a bulk payout of N
just advances TAT by N intervals. The core is
[`limiter.GCRAStep`](limiter/algorithm.go). The Lua script is the same
arithmetic in microseconds, and a test runs the real script against
`GCRAStep` step by step.

### Algorithms compared

| | GCRA | Token bucket | Sliding window (two windows) |
|---|---|---|---|
| State per key | 1 int (TAT) | 2 ints (deficit, last update) | 3 ints (window start, prev, curr) |
| Burst control | explicit (`Burst`) | explicit (`Burst`) | implicit, up to `Rate` |
| Decisions | exact leaky bucket | identical to GCRA | approximation of a sliding log |
| Cost-weighted | yes | yes | yes |
| `RetryAfter` | exact | exact | exact for the approximation |
| Redis store | yes | no | no |
| Memory store, hot key, parallel | 249 ns/op | 225 ns/op | 661 ns/op |

The token bucket here stores the bucket as a *deficit*, the refill time owed.
It is the same machine as GCRA. A property test runs both side by side for
20,000 random steps, with refunds mixed in, and checks they agree on every
decision and every number. The token bucket is included for people who
think in tokens. GCRA is the better choice because it stores less state.

The sliding window keeps counts for the current and previous fixed windows
and weights the previous one by how much of it still overlaps the sliding
window: `prev*(W-e)/W + curr + cost <= Rate`. Its arithmetic is exact
128-bit integer math, with no floats. It is checked against a reference that
keeps a full request log and finds `RetryAfter` and `ResetAfter` by brute
force. Use it when limits must read as "N per window". Otherwise prefer GCRA.

Every algorithm is a pure function of `(state, now, limit, cost)`. None of
them reads a clock. That is why one implementation serves the memory store,
the Redis emulation and the tests.

## Distributed design

```
app instance A ─┐                     ┌──────────── Redis ────────────┐
app instance B ─┼── EVALSHA gcra ───▶ │ now = TIME                    │
app instance C ─┘   (keys, T, burst,  │ for each key: GCRA(TAT, now)  │
                     cost per key)    │ all conform? SET TAT PX ttl   │
                                      └───────────────────────────────┘
```

- **One script, one round trip, atomic.** [`gcra.lua`](redisstore/gcra.lua)
  reads every key in the batch, applies GCRA in order and writes all new
  TATs only if every request conforms. Redis runs scripts one at a time, so
  the batch is one serializable step. Duplicate keys in a batch see each
  other's effects.
- **Redis' clock, not the app's.** The script reads `TIME`. If app servers
  passed `now` in, an instance whose clock ran 2s fast would see every key 2s
  "older" than it is and admit traffic the others would deny. Clock skew
  between instances would become a hole in the limit. With `TIME`, every
  instance has the same view of every key. Effects replication, the default
  since Redis 5 and forced on older versions with `redis.replicate_commands()`,
  makes replicas apply the resulting `SET`s instead of re-running a
  non-deterministic script. One caveat remains: after a failover the new
  primary's clock takes over. A forward jump makes keys look older for a
  moment (more permissive), and a backward jump makes them younger (more
  restrictive). NTP keeps this to milliseconds.
- **Microseconds.** Lua numbers are IEEE doubles. Microsecond Unix
  timestamps (~1.8e15) stay below 2^53, where doubles are exact integers.
  Nanoseconds would not. TATs are written with `%.0f`, never with Lua's
  default `%.14g`, and an integration test checks the stored value parses as
  an integer. Emission intervals are therefore truncated to whole
  microseconds on every store, so a limit behaves the same everywhere.
- **EVALSHA with NOSCRIPT fallback.** After a restart, a failover or a
  `SCRIPT FLUSH`, the client retries with `EVAL`, which also reloads the
  cache.
- **Cluster-friendly keys.** A logical key `k` is stored as `prefix{k}`, so
  everything for one logical key shares a slot. A key that already has a
  hash tag is used as is, so you can co-locate keys on purpose, for example
  `{merchant:42}:pix` and `{merchant:42}:payout`. Then a batch over them is
  still one atomic script. With `Options.Cluster`, batches that span slots
  are split by slot and committed with the protocol below. The client
  follows `MOVED`/`ASK` and caches slot owners. It does not discover cluster
  topology on its own.

## Combining limits: atomicity and its limits

A request is admitted only if every matching rule passes. If one denies,
the others must not be charged, or a payer at their limit would keep
draining their merchant's budget with requests that never go through.

| Where the limits live | Mechanism | Guarantee |
|---|---|---|
| One `MemoryStore` | locks the shards of all keys in index order | atomic, serializable |
| One Redis node, or keys in one cluster slot | one Lua script | atomic, serializable |
| Different stores, or different cluster slots | `limiter.TakeAll`: check-then-commit with refunds | see below |
| Concurrency rules | one in-process mutex, all-or-nothing | atomic, local to the instance |

`TakeAll` runs in two phases:

1. **Check.** `Peek` every batch. If anything would be denied, stop.
   Nothing was consumed anywhere.
2. **Commit.** `Take` each batch in order. If a commit is denied, because
   another caller took the capacity between check and commit, or fails,
   `Refund` the batches already committed.

This is not a distributed transaction, and it does not pretend to be one.
Its failure modes all err toward restriction:

- Between a commit and its refund, a concurrent request can be denied by
  capacity that is about to be returned (a false denial, briefly).
- If the process crashes, or the refund fails, between commit and refund,
  the capacity stays consumed until it decays. It is not leaked forever.
  A failed refund on a denial is not reported as an error, because a
  fail-open caller would then admit the request.
- It never admits more than any single limit allows. Every commit is an
  ordinary, individually atomic `Take`.

A GCRA refund subtracts `cost*T` from the TAT, which exactly undoes the
take when nothing else happened in between. If the TAT had already fallen
behind `now`, the refund clamps at idle. It never makes a key more
permissive than idle.

The engine checks concurrency rules first and holds their slots only if the
rate rules pass. A request whose cost exceeds some rule's burst, which no
amount of waiting can fix, is denied before any store is touched. It gets a
429 with `max_cost` in the body and no `Retry-After`.

## Middleware

- **Headers.** `RateLimit-Policy: "merchant-pix";q=30;w=15` and
  `RateLimit: "merchant-pix";r=29;t=1` follow the IETF HTTPAPI working
  group's [RateLimit header fields draft](https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/).
  It is still an Internet-Draft, so the format may change. There is one list
  member per policy: `q` is the burst, `w` the seconds to refill it, `r` the
  remaining units, `t` the seconds to full reset. `X-RateLimit-Limit`,
  `-Remaining` and `-Reset` (seconds) describe the tightest policy.
  `Retry-After` is rounded up to whole seconds, so clients never retry early.
- **Denials** are RFC 9457 `application/problem+json` with the `policy`,
  `retry_after` and, for oversized requests, `cost` and `max_cost`.
- **Shadow mode** evaluates and records every decision, logs would-be
  denials, and enforces nothing. Use it to roll out a new limit against
  real traffic before it can hurt anyone.
- **Failure policy.** `FailOpen` admits requests when the limiter errors.
  `FailClosed` answers 503. Misconfiguration errors, such as a cost the
  store rejects, are never admitted. To degrade instead of choosing between
  the two, wrap the Redis store:

```go
local := limiter.NewMemoryStore(limiter.MemoryOptions{})
store := limiter.NewFallbackStore(redisstore.New(client, redisstore.Options{}),
	limiter.FallbackOptions{
		Fallback: local,
		Scale:    1.0 / float64(replicas), // keep the fleet near the global budget
		Breaker:  limiter.NewBreaker(limiter.BreakerOptions{FailureThreshold: 5, OpenTimeout: 5 * time.Second}),
	})
```

After 5 consecutive Redis failures the breaker opens, and each instance
enforces `limit/replicas` locally. After 5 seconds a single probe goes back
to Redis, and the breaker closes when it succeeds. Request errors and
cancelled contexts do not count as failures. A test takes the fake Redis down
mid-traffic and checks that the API keeps answering 204/429, never 503.

## Benchmarks

The numbers are from `go test -bench . -benchmem` on a 2-vCPU Intel Xeon @
2.10GHz (linux/amd64, Go 1.24) with Redis 7.0 on the same host. Treat them
as an order of magnitude. On two vCPUs the client and the server compete for
the same cores.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `GCRAStep` (pure function) | 11.6 | 0 | 0 |
| Memory store, GCRA, 1 hot key, parallel | 249 | 64 | 1 |
| Memory store, GCRA, 10k keys, parallel | 346 | 64 | 1 |
| Memory store, token bucket, 1 hot key, parallel | 225 | 64 | 1 |
| Memory store, sliding window, 1 hot key, parallel | 661 | 64 | 1 |
| Memory store, 3-limit atomic batch, parallel | 1338 | 478 | 8 |
| RESP parse, 14-integer script reply | 1142 | 1280 | 1 |
| RESP round trip, PING, fake server over loopback | 6711 | 368 | 9 |
| RESP round trip, EVALSHA, fake server over loopback | 11175 | 2704 | 28 |
| Redis store `Take`, fake server | 14943 | 2928 | 36 |
| Redis store `Take`, 1 key, real Redis 7 | 47449 | 1184 | 14 |
| Redis store `Take`, 3 keys, real Redis 7 | 111235 | 2384 | 18 |
| Redis store `Take`, 1 key, real Redis 7, parallel | 36287 | 1184 | 14 |

The single allocation per memory-store decision is the result slice.
A Redis-backed decision costs one round trip whatever the batch size. The
network dominates the cost of the script.

```sh
go test -run '^$' -bench . -benchmem ./limiter ./resp ./redisstore
REDIS_URL=redis://localhost:6379 go test -tags integration -run '^$' -bench Integration ./redisstore
```

## Testing

- **Property tests with a fake clock.** GCRA and the token bucket are
  checked against a rational-arithmetic leaky bucket (`math/big`). The
  sliding window is checked against a full request log with brute-force
  searches. Both run over random traffic across several limits and seeds:
  simultaneous requests, long idles, costs from 0 to the burst.
- **Exactness under contention.** Goroutines racing on one key under
  `-race` must get exactly `Burst` admissions with the clock frozen, never
  one more. The same check runs against the fake server and against real
  Redis.
- **Fake RESP server.** [`internal/redistest`](internal/redistest) speaks
  RESP2/RESP3 over TCP (and TLS). It implements every command the client
  and store use, and runs a Go emulation of the Lua script built on
  `GCRAStep`. It can inject faults: refused connections, latency,
  `MOVED`/`ASK`, `CROSSSLOT`, scripted error replies, a flushed script cache.
  A randomized test drives the Redis store through it and the memory store
  side by side, and requires identical results.
- **Real Redis.** Integration tests run under the `integration` build tag.
  CI runs them against a `redis:7` service container over RESP2 and RESP3.
  They include a step-by-step comparison of the real Lua script with
  `GCRAStep`, using the `now` the script itself read.
- **Fuzzing.** `FuzzReadValue` feeds arbitrary bytes to the RESP parser,
  checking for no panic, no unbounded allocation and an exact re-encode
  round trip.
- Example tests with `// Output:` document the main entry points. Statement
  coverage is 91% measured package by package and 97% with
  `-coverpkg=./...`. Every library package is at 97-100%. The rest is the
  example's `main` and the test-only fake server.

```sh
go test -race ./...
REDIS_URL=redis://localhost:6379/15 go test -race -tags integration ./redisstore
go test -run '^$' -fuzz FuzzReadValue -fuzztime 15s ./resp
```

## Design notes

- **Why a home-grown Redis client.** The limiter needs a small part of
  Redis, and that part has to be right: pooled connections, deadlines
  derived from the context, connections discarded after any transport or
  protocol error (their stream position is unknown), EVALSHA with fallback,
  and a parser that does not trust lengths it is given. Bulk strings over
  64 KiB are read in chunks rather than allocated from the declared length.
  Aggregate sizes and nesting depth are bounded. Owning about a thousand lines
  instead of a dependency tree is a good trade for a library meant to sit
  on the payment path.
- **Results report state, not hypotheticals.** When a batch is denied,
  requests that would have passed on their own report `Allowed: true` and
  their current, unconsumed `Remaining`. Callers can tell which limit denied
  and how much room the others have.
- **Memory store bounds.** Keys are deleted as soon as their state decays
  to idle, so memory tracks recently active keys. `MaxKeys` caps the total.
  When a shard is full, expired keys from a random sample are reclaimed
  first. Failing that, the sampled key closest to expiry is evicted, which
  is the eviction that forgives the least. A background sweeper is optional.
- **Payer data.** CPF/CNPJ values are normalized (punctuation stripped,
  letters upper-cased for the alphanumeric CNPJ) and then pseudonymized with
  HMAC-SHA-256 under `Config.KeySecret`. The CPF space is about 10^9, so an
  unkeyed hash would be trivial to reverse. `policy.Request` implements
  `slog.LogValuer`: logging one prints `***.456.789-**`, never the document
  or the API key.
- **Concurrency limits are local.** A distributed in-flight limit needs
  leases with expiry so that a crashed instance does not hold slots forever.
  That is a different problem, and this library does not pretend to solve it.
- **Not in scope.** Cluster topology discovery (the client follows
  redirects but does not read `CLUSTER SLOTS`), sliding window and token
  bucket on Redis (GCRA covers the same ground with less state), and a
  request queue or delay mode (this library decides; it does not schedule).

## License

[MIT](LICENSE)
