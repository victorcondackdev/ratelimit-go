// Package redisstore is a distributed [limiter.Store] backed by Redis.
//
// Every decision runs inside one Lua script, server-side and atomically: the
// script reads each key's theoretical arrival time (TAT), applies GCRA for
// every request in the batch, and writes the new TATs only if all of them
// conform. Time comes from Redis' own clock (the TIME command inside the
// script), not from the application servers, so instances with skewed
// clocks still agree on every key's state.
//
// Each key holds a single integer, its TAT in microseconds, with a TTL that
// expires it exactly when it becomes idle again.
//
// Keys are written as Prefix + "{" + key + "}" so that, in a Redis Cluster,
// everything stored for one logical key lives in one slot. A key that
// already contains a hash tag is used as is, which lets callers co-locate
// keys on purpose. With Options.Cluster set, a batch whose keys span
// several slots is split per slot and committed with [limiter.TakeAll]'s
// check-then-commit protocol; otherwise the whole batch is one script and
// one atomic step.
package redisstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

//go:embed gcra.lua
var gcraLua string

// Script is the GCRA Lua script. Its first line is a marker the fake
// server used in tests recognizes.
var Script = resp.NewScript(gcraLua)

// ErrBadReply is wrapped when the script's reply has an unexpected shape.
var ErrBadReply = errors.New("redisstore: unexpected script reply")

// Options configures a Store.
type Options struct {
	// Prefix is prepended to every key. Defaults to "rl:".
	Prefix string
	// Cluster splits batches by hash slot. Leave it off for a single node
	// or a primary with replicas, where multi-key scripts are always
	// allowed and every batch is atomic.
	Cluster bool
	// Observe, if set, is called after every round trip with the operation
	// ("take", "peek", "refund"), its latency and its error.
	Observe func(op string, d time.Duration, err error)
}

// Store is a [limiter.Store] that runs GCRA in Redis.
type Store struct {
	c    *resp.Client
	opts Options
}

// New returns a Store using c.
func New(c *resp.Client, opts Options) *Store {
	if opts.Prefix == "" {
		opts.Prefix = "rl:"
	}
	return &Store{c: c, opts: opts}
}

// Key returns the Redis key used for a logical key.
func (s *Store) Key(key string) string {
	if strings.IndexByte(key, '{') >= 0 && resp.HashTag(key) != key {
		return s.opts.Prefix + key
	}
	return s.opts.Prefix + "{" + key + "}"
}

// Take implements [limiter.Store].
func (s *Store) Take(ctx context.Context, reqs ...limiter.Request) ([]limiter.Result, error) {
	return s.do(ctx, "take", reqs)
}

// Peek implements [limiter.Store].
func (s *Store) Peek(ctx context.Context, reqs ...limiter.Request) ([]limiter.Result, error) {
	return s.do(ctx, "peek", reqs)
}

// Refund implements [limiter.Store].
func (s *Store) Refund(ctx context.Context, reqs ...limiter.Request) error {
	_, err := s.do(ctx, "refund", reqs)
	return err
}

func (s *Store) do(ctx context.Context, mode string, reqs []limiter.Request) ([]limiter.Result, error) {
	if err := limiter.ValidateRequests(reqs); err != nil {
		return nil, err
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	if !s.opts.Cluster {
		return s.eval(ctx, mode, reqs)
	}
	// Group by slot, keeping the caller's order within each group.
	var order []int
	groups := map[int][]int{}
	for i, r := range reqs {
		slot := resp.Slot(s.Key(r.Key))
		if _, ok := groups[slot]; !ok {
			order = append(order, slot)
		}
		groups[slot] = append(groups[slot], i)
	}
	if len(order) == 1 {
		return s.eval(ctx, mode, reqs)
	}
	batches := make([]limiter.Batch, len(order))
	for g, slot := range order {
		b := limiter.Batch{Store: slotStore{s}}
		for _, i := range groups[slot] {
			b.Requests = append(b.Requests, reqs[i])
		}
		batches[g] = b
	}
	out := make([]limiter.Result, len(reqs))
	scatter := func(per [][]limiter.Result) {
		for g, slot := range order {
			for k, i := range groups[slot] {
				out[i] = per[g][k]
			}
		}
	}
	switch mode {
	case "take":
		per, _, err := limiter.TakeAll(ctx, batches...)
		if err != nil {
			return nil, err
		}
		scatter(per)
		return out, nil
	case "peek":
		per := make([][]limiter.Result, len(batches))
		all := true
		for g, b := range batches {
			res, err := s.eval(ctx, mode, b.Requests)
			if err != nil {
				return nil, err
			}
			per[g] = res
			all = all && limiter.AllAllowed(res)
		}
		if !all {
			// Mirror Take: a denied batch consumes nothing, so groups that
			// passed report their unconsumed remaining.
			for g, b := range batches {
				if limiter.AllAllowed(per[g]) {
					for k := range per[g] {
						per[g][k].Remaining += b.Requests[k].Cost
					}
				}
			}
		}
		scatter(per)
		return out, nil
	default:
		var errs error
		for _, b := range batches {
			_, err := s.eval(ctx, mode, b.Requests)
			errs = errors.Join(errs, err)
		}
		return nil, errs
	}
}

// slotStore runs batches that are already known to be single-slot.
type slotStore struct{ s *Store }

func (v slotStore) Take(ctx context.Context, reqs ...limiter.Request) ([]limiter.Result, error) {
	return v.s.eval(ctx, "take", reqs)
}

func (v slotStore) Peek(ctx context.Context, reqs ...limiter.Request) ([]limiter.Result, error) {
	return v.s.eval(ctx, "peek", reqs)
}

func (v slotStore) Refund(ctx context.Context, reqs ...limiter.Request) error {
	_, err := v.s.eval(ctx, "refund", reqs)
	return err
}

func micros(d time.Duration) string { return strconv.FormatInt(int64(d/time.Microsecond), 10) }

func (s *Store) eval(ctx context.Context, mode string, reqs []limiter.Request) ([]limiter.Result, error) {
	keys := make([]string, len(reqs))
	args := make([]string, 0, 1+3*len(reqs))
	args = append(args, mode)
	for i, r := range reqs {
		keys[i] = s.Key(r.Key)
		args = append(args,
			micros(r.Limit.Interval()),
			strconv.FormatInt(r.Limit.EffectiveBurst(), 10),
			strconv.FormatInt(r.Cost, 10))
	}
	start := time.Now()
	v, err := s.c.Run(ctx, Script, keys, args...)
	if s.opts.Observe != nil {
		s.opts.Observe(mode, time.Since(start), err)
	}
	if err != nil {
		return nil, fmt.Errorf("redisstore: %s: %w", mode, err)
	}
	if mode == "refund" {
		return nil, nil
	}
	return parseReply(v, reqs)
}

// parseReply decodes {all, now, (allowed, remaining, retry_us, reset_us)*}.
func parseReply(v resp.Value, reqs []limiter.Request) ([]limiter.Result, error) {
	if v.Kind != resp.KindArray || len(v.Elems) != 2+4*len(reqs) {
		return nil, fmt.Errorf("%w: %s", ErrBadReply, v)
	}
	nums := make([]int64, len(v.Elems))
	for i, e := range v.Elems {
		n, err := e.Int64()
		if err != nil {
			return nil, fmt.Errorf("%w: element %d: %v", ErrBadReply, i, err)
		}
		nums[i] = n
	}
	all := nums[0] == 1
	out := make([]limiter.Result, len(reqs))
	for i, r := range reqs {
		f := nums[2+4*i : 6+4*i]
		res := limiter.Result{
			Allowed:    f[0] == 1,
			Limit:      r.Limit,
			Remaining:  f[1],
			RetryAfter: time.Duration(f[2]) * time.Microsecond,
			ResetAfter: time.Duration(f[3]) * time.Microsecond,
		}
		if !all && res.Allowed {
			// Nothing was written; undo the tentative consumption so the
			// result describes the stored state.
			res.Remaining += r.Cost
			res.ResetAfter -= time.Duration(r.Cost) * r.Limit.Interval()
			if res.ResetAfter < 0 {
				res.ResetAfter = 0
			}
		}
		out[i] = res
	}
	return out, nil
}
