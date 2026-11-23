package limiter

import (
	"context"
	"errors"
)

// Store applies requests to keyed rate-limit state.
//
// Every method takes a batch. Take is all-or-nothing within a batch: either
// every request is allowed and consumed, or none is consumed and the
// results say which ones were denied. Whether the batch is also atomic with
// respect to concurrent callers depends on the store and is documented on
// each implementation: [MemoryStore] is, the Redis store is on a single
// node or when all keys share a cluster hash slot.
//
// When a batch is denied, results of requests that would have passed on
// their own report Allowed true and the current, unconsumed Remaining, so
// callers can tell which limit denied.
//
// Methods return an error only for invalid requests (see [ErrInvalidLimit],
// [ErrNegativeCost], [ErrCostExceedsBurst]) or when the store is
// unavailable; a denial is not an error.
type Store interface {
	// Take consumes every request if all of them are allowed.
	Take(ctx context.Context, reqs ...Request) ([]Result, error)
	// Peek reports what Take would return, without consuming anything.
	Peek(ctx context.Context, reqs ...Request) ([]Result, error)
	// Refund gives back the cost of requests that a previous Take consumed.
	// It is best effort and never grants more than the idle state.
	Refund(ctx context.Context, reqs ...Request) error
}

// Allow is a convenience for a single request against s.
func Allow(ctx context.Context, s Store, key string, l Limit, cost int64) (Result, error) {
	res, err := s.Take(ctx, Request{Key: key, Limit: l, Cost: cost})
	if err != nil {
		return Result{}, err
	}
	return res[0], nil
}

// IsRequestError reports whether err describes an invalid request rather
// than an unavailable store. Request errors are never worth retrying or
// falling back on.
func IsRequestError(err error) bool {
	return errors.Is(err, ErrInvalidLimit) || errors.Is(err, ErrNegativeCost) || errors.Is(err, ErrCostExceedsBurst)
}

// Batch is a group of requests bound for one store.
type Batch struct {
	Store    Store
	Requests []Request
}

// TakeAll applies several batches, possibly on different stores, with
// all-or-nothing semantics. It returns one result slice per batch and
// whether everything was allowed.
//
// A single batch is passed straight to its store. With more than one,
// TakeAll runs a check-then-commit protocol:
//
//  1. Check: Peek every batch. If any request would be denied, return
//     immediately; nothing has been consumed anywhere.
//  2. Commit: Take each batch in order. If a Take is denied (another caller
//     consumed the capacity between check and commit) or fails, Refund the
//     batches already committed and report the denial or error.
//
// The protocol is not linearizable across stores. Its failure modes are
// one-sided: a concurrent caller may be denied by capacity that is later
// refunded, and a crash or store error between commit and refund leaves
// capacity consumed until it decays. It never admits more than any limit
// allows, because every commit is an ordinary, individually atomic Take.
func TakeAll(ctx context.Context, batches ...Batch) ([][]Result, bool, error) {
	out := make([][]Result, len(batches))
	if len(batches) == 1 {
		res, err := batches[0].Store.Take(ctx, batches[0].Requests...)
		if err != nil {
			return nil, false, err
		}
		out[0] = res
		return out, AllAllowed(res), nil
	}

	allowed := true
	for i, b := range batches {
		res, err := b.Store.Peek(ctx, b.Requests...)
		if err != nil {
			return nil, false, err
		}
		out[i] = res
		allowed = allowed && AllAllowed(res)
	}
	if !allowed {
		// Batches that passed their own check report the remaining after a
		// consumption that is not going to happen; undo that. For every
		// algorithm here, consuming cost c lowers Remaining by exactly c.
		for i, b := range batches {
			if AllAllowed(out[i]) {
				for j := range out[i] {
					out[i][j].Remaining += b.Requests[j].Cost
				}
			}
		}
		return out, false, nil
	}

	for i, b := range batches {
		res, err := b.Store.Take(ctx, b.Requests...)
		if err == nil {
			out[i] = res
			if AllAllowed(res) {
				continue
			}
		}
		// Roll back what was committed. A refund is idempotent enough to be
		// worth attempting even if ctx is done, so use a detached context.
		var rerr error
		for j := i - 1; j >= 0; j-- {
			rerr = errors.Join(rerr, batches[j].Store.Refund(context.WithoutCancel(ctx), batches[j].Requests...))
		}
		if err != nil {
			return nil, false, errors.Join(err, rerr)
		}
		// A failed refund on a denial is deliberately not an error: the
		// decision is still a denial, and reporting an error would let a
		// fail-open caller admit the request. The capacity decays on its own.
		return out, false, nil
	}
	return out, true, nil
}
