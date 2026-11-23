package limiter

import (
	"math/big"
	"time"
)

// The reference models below are deliberately naive: exact rational
// arithmetic, full request logs and brute-force searches. They share no
// code with the algorithms they check.

// refMeter is a continuous leaky bucket ("meter"): a level of cost that
// drains at one unit per interval and holds at most burst units. GCRA and
// the token bucket must make exactly its decisions.
type refMeter struct {
	interval int64
	burst    int64
	level    *big.Rat
	last     int64
}

func newRefMeter(l Limit) *refMeter {
	return &refMeter{interval: int64(l.Interval()), burst: l.EffectiveBurst(), level: new(big.Rat)}
}

func (m *refMeter) drain(now int64) {
	if now > m.last {
		d := big.NewRat(now-m.last, m.interval)
		m.level.Sub(m.level, d)
		if m.level.Sign() < 0 {
			m.level.SetInt64(0)
		}
	}
	m.last = now
}

func ratFloor(r *big.Rat) int64 {
	q := new(big.Int).Quo(r.Num(), r.Denom())
	if r.Sign() < 0 && new(big.Int).Mul(q, r.Denom()).Cmp(r.Num()) != 0 {
		q.Sub(q, big.NewInt(1))
	}
	return q.Int64()
}

func ratCeil(r *big.Rat) int64 {
	neg := new(big.Rat).Neg(r)
	return -ratFloor(neg)
}

func (m *refMeter) take(now, cost int64) Result {
	m.drain(now)
	b := new(big.Rat).SetInt64(m.burst)
	after := new(big.Rat).Add(m.level, new(big.Rat).SetInt64(cost))
	iv := new(big.Rat).SetInt64(m.interval)
	var r Result
	if after.Cmp(b) <= 0 {
		m.level = after
		r.Allowed = true
	} else {
		over := new(big.Rat).Sub(after, b)
		r.RetryAfter = time.Duration(ratCeil(over.Mul(over, iv)))
	}
	r.Remaining = max(0, ratFloor(new(big.Rat).Sub(b, m.level)))
	r.ResetAfter = time.Duration(ratCeil(new(big.Rat).Mul(m.level, iv)))
	return r
}

// refWindowLog keeps every admitted request and evaluates the two-window
// estimate by scanning the log.
type refWindowLog struct {
	window int64
	rate   int64
	log    []logEntry
}

type logEntry struct{ at, cost int64 }

func (w *refWindowLog) estimate(now int64, extra []logEntry) *big.Rat {
	ws := now - now%w.window
	var prev, curr int64
	for _, e := range append(append([]logEntry(nil), w.log...), extra...) {
		switch {
		case e.at >= ws && e.at <= now:
			curr += e.cost
		case e.at >= ws-w.window && e.at < ws:
			prev += e.cost
		}
	}
	est := big.NewRat(prev*(w.window-(now-ws)), w.window)
	return est.Add(est, new(big.Rat).SetInt64(curr))
}

func (w *refWindowLog) admits(now, cost int64) bool {
	est := w.estimate(now, nil)
	est.Add(est, new(big.Rat).SetInt64(cost))
	return est.Cmp(new(big.Rat).SetInt64(w.rate)) <= 0
}

func (w *refWindowLog) take(now, cost int64) Result {
	r := Result{}
	if w.admits(now, cost) {
		r.Allowed = true
		w.log = append(w.log, logEntry{now, cost})
	} else {
		for t := now + 1; ; t++ {
			if w.admits(t, cost) {
				r.RetryAfter = time.Duration(t - now)
				break
			}
		}
	}
	est := w.estimate(now, nil)
	r.Remaining = max(0, ratFloor(new(big.Rat).Sub(new(big.Rat).SetInt64(w.rate), est)))
	if est.Sign() > 0 {
		for t := now + 1; ; t++ {
			if w.estimate(t, nil).Sign() == 0 {
				r.ResetAfter = time.Duration(t - now)
				break
			}
		}
	}
	return r
}
