package limiter

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryOptions configures a [MemoryStore].
type MemoryOptions struct {
	// Algorithm decides requests. Defaults to GCRA.
	Algorithm Algorithm
	// Shards is the number of independently locked shards, rounded up to a
	// power of two. Defaults to 64.
	Shards int
	// MaxKeys bounds the number of live keys across all shards. When a shard
	// is full, inserting a new key first drops expired keys found in a
	// random sample and, failing that, evicts the sampled key closest to
	// expiry, the one whose loss forgives the least. Defaults to 1<<20; negative means unbounded.
	MaxKeys int
	// SweepInterval is how often a background goroutine drops expired keys.
	// Expired keys are also treated as idle and overwritten lazily, so the
	// sweep only bounds memory. Defaults to one minute; negative disables it.
	SweepInterval time.Duration
	// Clock defaults to SystemClock.
	Clock Clock
}

// MemoryStats is a snapshot of a MemoryStore's bookkeeping.
type MemoryStats struct {
	Keys      int
	Evictions uint64
	Expired   uint64
}

// MemoryStore is an in-process [Store] for a single instance, or as the
// local fallback of a distributed one.
//
// Each key costs its string plus a State and an expiry (32 bytes). Keys are
// dropped once their state has decayed to idle, so memory tracks the number
// of recently active keys, bounded by MaxKeys.
//
// Take is atomic: the shards of every key in the batch are locked in index
// order for the duration of the decision, so concurrent batches are
// serializable and a GCRA key admits exactly what the algorithm allows,
// however many goroutines race for it.
type MemoryStore struct {
	alg      Algorithm
	clock    Clock
	shards   []shard
	mask     uint64
	perShard int

	evictions atomic.Uint64
	expired   atomic.Uint64

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

type entry struct {
	state    State
	expireAt int64
}

type shard struct {
	mu sync.Mutex
	m  map[string]entry
	_  [40]byte // keep hot mutexes on separate cache lines
}

// NewMemoryStore returns a MemoryStore. Call Close to stop its sweeper.
func NewMemoryStore(opts MemoryOptions) *MemoryStore {
	if opts.Algorithm == nil {
		opts.Algorithm = GCRA
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	n := 64
	if opts.Shards > 0 {
		n = 1
		for n < opts.Shards {
			n <<= 1
		}
	}
	if opts.MaxKeys == 0 {
		opts.MaxKeys = 1 << 20
	}
	perShard := -1
	if opts.MaxKeys > 0 {
		perShard = max(1, (opts.MaxKeys+n-1)/n)
	}
	s := &MemoryStore{
		alg:      opts.Algorithm,
		clock:    opts.Clock,
		shards:   make([]shard, n),
		mask:     uint64(n - 1),
		perShard: perShard,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[string]entry)
	}
	if opts.SweepInterval == 0 {
		opts.SweepInterval = time.Minute
	}
	if opts.SweepInterval > 0 {
		go s.sweeper(opts.SweepInterval)
	} else {
		close(s.done)
	}
	return s
}

// Algorithm returns the store's algorithm.
func (s *MemoryStore) Algorithm() Algorithm { return s.alg }

// Close stops the background sweeper. The store remains usable.
func (s *MemoryStore) Close() error {
	s.once.Do(func() { close(s.stop) })
	<-s.done
	return nil
}

// Stats returns the current key count and eviction counters.
func (s *MemoryStore) Stats() MemoryStats {
	st := MemoryStats{Evictions: s.evictions.Load(), Expired: s.expired.Load()}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		st.Keys += len(sh.m)
		sh.mu.Unlock()
	}
	return st
}

// Sweep drops every expired key now and returns how many it dropped.
func (s *MemoryStore) Sweep() int {
	now := s.clock.Now().UnixNano()
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, e := range sh.m {
			if e.expireAt <= now {
				delete(sh.m, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	s.expired.Add(uint64(n))
	return n
}

func (s *MemoryStore) sweeper(every time.Duration) {
	defer close(s.done)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.Sweep()
		}
	}
}

// Take implements [Store].
func (s *MemoryStore) Take(ctx context.Context, reqs ...Request) ([]Result, error) {
	return s.apply(ctx, reqs, opTake)
}

// Peek implements [Store].
func (s *MemoryStore) Peek(ctx context.Context, reqs ...Request) ([]Result, error) {
	return s.apply(ctx, reqs, opPeek)
}

// Refund implements [Store].
func (s *MemoryStore) Refund(ctx context.Context, reqs ...Request) error {
	_, err := s.apply(ctx, reqs, opRefund)
	return err
}

type op int

const (
	opTake op = iota
	opPeek
	opRefund
)

func (s *MemoryStore) validate(reqs []Request) error {
	for _, r := range reqs {
		if err := r.validate(s.alg.Capacity); err != nil {
			return fmt.Errorf("key %q: %w", r.Key, err)
		}
	}
	return nil
}

func (s *MemoryStore) shardOf(key string) int {
	// FNV-1a, inlined to avoid an allocation per call.
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return int(h & s.mask)
}

func (s *MemoryStore) apply(ctx context.Context, reqs []Request, o op) ([]Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.validate(reqs); err != nil {
		return nil, err
	}
	if len(reqs) == 1 {
		return s.applyOne(reqs[0], o), nil
	}
	return s.applyMany(reqs, o), nil
}

// applyOne is the fast path: one key, one shard, no allocation beyond the
// result slice.
func (s *MemoryStore) applyOne(r Request, o op) []Result {
	sh := &s.shards[s.shardOf(r.Key)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	now := s.clock.Now().UnixNano()
	e, ok := sh.m[r.Key]
	if ok && e.expireAt <= now {
		e = entry{}
	}
	if o == opRefund {
		next, exp := s.alg.Refund(e.state, now, r.Limit, r.Cost)
		s.store(sh, r.Key, ok, next, exp, now)
		return nil
	}
	next, res, exp := s.alg.Apply(e.state, now, r.Limit, r.Cost)
	if o == opTake && res.Allowed && (next != e.state || exp != e.expireAt) {
		s.store(sh, r.Key, ok, next, exp, now)
	}
	return []Result{res}
}

func (s *MemoryStore) applyMany(reqs []Request, o op) []Result {
	idx := make([]int, len(reqs))
	for i, r := range reqs {
		idx[i] = s.shardOf(r.Key)
	}
	locked := append([]int(nil), idx...)
	sort.Ints(locked)
	prev := -1
	for _, i := range locked {
		if i != prev {
			s.shards[i].mu.Lock()
			prev = i
		}
	}
	defer func() {
		prev := -1
		for _, i := range locked {
			if i != prev {
				s.shards[i].mu.Unlock()
				prev = i
			}
		}
	}()

	now := s.clock.Now().UnixNano()
	type pending struct {
		key    string
		shard  int
		state  State
		exp    int64
		exists bool
		dirty  bool
	}
	// A batch may name the same key more than once (two rules that happen
	// to resolve to one key); later requests see earlier ones' effects.
	work := make([]pending, 0, len(reqs))
	slot := func(i int) *pending {
		for j := range work {
			if work[j].key == reqs[i].Key {
				return &work[j]
			}
		}
		e, ok := s.shards[idx[i]].m[reqs[i].Key]
		if ok && e.expireAt <= now {
			e = entry{}
		}
		work = append(work, pending{key: reqs[i].Key, shard: idx[i], state: e.state, exp: e.expireAt, exists: ok})
		return &work[len(work)-1]
	}

	var results []Result
	if o != opRefund {
		results = make([]Result, len(reqs))
	}
	allowed := true
	for i, r := range reqs {
		p := slot(i)
		if o == opRefund {
			p.state, p.exp = s.alg.Refund(p.state, now, r.Limit, r.Cost)
			p.dirty = true
			continue
		}
		next, res, exp := s.alg.Apply(p.state, now, r.Limit, r.Cost)
		results[i] = res
		if res.Allowed {
			p.state, p.exp, p.dirty = next, exp, true
		} else {
			allowed = false
		}
	}
	if o == opPeek || (o == opTake && !allowed) {
		if !allowed {
			// Results computed after a denial in the same batch assumed the
			// earlier requests were consumed; recompute them against the
			// stored state so a denial reports nothing as consumed.
			s.recomputeDenied(reqs, idx, results, now)
		}
		return results
	}
	for _, p := range work {
		if p.dirty {
			s.store(&s.shards[p.shard], p.key, p.exists, p.state, p.exp, now)
		}
	}
	return results
}

// recomputeDenied evaluates every request of a denied batch against the
// stored state independently. Shards are already locked.
func (s *MemoryStore) recomputeDenied(reqs []Request, idx []int, results []Result, now int64) {
	for i, r := range reqs {
		e, ok := s.shards[idx[i]].m[r.Key]
		if ok && e.expireAt <= now {
			e = entry{}
		}
		_, res, _ := s.alg.Apply(e.state, now, r.Limit, r.Cost)
		if res.Allowed {
			// Nothing was consumed: report the current state, not the
			// state after a consumption that did not happen.
			_, cur, _ := s.alg.Apply(e.state, now, r.Limit, 0)
			res.Remaining, res.ResetAfter = cur.Remaining, cur.ResetAfter
		}
		if !results[i].Allowed && res.Allowed {
			// Keep the denial as decided in sequence: a duplicate key may be
			// allowed alone but not together with its twin.
			res.Allowed = false
			res.RetryAfter = results[i].RetryAfter
		}
		results[i] = res
	}
}

// store writes or deletes a key. Shard must be locked.
func (s *MemoryStore) store(sh *shard, key string, exists bool, st State, exp, now int64) {
	if st == (State{}) || exp <= now {
		if exists {
			delete(sh.m, key)
		}
		return
	}
	if !exists && s.perShard > 0 && len(sh.m) >= s.perShard {
		s.makeRoom(sh, now)
	}
	sh.m[key] = entry{state: st, expireAt: exp}
}

// makeRoom frees at least one slot in a full shard. Map iteration order is
// randomized, so the entries visited are a random sample: every expired key
// among the first scanLimit is dropped, and only if none was found is the
// live key closest to expiry among the first evictSample evicted.
func (s *MemoryStore) makeRoom(sh *shard, now int64) {
	const scanLimit, evictSample = 64, 8
	var victim string
	var victimExp int64
	seen, dropped := 0, 0
	for k, e := range sh.m {
		if e.expireAt <= now {
			delete(sh.m, k)
			dropped++
		} else if seen < evictSample {
			if seen == 0 || e.expireAt < victimExp {
				victim, victimExp = k, e.expireAt
			}
			seen++
		}
		if seen+dropped >= scanLimit {
			break
		}
	}
	if dropped > 0 {
		s.expired.Add(uint64(dropped))
		return
	}
	delete(sh.m, victim)
	s.evictions.Add(1)
}
