package policy

import "sync"

// Slot is a request for one unit of concurrency under Key, allowed while
// fewer than Max units are held.
type Slot struct {
	Key string
	Max int
}

// Concurrency limits in-flight work per key, for example at most 4
// concurrent bulk payouts per merchant. Rate limits bound how often work
// starts; a concurrency limit bounds how much of it runs at once, which is
// what protects downstream systems from slow requests piling up.
//
// It is process-local. A distributed equivalent needs leases with expiry
// so that a crashed instance does not hold slots forever; this package
// does not attempt that.
type Concurrency struct {
	mu sync.Mutex
	n  map[string]int
}

// NewConcurrency returns an empty Concurrency.
func NewConcurrency() *Concurrency {
	return &Concurrency{n: map[string]int{}}
}

// Acquire takes one unit of every slot, or none. It returns a release
// function (safe to call more than once) and -1, or nil and the index of
// the first slot that was full.
func (c *Concurrency) Acquire(slots ...Slot) (release func(), denied int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending := map[string]int{}
	for i, s := range slots {
		if c.n[s.Key]+pending[s.Key] >= s.Max {
			return nil, i
		}
		pending[s.Key]++
	}
	for k, v := range pending {
		c.n[k] += v
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			for k, v := range pending {
				if c.n[k] -= v; c.n[k] <= 0 {
					delete(c.n, k)
				}
			}
		})
	}, -1
}

// InFlight returns the units currently held under key.
func (c *Concurrency) InFlight(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[key]
}
