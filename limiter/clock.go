package limiter

import (
	"sync"
	"time"
)

// Clock supplies the current time to in-process stores.
type Clock interface {
	Now() time.Time
}

// SystemClock is the wall clock. time.Now carries a monotonic reading, but
// stores convert it to Unix nanoseconds, so a large backwards wall-clock
// step makes keys look younger than they are (more restrictive, never more
// permissive, for GCRA).
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// ManualClock is a Clock that only moves when told to. It is safe for
// concurrent use and intended for tests and simulations.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewManualClock returns a ManualClock set to t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{now: t} }

// Now returns the clock's current time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Set moves the clock to t.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}
