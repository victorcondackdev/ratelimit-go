package resp

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by a closed Client.
var ErrClosed = errors.New("resp: client closed")

// Options configures a Client.
type Options struct {
	// Addr is the host:port of the server. Defaults to localhost:6379.
	Addr string
	// Username and Password authenticate each new connection (AUTH, or
	// HELLO ... AUTH for RESP3). Username is optional (ACL users).
	Username string
	Password string
	// DB is selected on each new connection when non-zero.
	DB int
	// Protocol is 2 (default) or 3. With 3 the client sends HELLO 3 and
	// understands every RESP3 type; RESP2 replies are understood either way.
	Protocol int
	// ClientName is set with CLIENT SETNAME (or HELLO SETNAME) when not
	// empty.
	ClientName string

	// DialTimeout bounds connecting and the handshake. Defaults to 5s.
	DialTimeout time.Duration
	// ReadTimeout and WriteTimeout bound each round trip unless the context
	// has an earlier deadline. Default to 3s.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// PoolSize is the maximum number of connections per server address,
	// busy or idle. Callers wait for a free connection, bounded by their
	// context. Defaults to 10 per GOMAXPROCS.
	PoolSize int
	// MaxIdle is the number of idle connections kept per address. Defaults
	// to PoolSize.
	MaxIdle int
	// IdleTimeout closes connections idle for longer. Defaults to 5m;
	// negative disables.
	IdleTimeout time.Duration

	// TLSConfig enables TLS when non-nil.
	TLSConfig *tls.Config
	// Dialer overrides how connections are made, for tests or proxies.
	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)

	// MaxRedirects bounds MOVED/ASK redirects followed by keyed commands.
	// Defaults to 3; negative disables following.
	MaxRedirects int
}

func (o *Options) setDefaults() {
	if o.Addr == "" {
		o.Addr = "localhost:6379"
	}
	if o.Protocol == 0 {
		o.Protocol = 2
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 3 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 3 * time.Second
	}
	if o.PoolSize <= 0 {
		o.PoolSize = 10 * runtime.GOMAXPROCS(0)
	}
	if o.MaxIdle <= 0 || o.MaxIdle > o.PoolSize {
		o.MaxIdle = o.PoolSize
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = 5 * time.Minute
	}
	if o.MaxRedirects == 0 {
		o.MaxRedirects = 3
	}
	if o.Dialer == nil {
		d := &net.Dialer{Timeout: o.DialTimeout, KeepAlive: 30 * time.Second}
		o.Dialer = d.DialContext
	}
}

// ParseURL parses redis://[user:password@]host[:port][/db][?protocol=3]
// and rediss:// (TLS) URLs. Query parameters dial_timeout, read_timeout,
// write_timeout (Go durations), pool_size and client_name are also
// recognized.
func ParseURL(raw string) (Options, error) {
	var o Options
	u, err := url.Parse(raw)
	if err != nil {
		return o, fmt.Errorf("resp: parse url: %w", err)
	}
	switch u.Scheme {
	case "redis":
	case "rediss":
		o.TLSConfig = &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
	default:
		return o, fmt.Errorf("resp: unsupported url scheme %q", u.Scheme)
	}
	host, port := u.Hostname(), u.Port()
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "6379"
	}
	o.Addr = net.JoinHostPort(host, port)
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			o.Username, o.Password = u.User.Username(), pw
		} else {
			// redis://secret@host is a password-only URL by convention.
			o.Password = u.User.Username()
		}
	}
	if p := strings.Trim(u.Path, "/"); p != "" {
		if o.DB, err = strconv.Atoi(p); err != nil || o.DB < 0 {
			return o, fmt.Errorf("resp: bad database %q in url", p)
		}
	}
	q := u.Query()
	ints := map[string]*int{"protocol": &o.Protocol, "pool_size": &o.PoolSize, "db": &o.DB}
	for name, dst := range ints {
		if v := q.Get(name); v != "" {
			if *dst, err = strconv.Atoi(v); err != nil {
				return o, fmt.Errorf("resp: bad %s %q in url", name, v)
			}
		}
	}
	durations := map[string]*time.Duration{"dial_timeout": &o.DialTimeout, "read_timeout": &o.ReadTimeout, "write_timeout": &o.WriteTimeout}
	for name, dst := range durations {
		if v := q.Get(name); v != "" {
			if *dst, err = time.ParseDuration(v); err != nil {
				return o, fmt.Errorf("resp: bad %s %q in url", name, v)
			}
		}
	}
	o.ClientName = q.Get("client_name")
	if o.Protocol != 0 && o.Protocol != 2 && o.Protocol != 3 {
		return o, fmt.Errorf("resp: unsupported protocol %d", o.Protocol)
	}
	return o, nil
}

// Client is a pooled connection to a Redis server. It is safe for concurrent
// use.
type Client struct {
	opts   Options
	mu     sync.Mutex
	pools  map[string]*pool
	slots  map[int]string // MOVED cache: slot -> address
	closed atomic.Bool

	dials    atomic.Uint64
	timeouts atomic.Uint64
}

// NewClient returns a Client. No connection is made until the first
// command.
func NewClient(opts Options) *Client {
	opts.setDefaults()
	return &Client{opts: opts, pools: map[string]*pool{}, slots: map[int]string{}}
}

// Options returns the client's options with defaults applied.
func (c *Client) Options() Options { return c.opts }

// PoolStats reports connection usage summed over every address.
type PoolStats struct {
	Idle     int
	Active   int
	Dials    uint64
	Timeouts uint64
}

// Stats returns the pool statistics.
func (c *Client) Stats() PoolStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := PoolStats{Dials: c.dials.Load(), Timeouts: c.timeouts.Load()}
	for _, p := range c.pools {
		p.mu.Lock()
		st.Idle += len(p.idle)
		p.mu.Unlock()
		st.Active += len(p.sem)
	}
	return st
}

// Close closes idle connections and makes further commands fail. Busy
// connections are closed when they are returned.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pools {
		p.close()
	}
	return nil
}

func (c *Client) pool(addr string) *pool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pools[addr]
	if !ok {
		p = &pool{client: c, addr: addr, sem: make(chan struct{}, c.opts.PoolSize)}
		c.pools[addr] = p
	}
	return p
}

// Do sends one command and returns its reply. An error reply is returned as
// an *Error, with the connection kept for reuse.
func (c *Client) Do(ctx context.Context, args ...string) (Value, error) {
	return c.doAt(ctx, c.opts.Addr, args)
}

// DoKeyed is Do for a command whose routing key is key: it goes to the
// address that last answered MOVED for key's slot, and follows MOVED and
// ASK redirects.
func (c *Client) DoKeyed(ctx context.Context, key string, args ...string) (Value, error) {
	slot := Slot(key)
	c.mu.Lock()
	addr, ok := c.slots[slot]
	c.mu.Unlock()
	if !ok {
		addr = c.opts.Addr
	}
	asking := false
	for redirects := 0; ; redirects++ {
		var v Value
		var err error
		if asking {
			var vs []Value
			vs, err = c.pipelineAt(ctx, addr, [][]string{{"ASKING"}, args})
			if err == nil {
				v, err = vs[1], vs[1].Err()
			}
		} else {
			v, err = c.doAt(ctx, addr, args)
		}
		var rerr *Error
		if err == nil || !errors.As(err, &rerr) || redirects >= c.opts.MaxRedirects || c.opts.MaxRedirects < 0 {
			return v, err
		}
		code := rerr.Code()
		if code != "MOVED" && code != "ASK" {
			return v, err
		}
		// "MOVED 3999 127.0.0.1:6381"
		f := strings.Fields(rerr.Msg)
		if len(f) != 3 {
			return v, err
		}
		addr, asking = f[2], code == "ASK"
		if code == "MOVED" {
			if s, perr := strconv.Atoi(f[1]); perr == nil {
				c.mu.Lock()
				c.slots[s] = addr
				c.mu.Unlock()
			}
		}
	}
}

func (c *Client) doAt(ctx context.Context, addr string, args []string) (Value, error) {
	vs, err := c.pipelineAt(ctx, addr, [][]string{args})
	if err != nil {
		return Value{}, err
	}
	return vs[0], vs[0].Err()
}

// Pipeline sends every command in one write and reads all replies.
// Error replies are returned as values of kind KindError rather than as an
// error; the error result reports only transport and protocol failures.
func (c *Client) Pipeline(ctx context.Context, cmds ...[]string) ([]Value, error) {
	return c.pipelineAt(ctx, c.opts.Addr, cmds)
}

func (c *Client) pipelineAt(ctx context.Context, addr string, cmds [][]string) ([]Value, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if len(cmds) == 0 {
		return nil, nil
	}
	p := c.pool(addr)
	cn, err := p.get(ctx)
	if err != nil {
		return nil, err
	}
	vs, err := cn.roundTrip(ctx, cmds, c.opts.ReadTimeout, c.opts.WriteTimeout)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			c.timeouts.Add(1)
		}
	}
	p.put(cn)
	return vs, err
}

// Script is a Lua script identified by its SHA-1.
type Script struct {
	src string
	sha string
}

// NewScript returns a Script for src.
func NewScript(src string) *Script {
	sum := sha1.Sum([]byte(src))
	return &Script{src: src, sha: hex.EncodeToString(sum[:])}
}

// SHA returns the script's hex SHA-1, as used by EVALSHA.
func (s *Script) SHA() string { return s.sha }

// Source returns the script's source.
func (s *Script) Source() string { return s.src }

// Run executes the script with EVALSHA and, if the server does not have it
// cached (NOSCRIPT, after a restart, failover or SCRIPT FLUSH), with EVAL,
// which also caches it for next time. The command is routed by the first
// key.
func (c *Client) Run(ctx context.Context, s *Script, keys []string, args ...string) (Value, error) {
	route := ""
	if len(keys) > 0 {
		route = keys[0]
	}
	cmd := make([]string, 0, 3+len(keys)+len(args))
	cmd = append(cmd, "EVALSHA", s.sha, strconv.Itoa(len(keys)))
	cmd = append(cmd, keys...)
	cmd = append(cmd, args...)
	v, err := c.DoKeyed(ctx, route, cmd...)
	if !IsNoScript(err) {
		return v, err
	}
	cmd[0], cmd[1] = "EVAL", s.src
	return c.DoKeyed(ctx, route, cmd...)
}

// Load caches the script on the default server with SCRIPT LOAD.
func (c *Client) Load(ctx context.Context, s *Script) error {
	v, err := c.Do(ctx, "SCRIPT", "LOAD", s.src)
	if err != nil {
		return err
	}
	if got, _ := v.Text(); got != s.sha {
		return fmt.Errorf("resp: SCRIPT LOAD returned %s, want %s", v, s.sha)
	}
	return nil
}

type pool struct {
	client *Client
	addr   string
	sem    chan struct{} // one token per open connection
	mu     sync.Mutex
	idle   []*conn
	closed bool
}

func (p *pool) get(ctx context.Context) (*conn, error) {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	opts := &p.client.opts
	for {
		p.mu.Lock()
		n := len(p.idle)
		if n == 0 {
			p.mu.Unlock()
			break
		}
		cn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		if opts.IdleTimeout > 0 && time.Since(cn.usedAt) > opts.IdleTimeout {
			cn.nc.Close()
			continue
		}
		return cn, nil
	}
	cn, err := p.client.dial(ctx, p.addr)
	if err != nil {
		<-p.sem
		return nil, err
	}
	return cn, nil
}

func (p *pool) put(cn *conn) {
	defer func() { <-p.sem }()
	p.mu.Lock()
	defer p.mu.Unlock()
	if cn.broken || p.closed || len(p.idle) >= p.client.opts.MaxIdle {
		cn.nc.Close()
		return
	}
	cn.usedAt = time.Now()
	p.idle = append(p.idle, cn)
}

func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, cn := range p.idle {
		cn.nc.Close()
	}
	p.idle = nil
}

type conn struct {
	nc     net.Conn
	rd     *Reader
	wbuf   []byte
	usedAt time.Time
	broken bool
}

func (c *Client) dial(ctx context.Context, addr string) (*conn, error) {
	ctx, cancel := context.WithTimeout(ctx, c.opts.DialTimeout)
	defer cancel()
	nc, err := c.opts.Dialer(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("resp: dial %s: %w", addr, err)
	}
	if c.opts.TLSConfig != nil {
		tc := tls.Client(nc, c.opts.TLSConfig)
		if err := tc.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, fmt.Errorf("resp: tls handshake with %s: %w", addr, err)
		}
		nc = tc
	}
	c.dials.Add(1)
	cn := &conn{nc: nc, rd: NewReader(bufio.NewReaderSize(nc, 16<<10)), usedAt: time.Now()}
	if err := c.handshake(ctx, cn); err != nil {
		nc.Close()
		return nil, fmt.Errorf("resp: handshake with %s: %w", addr, err)
	}
	return cn, nil
}

func (c *Client) handshake(ctx context.Context, cn *conn) error {
	var cmds [][]string
	o := &c.opts
	if o.Protocol == 3 {
		hello := []string{"HELLO", "3"}
		if o.Password != "" {
			user := o.Username
			if user == "" {
				user = "default"
			}
			hello = append(hello, "AUTH", user, o.Password)
		}
		if o.ClientName != "" {
			hello = append(hello, "SETNAME", o.ClientName)
		}
		cmds = append(cmds, hello)
	} else {
		if o.Password != "" {
			if o.Username != "" {
				cmds = append(cmds, []string{"AUTH", o.Username, o.Password})
			} else {
				cmds = append(cmds, []string{"AUTH", o.Password})
			}
		}
		if o.ClientName != "" {
			cmds = append(cmds, []string{"CLIENT", "SETNAME", o.ClientName})
		}
	}
	if o.DB != 0 {
		cmds = append(cmds, []string{"SELECT", strconv.Itoa(o.DB)})
	}
	if len(cmds) == 0 {
		return nil
	}
	vs, err := cn.roundTrip(ctx, cmds, o.DialTimeout, o.DialTimeout)
	if err != nil {
		return err
	}
	for i, v := range vs {
		if err := v.Err(); err != nil {
			return fmt.Errorf("%s: %w", cmds[i][0], err)
		}
	}
	return nil
}

// roundTrip writes cmds and reads one reply per command. Any transport or
// protocol error marks the connection broken, since its stream position is
// unknown. RESP3 push messages interleaved with replies are skipped.
func (cn *conn) roundTrip(ctx context.Context, cmds [][]string, readTimeout, writeTimeout time.Duration) ([]Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() {
		// Unblock a pending read or write; the deadline error is mapped
		// back to the context's error below.
		cn.nc.SetDeadline(time.Unix(1, 0))
	})
	defer func() {
		if !stop() {
			// The cancellation hook ran (or is running) and poisoned the
			// deadline; never hand this connection to another caller.
			cn.broken = true
		}
	}()

	cn.wbuf = cn.wbuf[:0]
	for _, cmd := range cmds {
		cn.wbuf = AppendCommand(cn.wbuf, cmd...)
	}
	deadline := func(d time.Duration) time.Time {
		t := time.Now().Add(d)
		if cd, ok := ctx.Deadline(); ok && cd.Before(t) {
			return cd
		}
		return t
	}
	fail := func(err error) ([]Value, error) {
		cn.broken = true
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		// A read that hit the context's deadline can return before the
		// context itself reports expiry; report it the same way.
		if cd, ok := ctx.Deadline(); ok && !time.Now().Before(cd) {
			return nil, context.DeadlineExceeded
		}
		return nil, err
	}
	if err := cn.nc.SetWriteDeadline(deadline(writeTimeout)); err != nil {
		return fail(err)
	}
	if _, err := cn.nc.Write(cn.wbuf); err != nil {
		return fail(err)
	}
	if err := cn.nc.SetReadDeadline(deadline(readTimeout)); err != nil {
		return fail(err)
	}
	out := make([]Value, 0, len(cmds))
	for len(out) < len(cmds) {
		v, err := cn.rd.ReadValue()
		if err != nil {
			return fail(err)
		}
		if v.Kind == KindPush {
			continue
		}
		out = append(out, v)
	}
	if cap(cn.wbuf) > 64<<10 {
		cn.wbuf = nil // do not pin a large buffer in the pool
	}
	return out, nil
}
