// Package redistest is an in-process fake Redis server for tests.
//
// It speaks RESP2 and RESP3 over TCP and implements the commands the resp
// client and the Redis store use: PING, ECHO, HELLO, AUTH, SELECT, CLIENT,
// GET, SET (EX/PX/NX/XX), DEL, EXISTS, PTTL, TIME, DBSIZE, FLUSHALL,
// SCRIPT LOAD/EXISTS/FLUSH, EVAL, EVALSHA, ASKING and QUIT. It cannot run
// Lua: EVAL and EVALSHA execute a Go emulation of the GCRA script,
// recognized by its marker line, built on [limiter.GCRAStep].
//
// Faults can be injected: refusing connections (SetDown), latency
// (SetDelay), MOVED/ASK redirects (Redirect), CROSSSLOT checks (Cluster)
// and arbitrary error replies (FailNext).
package redistest

import (
	"bufio"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

// GCRAMarker is the first line of the script the server knows how to
// emulate.
const GCRAMarker = "-- ratelimit-go gcra v1"

// Options configures a Server.
type Options struct {
	// Username and Password, when Password is set, are required via AUTH or
	// HELLO ... AUTH before any other command.
	Username string
	Password string
	// Cluster makes multi-key commands fail with CROSSSLOT when their keys
	// hash to different slots, as a cluster node would.
	Cluster bool
	// Clock defaults to time.Now. It drives TIME and key expiry.
	Clock func() time.Time
	// TLS, if set, makes the server accept only TLS connections.
	TLS *tls.Config
}

type item struct {
	val      string
	expireAt time.Time
}

type redirect struct {
	addr string
	ask  bool
}

// Server is a fake Redis server.
type Server struct {
	opts Options
	ln   net.Listener

	mu        sync.Mutex
	dbs       map[int]map[string]item
	scripts   map[string]string
	counts    map[string]int
	redirects map[int]redirect
	failNext  map[string]string
	conns     map[net.Conn]struct{}
	down      bool
	delay     time.Duration
	clock     func() time.Time

	wg sync.WaitGroup
}

// Start listens on a random loopback port and serves until Close.
func Start(opts Options) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	if opts.TLS != nil {
		ln = tls.NewListener(ln, opts.TLS)
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	s := &Server{
		opts:      opts,
		ln:        ln,
		dbs:       map[int]map[string]item{},
		scripts:   map[string]string{},
		counts:    map[string]int{},
		redirects: map[int]redirect{},
		failNext:  map[string]string{},
		conns:     map[net.Conn]struct{}{},
		clock:     opts.Clock,
	}
	s.wg.Add(1)
	go s.accept()
	return s, nil
}

// Addr returns the host:port the server listens on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// URL returns a redis:// URL for the server.
func (s *Server) URL() string { return "redis://" + s.Addr() }

// Close stops the server and closes every connection.
func (s *Server) Close() error {
	err := s.ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

// SetDown, when true, closes every open connection and every new one as
// soon as it is accepted, simulating an unreachable server.
func (s *Server) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
	if down {
		for c := range s.conns {
			c.Close()
		}
	}
}

// SetDelay delays every reply by d.
func (s *Server) SetDelay(d time.Duration) {
	s.mu.Lock()
	s.delay = d
	s.mu.Unlock()
}

// SetClock replaces the server clock.
func (s *Server) SetClock(clock func() time.Time) {
	s.mu.Lock()
	s.clock = clock
	s.mu.Unlock()
}

// Redirect answers commands whose first key hashes to slot with MOVED (or
// ASK) pointing at addr. An empty addr removes the redirect.
func (s *Server) Redirect(slot int, addr string, ask bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if addr == "" {
		delete(s.redirects, slot)
		return
	}
	s.redirects[slot] = redirect{addr: addr, ask: ask}
}

// FailNext makes the next invocation of cmd reply with the error msg.
func (s *Server) FailNext(cmd, msg string) {
	s.mu.Lock()
	s.failNext[strings.ToUpper(cmd)] = msg
	s.mu.Unlock()
}

// FlushScripts empties the script cache, as SCRIPT FLUSH or a restart would.
func (s *Server) FlushScripts() {
	s.mu.Lock()
	s.scripts = map[string]string{}
	s.mu.Unlock()
}

// Count returns how many times cmd was received.
func (s *Server) Count(cmd string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[strings.ToUpper(cmd)]
}

// Get returns a key's value in database 0.
func (s *Server) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.lookup(0, key)
	return it.val, ok
}

// TTL returns a key's remaining time to live in database 0, or -1.
func (s *Server) TTL(key string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.lookup(0, key)
	if !ok || it.expireAt.IsZero() {
		return -1
	}
	return it.expireAt.Sub(s.clock())
}

// Keys returns the number of live keys in database 0.
func (s *Server) Keys() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k := range s.dbs[0] {
		if _, ok := s.lookup(0, k); ok {
			n++
		}
	}
	return n
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.down {
			s.mu.Unlock()
			c.Close()
			continue
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(c)
	}
}

type session struct {
	proto  int
	db     int
	authed bool
	asking bool
}

func (s *Server) serve(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		c.Close()
	}()
	rd := resp.NewReader(bufio.NewReader(c))
	w := bufio.NewWriter(c)
	sess := &session{proto: 2, authed: s.opts.Password == ""}
	var out []byte
	for {
		v, err := rd.ReadValue()
		if err != nil {
			return
		}
		args, ok := commandArgs(v)
		if !ok {
			out = s.encode(out[:0], sess, errorf("ERR Protocol error: expected array of bulk strings"))
			w.Write(out)
			w.Flush()
			return
		}
		reply, quit := s.handle(sess, args)
		s.mu.Lock()
		delay := s.delay
		s.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		out = s.encode(out[:0], sess, reply)
		if _, err := w.Write(out); err != nil {
			return
		}
		// Flush only when the client has nothing more pipelined, so a
		// pipeline is answered in as few writes as it was sent.
		if rd.Buffered() == 0 || quit {
			if err := w.Flush(); err != nil {
				return
			}
		}
		if quit {
			return
		}
	}
}

func commandArgs(v resp.Value) ([]string, bool) {
	if v.Kind != resp.KindArray || len(v.Elems) == 0 {
		return nil, false
	}
	args := make([]string, len(v.Elems))
	for i, e := range v.Elems {
		if e.Kind != resp.KindBulk {
			return nil, false
		}
		args[i] = e.Str
	}
	return args, true
}

// encode writes v in the session's protocol version, downgrading RESP3-only
// types for RESP2 clients the way Redis does.
func (s *Server) encode(dst []byte, sess *session, v resp.Value) []byte {
	if sess.proto == 3 {
		return resp.AppendValue(dst, v)
	}
	return encode2(dst, v)
}

func encode2(dst []byte, v resp.Value) []byte {
	switch v.Kind {
	case resp.KindNull:
		return append(dst, "$-1\r\n"...)
	case resp.KindBool:
		return resp.AppendValue(dst, integer(int64(boolInt(v.Bool))))
	case resp.KindDouble:
		return resp.AppendValue(dst, bulk(strconv.FormatFloat(v.Float, 'g', -1, 64)))
	case resp.KindBigNumber, resp.KindVerbatim:
		return resp.AppendValue(dst, bulk(v.Str))
	case resp.KindBulkError:
		return resp.AppendValue(dst, resp.Value{Kind: resp.KindError, Str: v.Str})
	case resp.KindMap, resp.KindSet, resp.KindPush, resp.KindArray:
		dst = append(dst, '*')
		dst = strconv.AppendInt(dst, int64(len(v.Elems)), 10)
		dst = append(dst, '\r', '\n')
		for _, e := range v.Elems {
			dst = encode2(dst, e)
		}
		return dst
	}
	return resp.AppendValue(dst, v)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func errorf(format string, args ...any) resp.Value {
	return resp.Value{Kind: resp.KindError, Str: fmt.Sprintf(format, args...)}
}

var (
	ok    = resp.Value{Kind: resp.KindSimple, Str: "OK"}
	null  = resp.Value{Kind: resp.KindNull}
	wrong = func(cmd string) resp.Value {
		return errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd))
	}
)

func integer(n int64) resp.Value { return resp.Value{Kind: resp.KindInt, Int: n} }
func bulk(s string) resp.Value   { return resp.Value{Kind: resp.KindBulk, Str: s} }

func (s *Server) handle(sess *session, args []string) (resp.Value, bool) {
	cmd := strings.ToUpper(args[0])
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[cmd]++
	if msg, ok := s.failNext[cmd]; ok {
		delete(s.failNext, cmd)
		return errorf("%s", msg), false
	}
	if !sess.authed && cmd != "AUTH" && cmd != "HELLO" && cmd != "QUIT" {
		return errorf("NOAUTH Authentication required."), false
	}
	asking := sess.asking
	sess.asking = false

	switch cmd {
	case "QUIT":
		return ok, true
	case "PING":
		if len(args) > 1 {
			return bulk(args[1]), false
		}
		return resp.Value{Kind: resp.KindSimple, Str: "PONG"}, false
	case "ECHO":
		if len(args) != 2 {
			return wrong(cmd), false
		}
		return bulk(args[1]), false
	case "AUTH":
		return s.auth(sess, args[1:]), false
	case "HELLO":
		return s.hello(sess, args[1:]), false
	case "SELECT":
		if len(args) != 2 {
			return wrong(cmd), false
		}
		db, err := strconv.Atoi(args[1])
		if err != nil || db < 0 || db > 15 {
			return errorf("ERR DB index is out of range"), false
		}
		sess.db = db
		return ok, false
	case "CLIENT":
		return ok, false
	case "ASKING":
		sess.asking = true
		return ok, false
	case "TIME":
		now := s.clock()
		return resp.Value{Kind: resp.KindArray, Elems: []resp.Value{
			bulk(strconv.FormatInt(now.Unix(), 10)),
			bulk(strconv.Itoa(now.Nanosecond() / 1000)),
		}}, false
	case "DBSIZE":
		n := 0
		for k := range s.dbs[sess.db] {
			if _, ok := s.lookup(sess.db, k); ok {
				n++
			}
		}
		return integer(int64(n)), false
	case "FLUSHALL":
		s.dbs = map[int]map[string]item{}
		return ok, false
	case "SCRIPT":
		return s.script(args[1:]), false
	}

	// Keyed commands: redirects and cross-slot checks apply.
	keys, reply, bad := s.keysOf(cmd, args)
	if bad {
		return reply, false
	}
	if len(keys) > 0 {
		if r, ok := s.redirects[resp.Slot(keys[0])]; ok && !(r.ask && asking) {
			kind := "MOVED"
			if r.ask {
				kind = "ASK"
			}
			return errorf("%s %d %s", kind, resp.Slot(keys[0]), r.addr), false
		}
		if s.opts.Cluster {
			for _, k := range keys[1:] {
				if resp.Slot(k) != resp.Slot(keys[0]) {
					return errorf("CROSSSLOT Keys in request don't hash to the same slot"), false
				}
			}
		}
	}

	switch cmd {
	case "GET":
		it, ok := s.lookup(sess.db, args[1])
		if !ok {
			return null, false
		}
		return bulk(it.val), false
	case "SET":
		return s.set(sess.db, args[1:]), false
	case "DEL", "EXISTS":
		var n int64
		for _, k := range keys {
			if _, ok := s.lookup(sess.db, k); ok {
				n++
				if cmd == "DEL" {
					delete(s.dbs[sess.db], k)
				}
			}
		}
		return integer(n), false
	case "PTTL":
		it, ok := s.lookup(sess.db, args[1])
		switch {
		case !ok:
			return integer(-2), false
		case it.expireAt.IsZero():
			return integer(-1), false
		}
		return integer(int64(math.Ceil(float64(it.expireAt.Sub(s.clock())) / float64(time.Millisecond)))), false
	case "EVAL", "EVALSHA":
		return s.eval(sess, cmd, args[1:]), false
	}
	return errorf("ERR unknown command '%s'", args[0]), false
}

// keysOf extracts the keys of keyed commands and validates arity.
func (s *Server) keysOf(cmd string, args []string) ([]string, resp.Value, bool) {
	switch cmd {
	case "GET", "PTTL":
		if len(args) != 2 {
			return nil, wrong(cmd), true
		}
		return args[1:2], resp.Value{}, false
	case "SET":
		if len(args) < 3 {
			return nil, wrong(cmd), true
		}
		return args[1:2], resp.Value{}, false
	case "DEL", "EXISTS":
		if len(args) < 2 {
			return nil, wrong(cmd), true
		}
		return args[1:], resp.Value{}, false
	case "EVAL", "EVALSHA":
		if len(args) < 3 {
			return nil, wrong(cmd), true
		}
		n, err := strconv.Atoi(args[2])
		if err != nil || n < 0 {
			return nil, errorf("ERR value is not an integer or out of range"), true
		}
		if n > len(args)-3 {
			return nil, errorf("ERR Number of keys can't be greater than number of args"), true
		}
		return args[3 : 3+n], resp.Value{}, false
	}
	return nil, resp.Value{}, false
}

func (s *Server) auth(sess *session, args []string) resp.Value {
	var user, pass string
	switch len(args) {
	case 1:
		user, pass = "default", args[0]
	case 2:
		user, pass = args[0], args[1]
	default:
		return wrong("auth")
	}
	if s.opts.Password == "" {
		return errorf("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}
	wantUser := s.opts.Username
	if wantUser == "" {
		wantUser = "default"
	}
	if user != wantUser || pass != s.opts.Password {
		return errorf("WRONGPASS invalid username-password pair or user is disabled.")
	}
	sess.authed = true
	return ok
}

func (s *Server) hello(sess *session, args []string) resp.Value {
	proto := sess.proto
	if len(args) > 0 {
		p, err := strconv.Atoi(args[0])
		if err != nil || (p != 2 && p != 3) {
			return errorf("NOPROTO unsupported protocol version")
		}
		proto = p
		args = args[1:]
	}
	for len(args) > 0 {
		switch strings.ToUpper(args[0]) {
		case "AUTH":
			if len(args) < 3 {
				return errorf("ERR Syntax error in HELLO option 'auth'")
			}
			if r := s.auth(sess, args[1:3]); r.Kind == resp.KindError {
				return r
			}
			args = args[3:]
		case "SETNAME":
			if len(args) < 2 {
				return errorf("ERR Syntax error in HELLO option 'setname'")
			}
			args = args[2:]
		default:
			return errorf("ERR Syntax error in HELLO option '%s'", args[0])
		}
	}
	if !sess.authed {
		return errorf("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
	}
	sess.proto = proto
	str := func(s string) resp.Value { return bulk(s) }
	return resp.Value{Kind: resp.KindMap, Elems: []resp.Value{
		str("server"), str("redis"),
		str("version"), str("7.2.0-fake"),
		str("proto"), integer(int64(proto)),
		str("mode"), str("standalone"),
		str("role"), str("master"),
		str("modules"), {Kind: resp.KindArray},
	}}
}

func (s *Server) script(args []string) resp.Value {
	if len(args) == 0 {
		return wrong("script")
	}
	switch strings.ToUpper(args[0]) {
	case "LOAD":
		if len(args) != 2 {
			return wrong("script|load")
		}
		return bulk(s.register(args[1]))
	case "EXISTS":
		elems := make([]resp.Value, 0, len(args)-1)
		for _, sha := range args[1:] {
			_, ok := s.scripts[strings.ToLower(sha)]
			elems = append(elems, integer(int64(boolInt(ok))))
		}
		return resp.Value{Kind: resp.KindArray, Elems: elems}
	case "FLUSH":
		s.scripts = map[string]string{}
		return ok
	}
	return errorf("ERR unknown subcommand '%s'", args[0])
}

func (s *Server) register(src string) string {
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])
	s.scripts[sha] = src
	return sha
}

func (s *Server) lookup(db int, key string) (item, bool) {
	it, ok := s.dbs[db][key]
	if !ok {
		return item{}, false
	}
	if !it.expireAt.IsZero() && !s.clock().Before(it.expireAt) {
		delete(s.dbs[db], key)
		return item{}, false
	}
	return it, true
}

func (s *Server) store(db int, key string, it item) {
	m, ok := s.dbs[db]
	if !ok {
		m = map[string]item{}
		s.dbs[db] = m
	}
	m[key] = it
}

func (s *Server) set(db int, args []string) resp.Value {
	key, val := args[0], args[1]
	var expireAt time.Time
	nx, xx := false, false
	for i := 2; i < len(args); i++ {
		switch opt := strings.ToUpper(args[i]); opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "EX", "PX":
			if i+1 >= len(args) {
				return errorf("ERR syntax error")
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || n <= 0 {
				return errorf("ERR invalid expire time in 'set' command")
			}
			unit := time.Millisecond
			if opt == "EX" {
				unit = time.Second
			}
			expireAt = s.clock().Add(time.Duration(n) * unit)
			i++
		default:
			return errorf("ERR syntax error")
		}
	}
	_, exists := s.lookup(db, key)
	if (nx && exists) || (xx && !exists) {
		return null
	}
	s.store(db, key, item{val: val, expireAt: expireAt})
	return ok
}

func (s *Server) eval(sess *session, cmd string, args []string) resp.Value {
	var src string
	if cmd == "EVAL" {
		src = args[0]
		s.register(src)
	} else {
		var ok bool
		if src, ok = s.scripts[strings.ToLower(args[0])]; !ok {
			return errorf("NOSCRIPT No matching script. Please use EVAL.")
		}
	}
	n, _ := strconv.Atoi(args[1])
	keys, argv := args[2:2+n], args[2+n:]
	if !strings.HasPrefix(src, GCRAMarker) {
		return errorf("ERR the fake server cannot run arbitrary Lua")
	}
	v, err := s.gcra(sess.db, keys, argv)
	if err != nil {
		return errorf("ERR Error running script: %v", err)
	}
	return v
}

// gcra emulates redisstore's gcra.lua line by line, in microseconds.
func (s *Server) gcra(db int, keys, argv []string) (resp.Value, error) {
	if len(argv) != 1+3*len(keys) {
		return resp.Value{}, errors.New("bad argument count")
	}
	nowT := s.clock()
	now := nowT.UnixMicro()
	mode := argv[0]
	tats := map[string]int64{}
	var changed []string
	out := []int64{1, now}
	all := true
	stored := func(key string) (int64, error) {
		if tat, ok := tats[key]; ok {
			return tat, nil
		}
		it, ok := s.lookup(db, key)
		if !ok {
			return 0, nil
		}
		return strconv.ParseInt(it.val, 10, 64)
	}
	for i, key := range keys {
		var nums [3]int64
		for j := range nums {
			n, err := strconv.ParseInt(argv[1+3*i+j], 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("argument %d: %w", 1+3*i+j, err)
			}
			nums[j] = n
		}
		iv, burst, cost := nums[0], nums[1], nums[2]
		tat, err := stored(key)
		if err != nil {
			return resp.Value{}, err
		}
		if mode == "refund" {
			if _, seen := tats[key]; !seen {
				changed = append(changed, key)
			}
			tats[key] = tat - iv*cost
			continue
		}
		o := limiter.GCRAStep(tat, now, iv, burst, cost)
		if !o.Allowed {
			all = false
			out = append(out, 0, o.Remaining, o.RetryAfter, o.ResetAfter)
			continue
		}
		out = append(out, 1, o.Remaining, 0, o.ResetAfter)
		if cost > 0 {
			if _, seen := tats[key]; !seen {
				changed = append(changed, key)
			}
			tats[key] = o.TAT
		}
	}
	write := func(key string, tat int64) {
		if tat > now {
			ttl := time.Duration((tat-now+999)/1000) * time.Millisecond
			s.store(db, key, item{val: strconv.FormatInt(tat, 10), expireAt: nowT.Add(ttl)})
		} else {
			delete(s.dbs[db], key)
		}
	}
	if mode == "refund" {
		for _, k := range changed {
			write(k, tats[k])
		}
		return ints(1, now), nil
	}
	if !all {
		out[0] = 0
	}
	if mode == "take" && all {
		for _, k := range changed {
			write(k, tats[k])
		}
	}
	return ints(out...), nil
}

func ints(ns ...int64) resp.Value {
	elems := make([]resp.Value, len(ns))
	for i, n := range ns {
		elems[i] = integer(n)
	}
	return resp.Value{Kind: resp.KindArray, Elems: elems}
}
