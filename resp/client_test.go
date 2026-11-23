package resp_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/internal/redistest"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

func startServer(t testing.TB, opts redistest.Options) *redistest.Server {
	t.Helper()
	srv, err := redistest.Start(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func newClient(t testing.TB, o resp.Options) *resp.Client {
	t.Helper()
	c := resp.NewClient(o)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestClientBasics(t *testing.T) {
	ctx := context.Background()
	for _, proto := range []int{2, 3} {
		t.Run(fmt.Sprint("RESP", proto), func(t *testing.T) {
			srv := startServer(t, redistest.Options{})
			c := newClient(t, resp.Options{Addr: srv.Addr(), Protocol: proto, ClientName: "test"})
			v, err := c.Do(ctx, "PING")
			if err != nil || v.Str != "PONG" {
				t.Fatalf("PING: %v %v", v, err)
			}
			if _, err := c.Do(ctx, "SET", "k", "v", "PX", "60000"); err != nil {
				t.Fatal(err)
			}
			v, _ = c.Do(ctx, "GET", "k")
			if s, _ := v.Text(); s != "v" {
				t.Fatalf("GET = %v", v)
			}
			v, _ = c.Do(ctx, "GET", "missing")
			if !v.IsNull() {
				t.Fatalf("GET missing = %v", v)
			}
			_, err = c.Do(ctx, "NOPE")
			var rerr *resp.Error
			if !errors.As(err, &rerr) || rerr.Code() != "ERR" {
				t.Fatalf("unknown command: %v", err)
			}
			// The connection survives an error reply.
			if _, err := c.Do(ctx, "PING"); err != nil {
				t.Fatal(err)
			}
			if st := c.Stats(); st.Dials != 1 || st.Idle != 1 || st.Active != 0 {
				t.Fatalf("stats %+v", st)
			}
			wantHello := 0
			if proto == 3 {
				wantHello = 1
			}
			if srv.Count("HELLO") != wantHello {
				t.Fatalf("HELLO count %d", srv.Count("HELLO"))
			}
		})
	}
}

func TestClientPipeline(t *testing.T) {
	srv := startServer(t, redistest.Options{})
	c := newClient(t, resp.Options{Addr: srv.Addr()})
	vs, err := c.Pipeline(context.Background(),
		[]string{"SET", "a", "1"},
		[]string{"GET", "a"},
		[]string{"BOGUS"},
		[]string{"DEL", "a", "b"},
		[]string{"EXISTS", "a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 5 || vs[1].Str != "1" || vs[2].Kind != resp.KindError || vs[3].Int != 1 || vs[4].Int != 0 {
		t.Fatalf("pipeline = %v", vs)
	}
	if vs, err := c.Pipeline(context.Background()); vs != nil || err != nil {
		t.Fatal("empty pipeline")
	}
}

func TestClientAuthAndSelect(t *testing.T) {
	ctx := context.Background()
	srv := startServer(t, redistest.Options{Username: "app", Password: "s3cret"})
	for _, proto := range []int{2, 3} {
		c := newClient(t, resp.Options{Addr: srv.Addr(), Username: "app", Password: "s3cret", DB: 3, Protocol: proto})
		if _, err := c.Do(ctx, "SET", "k", "db3"); err != nil {
			t.Fatalf("RESP%d: %v", proto, err)
		}
	}
	if _, ok := srv.Get("k"); ok {
		t.Fatal("SELECT was not applied: key landed in db 0")
	}
	bad := newClient(t, resp.Options{Addr: srv.Addr(), Username: "app", Password: "wrong"})
	if _, err := bad.Do(ctx, "PING"); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("wrong password: %v", err)
	}
	none := newClient(t, resp.Options{Addr: srv.Addr()})
	if _, err := none.Do(ctx, "PING"); err == nil || !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("no password: %v", err)
	}
	srv2 := startServer(t, redistest.Options{Password: "pw"})
	pwOnly := newClient(t, resp.Options{Addr: srv2.Addr(), Password: "pw"})
	if _, err := pwOnly.Do(ctx, "PING"); err != nil {
		t.Fatalf("password-only AUTH: %v", err)
	}
	pwOnly3 := newClient(t, resp.Options{Addr: srv2.Addr(), Password: "pw", Protocol: 3})
	if _, err := pwOnly3.Do(ctx, "PING"); err != nil {
		t.Fatalf("HELLO AUTH default: %v", err)
	}
}

func TestClientScriptNoScriptFallback(t *testing.T) {
	ctx := context.Background()
	srv := startServer(t, redistest.Options{})
	c := newClient(t, resp.Options{Addr: srv.Addr()})
	s := resp.NewScript(redistest.GCRAMarker + "\n-- test")
	if s.Source() == "" || len(s.SHA()) != 40 {
		t.Fatal("script")
	}
	args := []string{"take", "1000", "1", "1"}
	if _, err := c.Run(ctx, s, []string{"k"}, args...); err != nil {
		t.Fatal(err)
	}
	if srv.Count("EVALSHA") != 1 || srv.Count("EVAL") != 1 {
		t.Fatalf("first run: EVALSHA %d EVAL %d", srv.Count("EVALSHA"), srv.Count("EVAL"))
	}
	if _, err := c.Run(ctx, s, []string{"k"}, args...); err != nil {
		t.Fatal(err)
	}
	if srv.Count("EVAL") != 1 {
		t.Fatal("cached script re-sent")
	}
	srv.FlushScripts()
	if _, err := c.Run(ctx, s, []string{"k"}, args...); err != nil {
		t.Fatal(err)
	}
	if srv.Count("EVAL") != 2 {
		t.Fatal("no EVAL after flush")
	}
	srv.FlushScripts()
	if err := c.Load(ctx, s); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Do(ctx, "SCRIPT", "EXISTS", s.SHA()); v.Elems[0].Int != 1 {
		t.Fatal("SCRIPT LOAD did not cache")
	}
	if _, err := c.Run(ctx, resp.NewScript("return 1"), nil); err == nil {
		t.Fatal("fake server should refuse foreign Lua")
	}
	srv.FailNext("SCRIPT", "ERR load failed")
	if err := c.Load(ctx, s); err == nil {
		t.Fatal("Load error")
	}
}

func TestClientTimeouts(t *testing.T) {
	srv := startServer(t, redistest.Options{})
	c := newClient(t, resp.Options{Addr: srv.Addr(), ReadTimeout: 50 * time.Millisecond})
	if _, err := c.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	srv.SetDelay(200 * time.Millisecond)
	start := time.Now()
	_, err := c.Do(context.Background(), "PING")
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("want timeout, got %v", err)
	}
	if time.Since(start) > 150*time.Millisecond {
		t.Fatalf("timeout took %s", time.Since(start))
	}
	if c.Stats().Timeouts != 1 {
		t.Fatal("timeout not counted")
	}
	// Context deadline shorter than ReadTimeout wins.
	c2 := newClient(t, resp.Options{Addr: srv.Addr()})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c2.Do(ctx, "PING"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx deadline: %v", err)
	}
	// Cancellation mid-flight unblocks the read.
	ctx, cancel = context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if _, err := c2.Do(ctx, "PING"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	// Broken connections are dropped, and the next command dials afresh.
	srv.SetDelay(0)
	if _, err := c2.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Do(ctx, "PING"); !errors.Is(err, context.Canceled) {
		t.Fatalf("already cancelled: %v", err)
	}
}

func TestClientPoolBound(t *testing.T) {
	srv := startServer(t, redistest.Options{})
	srv.SetDelay(20 * time.Millisecond)
	c := newClient(t, resp.Options{Addr: srv.Addr(), PoolSize: 2})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Do(context.Background(), "PING"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if st := c.Stats(); st.Dials > 2 {
		t.Fatalf("dialed %d connections with PoolSize 2", st.Dials)
	}
	// Waiting for a connection respects the context.
	hold := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() { <-hold; _, _ = c.Do(context.Background(), "PING") }()
	}
	srv.SetDelay(300 * time.Millisecond)
	close(hold)
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Do(ctx, "PING"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool wait: %v", err)
	}
}

func TestClientServerDown(t *testing.T) {
	srv := startServer(t, redistest.Options{})
	c := newClient(t, resp.Options{Addr: srv.Addr(), DialTimeout: 100 * time.Millisecond})
	if _, err := c.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	srv.SetDown(true)
	if _, err := c.Do(context.Background(), "PING"); err == nil {
		t.Fatal("want error from a closed connection")
	}
	srv.SetDown(false)
	if _, err := c.Do(context.Background(), "PING"); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	dead := newClient(t, resp.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond})
	if _, err := dead.Do(context.Background(), "PING"); err == nil || !strings.Contains(err.Error(), "dial") {
		t.Fatalf("dial error: %v", err)
	}
}

func TestClientIdleTimeout(t *testing.T) {
	srv := startServer(t, redistest.Options{})
	c := newClient(t, resp.Options{Addr: srv.Addr(), IdleTimeout: 10 * time.Millisecond, MaxIdle: 1})
	_, _ = c.Do(context.Background(), "PING")
	time.Sleep(20 * time.Millisecond)
	_, _ = c.Do(context.Background(), "PING")
	if st := c.Stats(); st.Dials != 2 {
		t.Fatalf("idle connection reused: %+v", st)
	}
}

func TestClientClose(t *testing.T) {
	srv := startServer(t, redistest.Options{})
	c := resp.NewClient(resp.Options{Addr: srv.Addr()})
	_, _ = c.Do(context.Background(), "PING")
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(context.Background(), "PING"); !errors.Is(err, resp.ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
}

func TestClientRedirects(t *testing.T) {
	ctx := context.Background()
	a := startServer(t, redistest.Options{})
	b := startServer(t, redistest.Options{})
	c := newClient(t, resp.Options{Addr: a.Addr()})
	slot := resp.Slot("k")

	a.Redirect(slot, b.Addr(), false)
	if _, err := c.DoKeyed(ctx, "k", "SET", "k", "moved"); err != nil {
		t.Fatal(err)
	}
	if v, _ := b.Get("k"); v != "moved" {
		t.Fatal("MOVED not followed")
	}
	// The slot is now cached: a is not asked again.
	before := a.Count("GET")
	if v, err := c.DoKeyed(ctx, "k", "GET", "k"); err != nil || v.Str != "moved" {
		t.Fatalf("cached slot: %v %v", v, err)
	}
	if a.Count("GET") != before {
		t.Fatal("slot cache not used")
	}

	// ASK redirects once, with ASKING, without updating the cache.
	a.Redirect(slot, "", false)
	b.Redirect(slot, a.Addr(), true)
	if _, err := c.DoKeyed(ctx, "k", "SET", "k", "asked"); err != nil {
		t.Fatal(err)
	}
	if v, _ := a.Get("k"); v != "asked" || a.Count("ASKING") != 1 {
		t.Fatal("ASK not followed")
	}
	b.Redirect(resp.Slot("k"), "", false)

	// Redirect loops are bounded.
	a.Redirect(resp.Slot("loop"), a.Addr(), false)
	_, err := c.DoKeyed(ctx, "loop", "GET", "loop")
	var rerr *resp.Error
	if !errors.As(err, &rerr) || rerr.Code() != "MOVED" {
		t.Fatalf("redirect loop: %v", err)
	}
	// Following can be disabled.
	nf := newClient(t, resp.Options{Addr: a.Addr(), MaxRedirects: -1})
	if _, err := nf.DoKeyed(ctx, "loop", "GET", "loop"); !errors.As(err, &rerr) {
		t.Fatalf("no follow: %v", err)
	}
	// Malformed MOVED replies are returned as is.
	a.FailNext("GET", "MOVED garbage")
	if _, err := c.DoKeyed(ctx, "x", "GET", "x"); err == nil || err.Error() != "MOVED garbage" {
		t.Fatalf("malformed: %v", err)
	}
}

func TestParseURL(t *testing.T) {
	o, err := resp.ParseURL("redis://app:pw@cache.internal:6380/2?protocol=3&pool_size=7&read_timeout=250ms&client_name=api")
	if err != nil {
		t.Fatal(err)
	}
	if o.Addr != "cache.internal:6380" || o.Username != "app" || o.Password != "pw" || o.DB != 2 ||
		o.Protocol != 3 || o.PoolSize != 7 || o.ReadTimeout != 250*time.Millisecond || o.ClientName != "api" || o.TLSConfig != nil {
		t.Fatalf("%+v", o)
	}
	o, err = resp.ParseURL("rediss://secret@[::1]")
	if err != nil || o.Addr != "[::1]:6379" || o.Password != "secret" || o.Username != "" || o.TLSConfig == nil {
		t.Fatalf("%+v %v", o, err)
	}
	if o, _ := resp.ParseURL("redis://"); o.Addr != "localhost:6379" {
		t.Fatalf("default addr %q", o.Addr)
	}
	for _, bad := range []string{
		"http://x", "redis://x/abc", "redis://x/-1", "redis://x?protocol=4", "redis://x?protocol=a",
		"redis://x?dial_timeout=fast", "redis://%zz",
	} {
		if _, err := resp.ParseURL(bad); err == nil {
			t.Errorf("ParseURL(%q) succeeded", bad)
		}
	}
	if o := resp.NewClient(resp.Options{}).Options(); o.Addr != "localhost:6379" || o.PoolSize == 0 || o.MaxRedirects != 3 {
		t.Fatalf("defaults %+v", o)
	}
}

func TestClientHandshakeErrors(t *testing.T) {
	srv := startServer(t, redistest.Options{})
	c := newClient(t, resp.Options{Addr: srv.Addr(), DB: 99})
	if _, err := c.Do(context.Background(), "PING"); err == nil || !strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("bad SELECT: %v", err)
	}
}

func TestClientPushMessagesSkipped(t *testing.T) {
	// A server that sends an out-of-band push before the reply.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1024)
		_, _ = conn.Read(buf)
		_, _ = conn.Write([]byte(">2\r\n+invalidate\r\n*0\r\n+PONG\r\n"))
		_, _ = conn.Read(buf)
	}()
	c := newClient(t, resp.Options{Addr: ln.Addr().String()})
	v, err := c.Do(context.Background(), "PING")
	if err != nil || v.Str != "PONG" {
		t.Fatalf("got %v %v", v, err)
	}
}
