package redistest

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/resp"
)

func TestCommands(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	srv, err := Start(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetClock(func() time.Time { return now })
	c := resp.NewClient(resp.Options{Addr: srv.Addr()})
	defer c.Close()
	ctx := context.Background()

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"PING", "hi"}, `"hi"`},
		{[]string{"ECHO", "x"}, `"x"`},
		{[]string{"ECHO"}, "(error) ERR wrong number of arguments for 'echo' command"},
		{[]string{"SET", "k", "v", "EX", "10"}, `"OK"`},
		{[]string{"SET", "k", "w", "NX"}, "(nil)"},
		{[]string{"SET", "missing", "w", "XX"}, "(nil)"},
		{[]string{"SET", "k", "v", "PX"}, "(error) ERR syntax error"},
		{[]string{"SET", "k", "v", "PX", "0"}, "(error) ERR invalid expire time in 'set' command"},
		{[]string{"SET", "k", "v", "KEEPTTL"}, "(error) ERR syntax error"},
		{[]string{"SET", "k"}, "(error) ERR wrong number of arguments for 'set' command"},
		{[]string{"PTTL", "k"}, "10000"},
		{[]string{"PTTL", "nope"}, "-2"},
		{[]string{"SET", "p", "1"}, `"OK"`},
		{[]string{"PTTL", "p"}, "-1"},
		{[]string{"PTTL"}, "(error) ERR wrong number of arguments for 'pttl' command"},
		{[]string{"GET"}, "(error) ERR wrong number of arguments for 'get' command"},
		{[]string{"DEL"}, "(error) ERR wrong number of arguments for 'del' command"},
		{[]string{"DBSIZE"}, "2"},
		{[]string{"TIME"}, `*["1767225600" "0"]`},
		{[]string{"CLIENT", "SETNAME", "x"}, `"OK"`},
		{[]string{"SELECT", "x"}, "(error) ERR DB index is out of range"},
		{[]string{"SELECT"}, "(error) ERR wrong number of arguments for 'select' command"},
		{[]string{"SCRIPT"}, "(error) ERR wrong number of arguments for 'script' command"},
		{[]string{"SCRIPT", "LOAD"}, "(error) ERR wrong number of arguments for 'script|load' command"},
		{[]string{"SCRIPT", "EXISTS", "abc"}, "*[0]"},
		{[]string{"SCRIPT", "KILL"}, "(error) ERR unknown subcommand 'KILL'"},
		{[]string{"SCRIPT", "FLUSH"}, `"OK"`},
		{[]string{"EVALSHA", "abc"}, "(error) ERR wrong number of arguments for 'evalsha' command"},
		{[]string{"EVALSHA", "abc", "x"}, "(error) ERR value is not an integer or out of range"},
		{[]string{"EVALSHA", "abc", "2", "k"}, "(error) ERR Number of keys can't be greater than number of args"},
		{[]string{"EVAL", GCRAMarker, "1", "k", "take", "1"}, "(error) ERR Error running script: bad argument count"},
		{[]string{"EVAL", GCRAMarker, "1", "k", "take", "x", "1", "1"}, `(error) ERR Error running script: argument 1: strconv.ParseInt: parsing "x": invalid syntax`},
		{[]string{"EVAL", GCRAMarker, "1", "k", "take", "1", "1", "1"}, `(error) ERR Error running script: strconv.ParseInt: parsing "v": invalid syntax`},
		{[]string{"AUTH", "pw"}, "(error) ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"},
		{[]string{"AUTH"}, "(error) ERR wrong number of arguments for 'auth' command"},
		{[]string{"HELLO", "4"}, "(error) NOPROTO unsupported protocol version"},
		{[]string{"HELLO", "2", "AUTH", "u"}, "(error) ERR Syntax error in HELLO option 'auth'"},
		{[]string{"HELLO", "2", "SETNAME"}, "(error) ERR Syntax error in HELLO option 'setname'"},
		{[]string{"HELLO", "2", "BOGUS"}, "(error) ERR Syntax error in HELLO option 'BOGUS'"},
		{[]string{"HELLO", "2", "SETNAME", "n"}, `*["server" "redis" "version" "7.2.0-fake" "proto" 2 "mode" "standalone" "role" "master" "modules" *[]]`},
		{[]string{"FLUSHALL"}, `"OK"`},
		{[]string{"EXISTS", "k"}, "0"},
	}
	for _, s := range steps {
		v, _ := c.Do(ctx, s.args...)
		if got := v.String(); got != s.want {
			t.Errorf("%v = %s, want %s", s.args, got, s.want)
		}
	}
	if srv.TTL("none") != -1 || srv.URL() != "redis://"+srv.Addr() {
		t.Error("helpers")
	}
	srv.store(0, "x", item{val: "1"})
	if srv.TTL("x") != -1 {
		t.Error("TTL without expiry")
	}
}

func TestRESP2Downgrade(t *testing.T) {
	values := map[string]resp.Value{
		"$-1\r\n":            null,
		":1\r\n":             {Kind: resp.KindBool, Bool: true},
		":0\r\n":             {Kind: resp.KindBool},
		"$3\r\n1.5\r\n":      {Kind: resp.KindDouble, Float: 1.5},
		"$2\r\n12\r\n":       {Kind: resp.KindBigNumber, Str: "12"},
		"$1\r\nx\r\n":        {Kind: resp.KindVerbatim, Format: "txt", Str: "x"},
		"-E\r\n":             {Kind: resp.KindBulkError, Str: "E"},
		"*1\r\n$-1\r\n":      {Kind: resp.KindSet, Elems: []resp.Value{null}},
		"*2\r\n+k\r\n:1\r\n": {Kind: resp.KindMap, Elems: []resp.Value{{Kind: resp.KindSimple, Str: "k"}, integer(1)}},
	}
	for want, v := range values {
		if got := string(encode2(nil, v)); got != want {
			t.Errorf("encode2(%v) = %q, want %q", v, got, want)
		}
	}
}

func TestProtocolErrorAndQuit(t *testing.T) {
	srv, err := Start(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	for _, in := range []string{"+inline\r\n", "*1\r\n:1\r\n"} {
		conn, err := net.Dial("tcp", srv.Addr())
		if err != nil {
			t.Fatal(err)
		}
		conn.Write([]byte(in))
		line, _ := bufio.NewReader(conn).ReadString('\n')
		if !strings.HasPrefix(line, "-ERR Protocol error") {
			t.Errorf("%q -> %q", in, line)
		}
		conn.Close()
	}
	conn, _ := net.Dial("tcp", srv.Addr())
	defer conn.Close()
	conn.Write(resp.AppendCommand(nil, "QUIT"))
	br := bufio.NewReader(conn)
	if line, _ := br.ReadString('\n'); line != "+OK\r\n" {
		t.Errorf("QUIT -> %q", line)
	}
	if _, err := br.ReadByte(); err == nil {
		t.Error("connection still open after QUIT")
	}
}
