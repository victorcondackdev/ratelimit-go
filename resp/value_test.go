package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func parse(s string) (Value, error) {
	return NewReader(strings.NewReader(s)).ReadValue()
}

func TestReadValue(t *testing.T) {
	tests := []struct {
		in   string
		want Value
	}{
		{"+OK\r\n", Value{Kind: KindSimple, Str: "OK"}},
		{"-ERR boom\r\n", Value{Kind: KindError, Str: "ERR boom"}},
		{":-42\r\n", Value{Kind: KindInt, Int: -42}},
		{"$5\r\nhe\r\no\r\n", Value{Kind: KindBulk, Str: "he\r\no"}},
		{"$0\r\n\r\n", Value{Kind: KindBulk, Str: ""}},
		{"$-1\r\n", Value{Kind: KindNull}},
		{"*-1\r\n", Value{Kind: KindNull}},
		{"_\r\n", Value{Kind: KindNull}},
		{"#t\r\n", Value{Kind: KindBool, Bool: true}},
		{"#f\r\n", Value{Kind: KindBool}},
		{",3.25\r\n", Value{Kind: KindDouble, Float: 3.25}},
		{",-inf\r\n", Value{Kind: KindDouble, Float: math.Inf(-1)}},
		{"(-3492890328409238509324850943850943825024385\r\n", Value{Kind: KindBigNumber, Str: "-3492890328409238509324850943850943825024385"}},
		{"!21\r\nSYNTAX invalid syntax\r\n", Value{Kind: KindBulkError, Str: "SYNTAX invalid syntax"}},
		{"=15\r\ntxt:Some string\r\n", Value{Kind: KindVerbatim, Format: "txt", Str: "Some string"}},
		{"*2\r\n:1\r\n$1\r\na\r\n", Value{Kind: KindArray, Elems: []Value{{Kind: KindInt, Int: 1}, {Kind: KindBulk, Str: "a"}}}},
		{"*0\r\n", Value{Kind: KindArray, Elems: []Value{}}},
		{"%1\r\n+k\r\n:2\r\n", Value{Kind: KindMap, Elems: []Value{{Kind: KindSimple, Str: "k"}, {Kind: KindInt, Int: 2}}}},
		{"~1\r\n#t\r\n", Value{Kind: KindSet, Elems: []Value{{Kind: KindBool, Bool: true}}}},
		{">1\r\n+x\r\n", Value{Kind: KindPush, Elems: []Value{{Kind: KindSimple, Str: "x"}}}},
		{"|1\r\n+ttl\r\n:3\r\n:7\r\n", Value{Kind: KindInt, Int: 7}}, // attribute skipped
	}
	for _, tt := range tests {
		got, err := parse(tt.in)
		if err != nil {
			t.Errorf("parse(%q): %v", tt.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("parse(%q) = %#v, want %#v", tt.in, got, tt.want)
		}
	}
}

func TestReadValueErrors(t *testing.T) {
	tests := []string{
		"",
		"+OK",
		"+OK\n",
		"\r\n",
		"?x\r\n",
		":\r\n",
		":12a\r\n",
		"$-2\r\n",
		"$3\r\nab\r\n",
		"$3\r\nabcd\r\n",
		"$2\r\nab",
		"*-2\r\n",
		"*1\r\n",
		"*x\r\n",
		"_x\r\n",
		"#x\r\n",
		",\r\n",
		",abc\r\n",
		"(12x\r\n",
		"(\r\n",
		"=3\r\nabc\r\n",
		"=5\r\ntxtxx\r\n",
		"%1\r\n+k\r\n",
		"$" + strings.Repeat("9", 30) + "\r\n",
		"$" + strings.Repeat("a", 20000) + "\r\n",
	}
	for _, in := range tests {
		if v, err := parse(in); err == nil {
			t.Errorf("parse(%q) = %v, want error", in, v)
		}
	}
	if _, err := parse("?x\r\n"); !errors.Is(err, ErrProtocol) {
		t.Errorf("unknown type not a protocol error: %v", err)
	}
	if _, err := parse(""); !errors.Is(err, io.EOF) {
		t.Errorf("empty input: %v", err)
	}
	if _, err := parse("$5\r\nab"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("short bulk: %v", err)
	}
}

func TestReaderLimits(t *testing.T) {
	r := NewReader(strings.NewReader("$11\r\nhello world\r\n"))
	r.MaxBulk = 10
	if _, err := r.ReadValue(); !errors.Is(err, ErrProtocol) {
		t.Errorf("MaxBulk: %v", err)
	}
	r = NewReader(strings.NewReader("*3\r\n:1\r\n:2\r\n:3\r\n"))
	r.MaxElems = 2
	if _, err := r.ReadValue(); !errors.Is(err, ErrProtocol) {
		t.Errorf("MaxElems: %v", err)
	}
	deep := strings.Repeat("*1\r\n", 40) + ":1\r\n"
	if _, err := parse(deep); !errors.Is(err, ErrProtocol) {
		t.Errorf("MaxDepth: %v", err)
	}
	// A huge declared length with little data must fail fast, not allocate.
	big := "$" + "500000000" + "\r\nabc"
	if _, err := parse(big); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("huge bulk: %v", err)
	}
	// Large bulk strings are read in chunks.
	payload := strings.Repeat("x", 200<<10)
	v, err := parse("$" + itoa(len(payload)) + "\r\n" + payload + "\r\n")
	if err != nil || v.Str != payload {
		t.Errorf("large bulk: %v", err)
	}
	if _, err := parse("$" + itoa(len(payload)) + "\r\n" + payload + "xx"); !errors.Is(err, ErrProtocol) {
		t.Errorf("large bulk bad terminator: %v", err)
	}
	br := bufio.NewReader(strings.NewReader("+a\r\n+b\r\n"))
	rd := NewReader(br)
	_, _ = rd.ReadValue()
	if rd.Buffered() != 4 {
		t.Errorf("Buffered = %d", rd.Buffered())
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestAppendValueRoundTrip(t *testing.T) {
	values := []Value{
		{Kind: KindSimple, Str: "OK"},
		{Kind: KindError, Str: "ERR x"},
		{Kind: KindInt, Int: math.MinInt64},
		{Kind: KindBulk, Str: "bin\x00\r\n"},
		{Kind: KindNull},
		{Kind: KindBool, Bool: true},
		{Kind: KindBool},
		{Kind: KindDouble, Float: 1.5},
		{Kind: KindDouble, Float: math.Inf(1)},
		{Kind: KindDouble, Float: math.Inf(-1)},
		{Kind: KindBigNumber, Str: "123456789012345678901234567890"},
		{Kind: KindBulkError, Str: "SYNTAX x"},
		{Kind: KindVerbatim, Format: "mkd", Str: "# hi"},
		{Kind: KindArray, Elems: []Value{{Kind: KindInt, Int: 1}, {Kind: KindNull}}},
		{Kind: KindMap, Elems: []Value{{Kind: KindBulk, Str: "k"}, {Kind: KindSet, Elems: []Value{}}}},
		{Kind: KindPush, Elems: []Value{{Kind: KindSimple, Str: "message"}}},
	}
	for _, v := range values {
		enc := AppendValue(nil, v)
		got, err := parse(string(enc))
		if err != nil {
			t.Errorf("%v: %v (encoded %q)", v, err, enc)
			continue
		}
		if !reflect.DeepEqual(got, v) {
			t.Errorf("round trip %#v -> %q -> %#v", v, enc, got)
		}
	}
	nan, _ := parse(string(AppendValue(nil, Value{Kind: KindDouble, Float: math.NaN()})))
	if !math.IsNaN(nan.Float) {
		t.Error("NaN round trip")
	}
	verb, _ := parse(string(AppendValue(nil, Value{Kind: KindVerbatim, Str: "x"})))
	if verb.Format != "txt" {
		t.Error("default verbatim format")
	}
	if bad := AppendValue(nil, Value{Kind: 'Z'}); !bytes.HasPrefix(bad, []byte("-ERR")) {
		t.Errorf("unknown kind encodes as %q", bad)
	}
}

func TestAppendCommand(t *testing.T) {
	got := string(AppendCommand(nil, "SET", "k", ""))
	want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n"
	if got != want {
		t.Errorf("AppendCommand = %q, want %q", got, want)
	}
}

func TestValueAccessors(t *testing.T) {
	if n, err := (Value{Kind: KindBulk, Str: "12"}).Int64(); err != nil || n != 12 {
		t.Error("bulk Int64")
	}
	if n, err := (Value{Kind: KindDouble, Float: 3}).Int64(); err != nil || n != 3 {
		t.Error("double Int64")
	}
	if _, err := (Value{Kind: KindDouble, Float: 3.5}).Int64(); err == nil {
		t.Error("fractional double Int64")
	}
	if _, err := (Value{Kind: KindArray}).Int64(); err == nil {
		t.Error("array Int64")
	}
	if s, ok := (Value{Kind: KindVerbatim, Str: "x"}).Text(); !ok || s != "x" {
		t.Error("Text")
	}
	if _, ok := (Value{Kind: KindInt}).Text(); ok {
		t.Error("int Text")
	}
	e := Value{Kind: KindError, Str: "NOSCRIPT No matching script"}.Err()
	if !IsNoScript(e) || e.(*Error).Code() != "NOSCRIPT" || e.Error() != "NOSCRIPT No matching script" {
		t.Error("NOSCRIPT")
	}
	if (Value{Kind: KindInt}).Err() != nil || IsNoScript(errors.New("x")) {
		t.Error("non-error")
	}
	if !(Value{Kind: KindNull}).IsNull() {
		t.Error("IsNull")
	}
	strs := map[string]Value{
		`"a"`:          {Kind: KindBulk, Str: "a"},
		"(error) E":    {Kind: KindError, Str: "E"},
		"7":            {Kind: KindInt, Int: 7},
		"(nil)":        {Kind: KindNull},
		"true":         {Kind: KindBool, Bool: true},
		"2.5":          {Kind: KindDouble, Float: 2.5},
		`*[1 "b"]`:     {Kind: KindArray, Elems: []Value{{Kind: KindInt, Int: 1}, {Kind: KindBulk, Str: "b"}}},
		"(kind 'Z')":   {Kind: 'Z'},
		`%["k" (nil)]`: {Kind: KindMap, Elems: []Value{{Kind: KindBulk, Str: "k"}, {Kind: KindNull}}},
	}
	for want, v := range strs {
		if got := v.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}

func TestSlot(t *testing.T) {
	// Reference values from the Redis Cluster specification and redis-cli.
	tests := map[string]int{
		"123456789":            12739, // CRC16 check value 0x31C3
		"foo":                  12182,
		"{user1000}.following": Slot("user1000"),
		"foo{}{bar}":           Slot("foo{}{bar}"),
		"foo{{bar}}zap":        Slot("{bar"),
		"foo{bar}{zap}":        Slot("bar"),
	}
	for key, want := range tests {
		if got := Slot(key); got != want {
			t.Errorf("Slot(%q) = %d, want %d", key, got, want)
		}
	}
	if HashTag("foo{}{bar}") != "foo{}{bar}" || HashTag("a{b}c") != "b" {
		t.Error("HashTag")
	}
}

// FuzzReadValue checks that arbitrary bytes never panic or hang the parser,
// and that anything it accepts re-encodes to something it parses to the same
// value.
func FuzzReadValue(f *testing.F) {
	seeds := []string{
		"+OK\r\n", "-ERR x\r\n", ":1\r\n", "$3\r\nabc\r\n", "$-1\r\n", "*-1\r\n",
		"*2\r\n:1\r\n+a\r\n", "%1\r\n+k\r\n:1\r\n", "~1\r\n_\r\n", ">1\r\n#t\r\n",
		",1.5\r\n", "(123\r\n", "!3\r\nERR\r\n", "=7\r\ntxt:abc\r\n", "|1\r\n+a\r\n+b\r\n+c\r\n",
		"$500000000\r\n", "*16777217\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		r.MaxBulk = 1 << 20
		v, err := r.ReadValue()
		if err != nil {
			return
		}
		enc := AppendValue(nil, v)
		r2 := NewReader(bytes.NewReader(enc))
		r2.MaxBulk = 1 << 20
		v2, err := r2.ReadValue()
		if err != nil {
			t.Fatalf("re-encoded value does not parse: %v\ninput %q\nencoded %q", err, data, enc)
		}
		if !equalValue(v, v2) {
			t.Fatalf("round trip mismatch:\n%#v\n%#v", v, v2)
		}
	})
}

func equalValue(a, b Value) bool {
	if a.Kind != b.Kind || a.Str != b.Str || a.Format != b.Format || a.Int != b.Int || a.Bool != b.Bool || len(a.Elems) != len(b.Elems) {
		return false
	}
	if a.Float != b.Float && !(math.IsNaN(a.Float) && math.IsNaN(b.Float)) {
		return false
	}
	for i := range a.Elems {
		if !equalValue(a.Elems[i], b.Elems[i]) {
			return false
		}
	}
	return true
}
