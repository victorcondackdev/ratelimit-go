// Package resp is a small Redis client built on the standard library: a
// RESP2/RESP3 parser and encoder, a connection pool, pipelining, Lua
// scripts with EVALSHA and a NOSCRIPT fallback, AUTH/SELECT/HELLO on
// connect, per-command timeouts, and MOVED/ASK redirects for keys routed
// through a Redis Cluster.
//
// It implements what a rate limiter needs from Redis and nothing more: there
// is no command builder, no pub/sub and no cluster topology discovery. Every
// command is a list of strings and every reply a [Value].
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// Kind is the RESP type of a Value; its value is the type's prefix byte.
type Kind byte

// RESP2 and RESP3 kinds.
const (
	KindSimple    Kind = '+'
	KindError     Kind = '-'
	KindInt       Kind = ':'
	KindBulk      Kind = '$'
	KindArray     Kind = '*'
	KindNull      Kind = '_'
	KindBool      Kind = '#'
	KindDouble    Kind = ','
	KindBigNumber Kind = '('
	KindBulkError Kind = '!'
	KindVerbatim  Kind = '='
	KindMap       Kind = '%'
	KindSet       Kind = '~'
	KindPush      Kind = '>'
)

// kindAttribute is parsed and discarded: attributes annotate the reply that
// follows them and carry nothing this client uses.
const kindAttribute = '|'

// Value is a decoded RESP reply.
//
// Strings (simple, bulk, verbatim, big number and both error kinds) are in
// Str; for verbatim strings, Str excludes the three-letter format and Format
// holds it. Aggregates (array, set, push) are in Elems; a map is in Elems as
// alternating keys and values. RESP2 null bulk strings and null arrays
// decode as KindNull.
type Value struct {
	Kind   Kind
	Str    string
	Format string
	Int    int64
	Float  float64
	Bool   bool
	Elems  []Value
}

// ErrProtocol is wrapped by every parse error.
var ErrProtocol = errors.New("resp: protocol error")

// Error is an error reply from the server ("-ERR ...", "!...").
type Error struct {
	Msg string
}

func (e *Error) Error() string { return e.Msg }

// Code returns the first word of the error, such as "ERR", "NOSCRIPT" or
// "MOVED".
func (e *Error) Code() string {
	code, _, _ := strings.Cut(e.Msg, " ")
	return code
}

// IsNoScript reports whether err is a NOSCRIPT reply.
func IsNoScript(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Code() == "NOSCRIPT"
}

// Err returns the reply as an *Error if it is one, and nil otherwise.
func (v Value) Err() error {
	if v.Kind == KindError || v.Kind == KindBulkError {
		return &Error{Msg: v.Str}
	}
	return nil
}

// IsNull reports whether v is a null of any protocol version.
func (v Value) IsNull() bool { return v.Kind == KindNull }

// Int64 returns v as an integer: integer replies directly, strings parsed in
// base 10.
func (v Value) Int64() (int64, error) {
	switch v.Kind {
	case KindInt:
		return v.Int, nil
	case KindSimple, KindBulk, KindBigNumber:
		return strconv.ParseInt(v.Str, 10, 64)
	case KindDouble:
		if v.Float == math.Trunc(v.Float) && math.Abs(v.Float) < 1<<63 {
			return int64(v.Float), nil
		}
	}
	return 0, fmt.Errorf("resp: %s is not an integer", v)
}

// Text returns the string content of string-like replies.
func (v Value) Text() (string, bool) {
	switch v.Kind {
	case KindSimple, KindBulk, KindVerbatim, KindBigNumber:
		return v.Str, true
	}
	return "", false
}

// String renders v for debugging.
func (v Value) String() string {
	switch v.Kind {
	case KindSimple, KindBulk, KindVerbatim, KindBigNumber:
		return strconv.Quote(v.Str)
	case KindError, KindBulkError:
		return "(error) " + v.Str
	case KindInt:
		return strconv.FormatInt(v.Int, 10)
	case KindNull:
		return "(nil)"
	case KindBool:
		return strconv.FormatBool(v.Bool)
	case KindDouble:
		return strconv.FormatFloat(v.Float, 'g', -1, 64)
	case KindArray, KindSet, KindPush, KindMap:
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = e.String()
		}
		return string(v.Kind) + "[" + strings.Join(parts, " ") + "]"
	}
	return fmt.Sprintf("(kind %q)", byte(v.Kind))
}

// Limits applied by a Reader unless overridden.
const (
	DefaultMaxBulk  = 512 << 20 // Redis' own proto-max-bulk-len default
	DefaultMaxElems = 1 << 24
	DefaultMaxDepth = 32
)

// Reader decodes RESP values from a buffered stream. Its limits protect the
// client from a misbehaving or malicious server: bulk strings are read
// incrementally rather than allocated up front from the declared length,
// aggregate sizes and nesting depth are bounded, and every line must end in
// CRLF.
type Reader struct {
	br       *bufio.Reader
	MaxBulk  int
	MaxElems int
	MaxDepth int
}

// NewReader returns a Reader with default limits. If r is a *bufio.Reader
// it is used directly.
func NewReader(r io.Reader) *Reader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, 16<<10)
	}
	return &Reader{br: br, MaxBulk: DefaultMaxBulk, MaxElems: DefaultMaxElems, MaxDepth: DefaultMaxDepth}
}

func protoErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}

// Buffered returns the number of bytes already read from the stream but not
// yet decoded.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadValue reads one complete value.
func (r *Reader) ReadValue() (Value, error) {
	return r.read(0)
}

func (r *Reader) line() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErr("line longer than %d bytes", r.br.Size())
		}
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErr("line not terminated by CRLF")
	}
	return line[:len(line)-2], nil
}

func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, protoErr("empty integer")
	}
	// Fast path for the common case: up to 18 plain digits cannot overflow.
	if len(b) <= 18 {
		neg, d := false, b
		if d[0] == '-' {
			neg, d = true, d[1:]
		}
		var n int64
		ok := len(d) > 0
		for _, c := range d {
			if c < '0' || c > '9' {
				ok = false
				break
			}
			n = n*10 + int64(c-'0')
		}
		if ok {
			if neg {
				n = -n
			}
			return n, nil
		}
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, protoErr("bad integer %q", b)
	}
	return n, nil
}

func (r *Reader) read(depth int) (Value, error) {
	if depth > r.MaxDepth {
		return Value{}, protoErr("nesting deeper than %d", r.MaxDepth)
	}
	line, err := r.line()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, protoErr("empty line")
	}
	kind, body := Kind(line[0]), line[1:]
	switch kind {
	case KindSimple, KindError:
		return Value{Kind: kind, Str: string(body)}, nil
	case KindBigNumber:
		if !isBigNumber(body) {
			return Value{}, protoErr("bad big number %q", body)
		}
		return Value{Kind: kind, Str: string(body)}, nil
	case KindInt:
		n, err := parseInt(body)
		return Value{Kind: kind, Int: n}, err
	case KindNull:
		if len(body) != 0 {
			return Value{}, protoErr("bad null")
		}
		return Value{Kind: KindNull}, nil
	case KindBool:
		if len(body) != 1 || (body[0] != 't' && body[0] != 'f') {
			return Value{}, protoErr("bad boolean %q", body)
		}
		return Value{Kind: kind, Bool: body[0] == 't'}, nil
	case KindDouble:
		f, err := strconv.ParseFloat(string(body), 64)
		if err != nil || len(body) == 0 {
			return Value{}, protoErr("bad double %q", body)
		}
		return Value{Kind: kind, Float: f}, nil
	case KindBulk, KindBulkError, KindVerbatim:
		n, err := parseInt(body)
		if err != nil {
			return Value{}, err
		}
		if n == -1 && kind == KindBulk {
			return Value{Kind: KindNull}, nil
		}
		if n < 0 || n > int64(r.MaxBulk) {
			return Value{}, protoErr("bulk length %d out of range", n)
		}
		data, err := r.bulk(int(n))
		if err != nil {
			return Value{}, err
		}
		v := Value{Kind: kind, Str: data}
		if kind == KindVerbatim {
			if len(data) < 4 || data[3] != ':' {
				return Value{}, protoErr("bad verbatim string")
			}
			v.Format, v.Str = data[:3], data[4:]
		}
		return v, nil
	case KindArray, KindSet, KindPush, KindMap, kindAttribute:
		n, err := parseInt(body)
		if err != nil {
			return Value{}, err
		}
		if n == -1 && kind == KindArray {
			return Value{Kind: KindNull}, nil
		}
		if n < 0 || n > int64(r.MaxElems) {
			return Value{}, protoErr("aggregate length %d out of range", n)
		}
		count := int(n)
		if kind == KindMap || kind == kindAttribute {
			count *= 2
		}
		elems := make([]Value, 0, min(count, 64))
		for i := 0; i < count; i++ {
			e, err := r.read(depth + 1)
			if err != nil {
				return Value{}, err
			}
			elems = append(elems, e)
		}
		if kind == kindAttribute {
			return r.read(depth)
		}
		return Value{Kind: kind, Elems: elems}, nil
	}
	return Value{}, protoErr("unknown type byte %q", byte(kind))
}

// bulk reads n bytes plus CRLF. Lengths above 64 KiB are read in chunks so
// that a declared length alone cannot make the client allocate memory.
func (r *Reader) bulk(n int) (string, error) {
	var data []byte
	if n <= 64<<10 {
		data = make([]byte, n+2)
		if _, err := io.ReadFull(r.br, data); err != nil {
			return "", unexpected(err)
		}
	} else {
		var buf bytes.Buffer
		if _, err := io.CopyN(&buf, r.br, int64(n)+2); err != nil {
			return "", unexpected(err)
		}
		data = buf.Bytes()
	}
	if data[n] != '\r' || data[n+1] != '\n' {
		return "", protoErr("bulk string not terminated by CRLF")
	}
	return string(data[:n]), nil
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func isBigNumber(b []byte) bool {
	if len(b) > 0 && (b[0] == '-' || b[0] == '+') {
		b = b[1:]
	}
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// AppendCommand appends a command, encoded as an array of bulk strings, to
// dst.
func AppendCommand(dst []byte, args ...string) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = appendBulk(dst, '$', a)
	}
	return dst
}

func appendBulk(dst []byte, prefix byte, s string) []byte {
	dst = append(dst, prefix)
	dst = strconv.AppendInt(dst, int64(len(s)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

func appendLine(dst []byte, prefix byte, s string) []byte {
	dst = append(dst, prefix)
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

// AppendValue appends the RESP3 encoding of v to dst. Simple strings and
// errors must not contain CR or LF; use bulk kinds for arbitrary bytes.
func AppendValue(dst []byte, v Value) []byte {
	switch v.Kind {
	case KindSimple, KindError, KindBigNumber:
		return appendLine(dst, byte(v.Kind), v.Str)
	case KindBulk, KindBulkError:
		return appendBulk(dst, byte(v.Kind), v.Str)
	case KindVerbatim:
		format := v.Format
		if len(format) != 3 {
			format = "txt"
		}
		return appendBulk(dst, byte(v.Kind), format+":"+v.Str)
	case KindInt:
		return appendLine(dst, ':', strconv.FormatInt(v.Int, 10))
	case KindNull:
		return append(dst, '_', '\r', '\n')
	case KindBool:
		if v.Bool {
			return append(dst, '#', 't', '\r', '\n')
		}
		return append(dst, '#', 'f', '\r', '\n')
	case KindDouble:
		var s string
		switch {
		case math.IsInf(v.Float, 1):
			s = "inf"
		case math.IsInf(v.Float, -1):
			s = "-inf"
		case math.IsNaN(v.Float):
			s = "nan"
		default:
			s = strconv.FormatFloat(v.Float, 'g', -1, 64)
		}
		return appendLine(dst, ',', s)
	case KindArray, KindSet, KindPush, KindMap:
		n := len(v.Elems)
		if v.Kind == KindMap {
			n /= 2
		}
		dst = appendLine(dst, byte(v.Kind), strconv.Itoa(n))
		for _, e := range v.Elems[:n*(1+boolInt(v.Kind == KindMap))] {
			dst = AppendValue(dst, e)
		}
		return dst
	}
	return appendLine(dst, '-', fmt.Sprintf("ERR cannot encode kind %q", byte(v.Kind)))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
