package resp_test

import (
	"bufio"
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/victorcondackdev/ratelimit-go/internal/redistest"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

func BenchmarkReadValue(b *testing.B) {
	// The shape of a GCRA script reply for three keys.
	v := resp.Value{Kind: resp.KindArray}
	for i := 0; i < 14; i++ {
		v.Elems = append(v.Elems, resp.Value{Kind: resp.KindInt, Int: 1763812800000000 + int64(i)})
	}
	enc := resp.AppendValue(nil, v)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	r := bytes.NewReader(enc)
	br := bufio.NewReader(r)
	rd := resp.NewReader(br)
	for i := 0; i < b.N; i++ {
		r.Reset(enc)
		br.Reset(r)
		if _, err := rd.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRoundTrip measures full client round trips against the
// in-process fake server over loopback TCP: pooling, encoding, the
// network stack and parsing, but not Redis itself.
func BenchmarkRoundTrip(b *testing.B) {
	srv := startServer(b, redistest.Options{})
	c := newClient(b, resp.Options{Addr: srv.Addr()})
	ctx := context.Background()
	script := resp.NewScript(redistest.GCRAMarker + "\n")
	b.Run("PING", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := c.Do(ctx, "PING"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("EVALSHA-gcra", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := c.Run(ctx, script, []string{"k"}, "take", "1", "1000000000", "1"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("PING-parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := c.Do(ctx, "PING"); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("pipeline-16", func(b *testing.B) {
		cmds := make([][]string, 16)
		for i := range cmds {
			cmds[i] = []string{"GET", "k" + strconv.Itoa(i)}
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := c.Pipeline(ctx, cmds...); err != nil {
				b.Fatal(err)
			}
		}
	})
}
