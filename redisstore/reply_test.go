package redisstore

import (
	"errors"
	"testing"

	"github.com/victorcondackdev/ratelimit-go/limiter"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

func ints(ns ...int64) resp.Value {
	v := resp.Value{Kind: resp.KindArray}
	for _, n := range ns {
		v.Elems = append(v.Elems, resp.Value{Kind: resp.KindInt, Int: n})
	}
	return v
}

func TestParseReply(t *testing.T) {
	reqs := []limiter.Request{{Key: "k", Limit: limiter.PerSecond(10), Cost: 1}}
	res, err := parseReply(ints(1, 0, 1, 9, 0, 100000), reqs)
	if err != nil || !res[0].Allowed || res[0].Remaining != 9 || res[0].ResetAfter.Microseconds() != 100000 {
		t.Fatalf("%+v %v", res, err)
	}
	// RESP3 servers may send doubles; integral ones are accepted.
	v := ints(1, 0, 1, 9, 0, 100000)
	v.Elems[3] = resp.Value{Kind: resp.KindDouble, Float: 9}
	if _, err := parseReply(v, reqs); err != nil {
		t.Fatal(err)
	}
	bad := []resp.Value{
		{Kind: resp.KindInt, Int: 1},
		ints(1, 0, 1),
		{Kind: resp.KindArray, Elems: []resp.Value{{Kind: resp.KindBulk, Str: "x"}, {}, {}, {}, {}, {}}},
	}
	for _, b := range bad {
		if _, err := parseReply(b, reqs); !errors.Is(err, ErrBadReply) {
			t.Errorf("parseReply(%v) = %v", b, err)
		}
	}
}
