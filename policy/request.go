package policy

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strconv"
	"strings"
	"unicode"
)

// Dimension is an attribute of a request that a rule can key on.
type Dimension string

// The dimensions found in payment APIs.
const (
	// APIKey is the client credential. It is pseudonymized in store keys
	// and masked in logs, since a store key is not a place for secrets.
	APIKey Dimension = "api_key"
	// Merchant is the account the request acts for.
	Merchant Dimension = "merchant"
	// Payer is the payer's document (CPF or CNPJ). It is personal data
	// under the LGPD: pseudonymized in store keys and masked in logs.
	Payer Dimension = "payer"
	// Endpoint is the operation, for example "POST /pix/transfers".
	Endpoint Dimension = "endpoint"
	// IP is the client address.
	IP Dimension = "ip"
)

func (d Dimension) valid() bool {
	switch d {
	case APIKey, Merchant, Payer, Endpoint, IP:
		return true
	}
	return false
}

// sensitive reports whether values of d must not appear in clear in keys.
func (d Dimension) sensitive() bool { return d == APIKey || d == Payer }

// Request describes one API call to be rate limited.
type Request struct {
	APIKey   string
	Merchant string
	// Payer is the payer's CPF or CNPJ, formatted or not.
	Payer    string
	Endpoint string
	IP       string
	// Plan selects a tier from Config.Plans. Empty means the default plan.
	Plan string
	// Cost is how many units the request consumes from weighted rules. A
	// Pix transfer costs 1; a bulk payout of N transfers costs N. Zero or
	// negative means 1.
	Cost int64
}

// Value returns the request's value for a dimension.
func (r Request) Value(d Dimension) string {
	switch d {
	case APIKey:
		return r.APIKey
	case Merchant:
		return r.Merchant
	case Payer:
		return r.Payer
	case Endpoint:
		return r.Endpoint
	case IP:
		return r.IP
	}
	return ""
}

// LogValue implements slog.LogValuer so that logging a Request never leaks
// the API key or the payer's document.
func (r Request) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, 7)
	add := func(k, v string) {
		if v != "" {
			attrs = append(attrs, slog.String(k, v))
		}
	}
	add("api_key", MaskSecret(r.APIKey))
	add("merchant", r.Merchant)
	add("payer", MaskDocument(r.Payer))
	add("endpoint", r.Endpoint)
	add("ip", r.IP)
	add("plan", r.Plan)
	attrs = append(attrs, slog.Int64("cost", r.cost()))
	return slog.GroupValue(attrs...)
}

func (r Request) cost() int64 {
	if r.Cost <= 0 {
		return 1
	}
	return r.Cost
}

// NormalizeDocument strips punctuation and spaces from a CPF or CNPJ and
// upper-cases letters (alphanumeric CNPJs, issued from July 2026, contain
// letters), so "123.456.789-09" and "12345678909" are the same payer.
func NormalizeDocument(doc string) string {
	var b strings.Builder
	b.Grow(len(doc))
	for _, c := range doc {
		if unicode.IsDigit(c) || unicode.IsLetter(c) {
			b.WriteRune(unicode.ToUpper(c))
		}
	}
	return b.String()
}

// MaskDocument masks a CPF or CNPJ for logs. CPFs follow the convention of
// Brazilian government services (***.456.789-**); CNPJs keep the middle
// digits (**.345.678/****-**). Anything else keeps only its last two
// characters.
func MaskDocument(doc string) string {
	n := NormalizeDocument(doc)
	switch len(n) {
	case 0:
		return ""
	case 11:
		return "***." + n[3:6] + "." + n[6:9] + "-**"
	case 14:
		return "**." + n[2:5] + "." + n[5:8] + "/****-**"
	}
	if len(n) <= 2 {
		return strings.Repeat("*", len(n))
	}
	return strings.Repeat("*", len(n)-2) + n[len(n)-2:]
}

// MaskSecret keeps the first four characters of a credential, enough to
// tell keys apart in logs, and the length.
func MaskSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "…(" + strconv.Itoa(len(s)) + ")"
}

// pseudonymize returns a stable, non-reversible identifier for a sensitive
// value. With a secret it is an HMAC-SHA-256, which resists guessing even
// though the CPF space (10^9) is small enough to enumerate; without one it
// is a plain SHA-256, which only hides values from casual inspection.
func pseudonymize(secret []byte, d Dimension, v string) string {
	if d == Payer {
		v = NormalizeDocument(v)
	}
	var sum []byte
	if len(secret) > 0 {
		m := hmac.New(sha256.New, secret)
		m.Write([]byte(d))
		m.Write([]byte{0})
		m.Write([]byte(v))
		sum = m.Sum(nil)
	} else {
		s := sha256.Sum256([]byte(string(d) + "\x00" + v))
		sum = s[:]
	}
	return hex.EncodeToString(sum[:16])
}

var keyEscaper = strings.NewReplacer("%", "%25", ":", "%3A", "{", "%7B", "}", "%7D", "=", "%3D")
