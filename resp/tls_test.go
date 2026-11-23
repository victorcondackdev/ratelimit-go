package resp_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/victorcondackdev/ratelimit-go/internal/redistest"
	"github.com/victorcondackdev/ratelimit-go/resp"
)

func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "redis.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

func TestClientTLS(t *testing.T) {
	cert, pool := selfSigned(t)
	srv := startServer(t, redistest.Options{TLS: &tls.Config{Certificates: []tls.Certificate{cert}}})
	opts, err := resp.ParseURL("rediss://" + srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig.RootCAs = pool
	c := newClient(t, opts)
	if v, err := c.Do(context.Background(), "PING"); err != nil || v.Str != "PONG" {
		t.Fatalf("PING over TLS: %v %v", v, err)
	}
	// An untrusted certificate fails the handshake.
	opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	bad := newClient(t, opts)
	if _, err := bad.Do(context.Background(), "PING"); err == nil || !strings.Contains(err.Error(), "tls handshake") {
		t.Fatalf("untrusted cert: %v", err)
	}
}
