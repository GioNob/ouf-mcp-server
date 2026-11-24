package analysis

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type fixtureProofVerifier struct{ calls int }

func (v *fixtureProofVerifier) CheckReservation(_ context.Context, c ReservationCheck) (ReservationProof, error) {
	v.calls++
	return ReservationProof{Current: true, ChangeSeq: "1", ValidUntil: time.Now().Add(time.Second).Unix()}, nil
}

func TestReservationVerifiedGatewayAndExactPost(t *testing.T) { // MCP-50U-09
	now := time.Now()
	pk, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	cert, err := x509.CreateCertificate(rand.Reader, ca, ca, pk, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(cert)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(root)
	makeLeaf := func(uri string, serial int64) tls.Certificate {
		pub, priv, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		u, e := url.Parse(uri)
		if e != nil {
			t.Fatal(e)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{u}}
		der, e := x509.CreateCertificate(rand.Reader, leaf, root, pub, key)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	}
	v := &fixtureProofVerifier{}
	const gateway = "spiffe://fixture/gateway"
	srv := httptest.NewUnstartedServer(ReservationHandler(v, gateway))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS13}
	srv.StartTLS()
	defer srv.Close()
	check := ReservationCheck{Contract: ReservationContract, Nonce: "fixture", Workload: "fixture", AnalysisID: strings.Repeat("a", 64), IntentKey: strings.Repeat("b", 64),
		ArgumentsHash: strings.Repeat("c", 64), Generation: "1", Fence: "1", Operation: "source.file.preview",
		MandateID: "11111111-1111-4111-8111-111111111111", AttemptID: "22222222-2222-4222-8222-222222222222"}
	raw, _ := json.MarshalIndent(check, "", "  ")
	for _, tc := range []struct {
		uri    string
		status int
		serial int64
	}{{"spiffe://fixture/foreign", 403, 2}, {gateway, 200, 3}} {
		transport := srv.Client().Transport.(*http.Transport).Clone()
		transport.TLSClientConfig.Certificates = []tls.Certificate{makeLeaf(tc.uri, tc.serial)}
		client := &http.Client{Transport: transport}
		res, e := client.Post(srv.URL, "application/json", bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		if res.StatusCode != tc.status {
			t.Fatal(res.StatusCode)
		}
		if tc.status == 200 {
			var proof ReservationProof
			if json.NewDecoder(res.Body).Decode(&proof) != nil || proof.RequestHash != BytesDigest(raw) || !proof.Current || proof.RequestID != nil {
				t.Fatal(proof)
			}
		}
		res.Body.Close()
		transport.CloseIdleConnections()
	}
	if v.calls != 1 {
		t.Fatal("untrusted client reached reservation store")
	}
}
