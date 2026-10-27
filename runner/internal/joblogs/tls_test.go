package joblogs

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestReporterUsesMutualTLSAndReusesConnectionUntilClosed(t *testing.T) {
	caIdentity, caKey, caPEM := testCertificate(t, nil, nil, true)
	ca := caIdentity.Leaf
	serverCert, _, _ := testCertificate(t, ca, caKey, false)
	_, clientKey, clientPEM := testCertificate(t, ca, caKey, false)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var handshakes atomic.Int32
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert,
		VerifyConnection: func(tls.ConnectionState) error { handshakes.Add(1); return nil },
	})))
	requests := make(chan string, 2)
	runnerv1.RegisterJobLogServiceServer(server, logService{append: func(ctx context.Context, request *runnerv1.AppendJobLogRequest) (*runnerv1.AppendJobLogResponse, error) {
		p, ok := peer.FromContext(ctx)
		if !ok {
			t.Error("missing peer")
		} else if info, ok := p.AuthInfo.(credentials.TLSInfo); !ok || info.State.PeerCertificates[0].URIs[0].String() != "urn:zeroyaml:runner:runner" {
			t.Error("missing client certificate identity")
		}
		requests <- request.GetJobId()
		return &runnerv1.AppendJobLogResponse{}, nil
	}})
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	directory := t.TempDir()
	write := func(name string, data []byte) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	endpoint := Endpoint{Address: net.JoinHostPort("127.0.0.1", port), Certificate: write("client.pem", clientPEM), PrivateKey: write("client-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})), CA: write("ca.pem", caPEM)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reporter, err := NewReporter(endpoint, runneridentity.Identity{RunnerID: "runner", InstanceID: "instance", ProtocolVersion: "runner.v1"}, time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reporter.Close() })
	for _, job := range []string{"first-job", "second-job"} {
		if err := reporter.Append(context.Background(), Record{JobID: job, ObservedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		if got := <-requests; got != job {
			t.Fatalf("job = %q", got)
		}
	}
	if handshakes.Load() != 1 {
		t.Fatalf("connections = %d, want one", handshakes.Load())
	}
	if err := reporter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reporter.Append(context.Background(), Record{ObservedAt: time.Now()}); err == nil {
		t.Fatal("closed reporter accepted delivery")
	}
	// Both server trust and client trust are enforced independently.
	_, untrustedKey, untrustedPEM := testCertificate(t, nil, nil, true)
	untrustedDER, err := x509.MarshalPKCS8PrivateKey(untrustedKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []Endpoint{
		{Address: endpoint.Address, Certificate: write("untrusted-client.pem", untrustedPEM), PrivateKey: write("untrusted-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: untrustedDER})), CA: endpoint.CA},
		{Address: endpoint.Address, Certificate: endpoint.Certificate, PrivateKey: endpoint.PrivateKey, CA: write("wrong-ca.pem", clientPEM)},
	} {
		bad, err := NewReporter(invalid, runneridentity.Identity{}, time.Second, logger)
		if err == nil {
			if err := bad.Append(context.Background(), Record{ObservedAt: time.Now()}); err == nil {
				t.Error("invalid TLS configuration delivered output")
			}
			bad.Close()
		}
	}
}

func TestReporterDisablesOnlyEmptyConfigurationAndRejectsPartialTLS(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if reporter, err := NewReporter(Endpoint{}, runneridentity.Identity{}, time.Second, logger); reporter != nil || err != nil {
		t.Fatal("empty endpoint must disable delivery")
	}
	for _, endpoint := range []Endpoint{{Address: "localhost:50053"}, {CA: "ca.pem"}, {Address: "localhost:50053", Certificate: "missing", PrivateKey: "missing", CA: "missing"}} {
		if _, err := NewReporter(endpoint, runneridentity.Identity{}, time.Second, logger); err == nil {
			t.Fatal("partial or unreadable TLS configuration accepted")
		}
	}
}

func TestReporterRejectsMalformedLogAddressBeforeConnecting(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, address := range []string{"localhost", ":50053", "localhost:http", "localhost:0", "localhost:70000", "https://localhost:50053"} {
		t.Run(address, func(t *testing.T) {
			_, err := NewReporter(Endpoint{Address: address, Certificate: "missing", PrivateKey: "missing", CA: "missing"}, runneridentity.Identity{}, time.Second, logger)
			if err == nil || !strings.Contains(err.Error(), "ZEROYAML_JOB_LOGS_ADDRESS") {
				t.Fatalf("address %q: expected startup validation error, got %v", address, err)
			}
		})
	}
}

// testCertificate generates ephemeral certificates without checked-in key material.
func testCertificate(t *testing.T, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, authority bool) (tls.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "ephemeral-test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, IsCA: authority,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{{Scheme: "urn", Opaque: "zeroyaml:runner:runner"}}}
	if authority {
		template.KeyUsage |= x509.KeyUsageCertSign
	}
	if parent == nil {
		parent, parentKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: certificate}, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
