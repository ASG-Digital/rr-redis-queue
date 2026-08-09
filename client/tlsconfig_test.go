package client

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTLSConfigIsOptional(t *testing.T) {
	config, err := tlsConfig(nil)
	if err != nil {
		t.Fatalf("unexpected nil TLS configuration error: %v", err)
	}
	if config != nil {
		t.Fatal("expected nil TLS configuration")
	}
}

func TestTLSConfigAllowsSystemRootsWithoutCustomRootCA(t *testing.T) {
	config, err := tlsConfig(&TLSConfig{})
	if err != nil {
		t.Fatalf("unexpected TLS configuration error: %v", err)
	}
	if config == nil {
		t.Fatal("expected TLS configuration")
	}
}

func TestTLSConfigRejectsIncompleteClientCertificatePair(t *testing.T) {
	_, err := tlsConfig(&TLSConfig{Cert: "client.crt"})
	if err == nil {
		t.Fatal("expected incomplete client certificate pair to fail")
	}
}

func TestTLSConfigRejectsInvalidRootCA(t *testing.T) {
	rootCAPath := filepath.Join(t.TempDir(), "invalid-root-ca.pem")
	if err := os.WriteFile(rootCAPath, []byte("not a PEM certificate"), 0o600); err != nil {
		t.Fatalf("write invalid root CA: %v", err)
	}

	_, err := tlsConfig(&TLSConfig{RootCa: rootCAPath})
	if err == nil {
		t.Fatal("expected invalid root CA to fail")
	}
}
