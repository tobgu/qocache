package http_test

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/stretchr/testify/assert"
	"github.com/tobgu/qocache/config"
	qhttp "github.com/tobgu/qocache/http"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestTLSServer(t *testing.T) {
	// Test connecting to TLS server with valid client side cert.
	c := config.Config{CAFile: "../tls/ca.pem", CertFile: "../tls/host.pem", KeyFile: "../tls/host-key.pem", Port: 8888}
	logger := log.New(os.Stderr, "qocache-test", log.LstdFlags)
	srv, err := qhttp.NewServer(c, logger)
	assert.Nil(t, err)

	go func() {
		err := srv.ListAndServeAsConfigured()
		if err != nil {
			logger.Printf("Logger finished with error: %v", err)
		}
	}()

	defer srv.Close()
	time.Sleep(100 * time.Millisecond)

	t.Run("Successful mutual TLS", func(t *testing.T) {
		client := newClient(t, true)
		res, err := client.Get("https://localhost:8888/qcache/status")
		if err != nil {
			t.Fatal(err)
		}

		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Errorf("Response code was %v; want 200", res.StatusCode)
		}
	})

	t.Run("Unsuccessful mutual TLS, no client cert", func(t *testing.T) {
		client := newClient(t, false)
		_, err := client.Get("https://localhost:8888/qcache/status")
		assert.Error(t, err)
	})
}

func newClient(t *testing.T, withClientCert bool) *http.Client {
	t.Helper()

	var clientCerts []tls.Certificate
	if withClientCert {
		cert, err := tls.LoadX509KeyPair("../tls/host.pem", "../tls/host-key.pem")
		if err != nil {
			t.Fatalf("Unable to load cert: %v", err)
		}
		clientCerts = append(clientCerts, cert)
	}

	// Load our CA certificate
	clientCACert, err := ioutil.ReadFile("../tls/ca.pem")
	if err != nil {
		t.Fatalf("Unable to open cert: %v", err)
	}

	clientCertPool := x509.NewCertPool()
	clientCertPool.AppendCertsFromPEM(clientCACert)

	tlsConfig := &tls.Config{
		Certificates: clientCerts,
		RootCAs:      clientCertPool,
	}
	tlsConfig.BuildNameToCertificate()

	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
}
