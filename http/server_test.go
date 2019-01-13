package http_test

import (
	"crypto/tls"
	"crypto/x509"
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
	c := config.Config{CAFile: "../tls/ca.pem", CertFile: "../tls/host.pem", Port: 8888}
	logger := log.New(os.Stderr, "qocache-test", log.LstdFlags)
	srv := qhttp.NewServer(c, logger)

	go func() {
		err := srv.ListAndServeAsConfigured()
		if err != nil {
			logger.Printf("Logger finished with error: %v", err)
		}
	}()

	defer srv.Close()
	time.Sleep(100 * time.Millisecond)

	client := newClient(t)
	res, err := client.Get("https://localhost:8888/qcache/foo")
	if err != nil {
		t.Fatal(err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusNotFound {
		t.Errorf("Response code was %v; want 404", res.StatusCode)
	}
}

func newClient(t *testing.T) *http.Client {
	t.Helper()

	cert, err := tls.LoadX509KeyPair("../tls/host.pem", "../tls/host.pem")
	if err != nil {
		t.Fatalf("Unable to load cert: %v", err)
	}

	// Load our CA certificate
	clientCACert, err := ioutil.ReadFile("../tls/ca.pem")
	if err != nil {
		t.Fatalf("Unable to open cert: %v", err)
	}

	clientCertPool := x509.NewCertPool()
	clientCertPool.AppendCertsFromPEM(clientCACert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      clientCertPool,
	}
	tlsConfig.BuildNameToCertificate()

	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
}

// TODO: Negative tests
