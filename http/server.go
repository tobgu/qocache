package http

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/tobgu/qocache/config"
	"github.com/tobgu/qocache/qlog"
	"net/http"
	"os"
	"time"
)

type Server struct {
	http.Server
	c config.Config
}

func (s *Server) ListAndServeAsConfigured() error {
	if s.TLSConfig != nil {
		return s.ListenAndServeTLS(s.c.CertFile, s.c.KeyFile)
	}
	return s.ListenAndServe()
}

func newHTTPServer(c config.Config, port int, handler http.Handler) http.Server {
	return http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		ReadHeaderTimeout: time.Duration(c.ReadHeaderTimeout) * time.Second,
		ReadTimeout:       time.Duration(c.ReadTimeout) * time.Second,
		WriteTimeout:      time.Duration(c.WriteTimeout) * time.Second,
		Handler:           handler}
}

func NewServer(c config.Config, logger qlog.Logger) (*Server, error) {
	app, err := Application(c, logger)
	if err != nil {
		return nil, err
	}

	srv := &Server{Server: newHTTPServer(c, c.Port, app), c: c}
	if c.CertFile != "" {
		srv.TLSConfig, err = newTLSConfig(c, logger)
		if err != nil {
			return nil, err
		}
		srv.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler))
	}

	return srv, nil
}

func newTLSConfig(c config.Config, logger qlog.Logger) (*tls.Config, error) {
	logger.Printf("Using server side TLS")
	cfg := &tls.Config{
		MinVersion:       tls.VersionTLS12,
		CurvePreferences: []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}

	if c.CAFile != "" {
		logger.Printf("Verifying client certificates")
		clientCACert, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("unable to open CA cert: %v", err)
		}

		clientCertPool := x509.NewCertPool()
		clientCertPool.AppendCertsFromPEM(clientCACert)

		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = clientCertPool
	}

	return cfg, nil
}
