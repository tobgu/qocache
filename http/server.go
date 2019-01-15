package http

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/tobgu/qocache/config"
	"io/ioutil"
	"log"
	"net/http"
)

type Server struct {
	http.Server
	c config.Config
}

func (s *Server) ListAndServeAsConfigured() error {
	if s.TLSConfig != nil {
		return s.ListenAndServeTLS(s.c.CertFile, s.c.CertFile)
	}
	return s.ListenAndServe()
}

func NewServer(c config.Config, logger *log.Logger) (*Server, error) {
	app, err := Application(c, logger)
	if err != nil {
		return nil, err
	}
	srv := &Server{
		Server: http.Server{Addr: fmt.Sprintf(":%d", c.Port), Handler: app},
		c:      c,
	}

	if c.CertFile != "" {
		srv.TLSConfig = newTLSConfig(c, logger)
		srv.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0)
	}

	return srv, nil
}

func newTLSConfig(c config.Config, logger *log.Logger) *tls.Config {
	logger.Printf("Using server side TLS")
	cfg := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}

	if c.CAFile != "" {
		logger.Printf("Verifying client certificates")
		clientCACert, err := ioutil.ReadFile(c.CAFile)
		if err != nil {
			logger.Fatal("Unable to open CA cert", err)
		}

		clientCertPool := x509.NewCertPool()
		clientCertPool.AppendCertsFromPEM(clientCACert)

		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = clientCertPool
	}

	return cfg
}
