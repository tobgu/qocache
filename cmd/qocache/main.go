package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/tobgu/qocache/config"
	qhttp "github.com/tobgu/qocache/http"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	c, err := config.GetConfig()
	logger := log.New(os.Stderr, "qocache", log.LstdFlags)
	if err != nil {
		logger.Fatalf("Configuration error: %s", err.Error())
	}

	logger.Printf("Starting qocache, MaxAge: %d, MaxSize: %d, Port: %d, \n", c.Age, c.Size, c.Port)

	app := qhttp.Application(c, logger)
	srv := &http.Server{Addr: fmt.Sprintf(":%d", c.Port), Handler: app}
	idleConnsClosed := make(chan struct{})

	if c.CertFile != "" {
		srv.TLSConfig = newTLSConfig(c, logger)
		srv.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0)
		err = srv.ListenAndServeTLS(c.CertFile, c.CertFile)
	} else {
		err = srv.ListenAndServe()
	}

	if err == http.ErrServerClosed {
		logger.Printf("Starting server shutdown...")
	} else if err != nil {
		logger.Fatalf("HTTP server ListenAndServe: %v", err)
	}

	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		signal.Notify(sigint, syscall.SIGTERM)
		<-sigint

		// We received an interrupt signal, shut down.
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.Printf("HTTP server Shutdown: %v", err)
		}
		close(idleConnsClosed)
	}()

	<-idleConnsClosed
	logger.Printf("Shutdown complete")
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
