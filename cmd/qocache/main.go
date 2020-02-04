package main

import (
	"context"
	"github.com/tobgu/qocache/config"
	qhttp "github.com/tobgu/qocache/http"
	"github.com/tobgu/qocache/qlog"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	c, err := config.GetConfig()
	if err != nil {
		log.Fatalf("Configuration error: %s", err.Error())
	}

	logger := qlog.NewStdLogger(c.UseSyslog, c.LogDestination)
	srv, err := qhttp.NewServer(c, logger)
	if err != nil {
		logger.Fatalf("Server setup error: %s", err.Error())
	}

	logger.Printf("Starting qocache, MaxAge: %d, MaxSize: %d, Port: %d, GOMAXPROCS: %d\n", c.Age, c.Size, c.Port, runtime.GOMAXPROCS(0))
	idleConnsClosed := make(chan struct{})
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

	if c.HTTPStatusPort != 0 {
		qhttp.StartHTTPStatusEndpoint(c, logger)
	}

	err = srv.ListAndServeAsConfigured()
	if err == http.ErrServerClosed {
		logger.Printf("Starting server shutdown...")
	} else if err != nil {
		logger.Fatalf("HTTP server ListenAndServe: %v", err)
	}

	<-idleConnsClosed
	logger.Printf("Shutdown complete")
}
