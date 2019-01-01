package main

import (
	"fmt"
	"github.com/tobgu/qocache/config"
	qhttp "github.com/tobgu/qocache/http"
	"log"
	"net/http"
	"os/signal"
	"os"
	"syscall"
	"context"
)

type Config struct {
	Foo string `mapstructure:"foo"`
}

func main() {
	c, err := config.GetConfig()
	if err != nil {
		log.Fatalf("Configuration error: %s", err.Error())
	}

	log.Printf("Starting qocache, MaxAge: %d, MaxSize: %d, Port: %d, \n", c.Age, c.Size, c.Port)

	app := qhttp.Application(c)
	srv := &http.Server{Addr: fmt.Sprintf(":%d", c.Port), Handler: app}
	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		signal.Notify(sigint, syscall.SIGTERM)
		<-sigint

		// We received an interrupt signal, shut down.
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
		close(idleConnsClosed)
	}()

	err = srv.ListenAndServe()
	if err == http.ErrServerClosed {
		log.Printf("Starting server shutdown...")
	} else if err != nil {
		log.Printf("HTTP server ListenAndServe: %v", err)
		return
	}

	<-idleConnsClosed
	log.Printf("Shutdown complete")
}
