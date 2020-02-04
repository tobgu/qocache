package http

import (
	"github.com/gorilla/mux"
	"github.com/tobgu/qocache/config"
	"github.com/tobgu/qocache/qlog"
	"net/http"
)

func status(logger qlog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("OK"))
		if err != nil {
			logger.Printf("Error: write /status: %v", err)
		}
	}
}

func StartHTTPStatusEndpoint(c config.Config, logger qlog.Logger) {
	r := mux.NewRouter()
	r.HandleFunc("/status", status(logger)).Methods("GET")
	srv := newHTTPServer(c, c.HTTPStatusPort, r)
	go func() {
		logger.Printf("Starting HTTP status endpoint")
		if err := srv.ListenAndServe(); err != nil {
			logger.Printf("Error starting HTTP status endpoint: %v", err)
		}
	}()
}
