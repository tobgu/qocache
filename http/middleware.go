package http

import (
	"log"
	"net/http"
)

// Inspired by simple middleware here: https://gist.github.com/gbbr/85448fc35bf1a008363a4f5da469fa4d

type middleware func(http.HandlerFunc) http.HandlerFunc

func chainMiddleware(mw ...middleware) middleware {
	return func(final http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			last := final
			for i := len(mw) - 1; i >= 0; i-- {
				last = mw[i](last)
			}
			last(w, r)
		}
	}
}

func withLogging(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Logged connection from %s", r.RemoteAddr)
		next.ServeHTTP(w, r)
	}
}

func withTracing(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Tracing request for %s", r.RequestURI)
		next.ServeHTTP(w, r)
	}
}

// TODO:
// - Compression and decompression using LZ4 (https://github.com/pierrec/lz4)
// - Logging?
// - statistics, query count, query duration, etc? Or should that rather be done in the specific functions?
