package http

import (
	"net/http"

	"go.uber.org/zap"
)

// Returns a middleware func as a closure with a Zap logger
func getZapMiddleware(log *zap.SugaredLogger) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			log.Infof("method=%s length=%d url=%s useragent=%s ", r.Method, r.ContentLength, r.URL.String(), r.UserAgent())
			next.ServeHTTP(w, r)
		}
	}
}
