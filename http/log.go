package http

import (
	"log"
	"net/http"
	"time"
)

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.code = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func withRequestLog(app *application) middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			recorder := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
			next.ServeHTTP(recorder, r)
			log.Printf("%s %s %d %d ms %s %s", r.Method, r.URL.Path, recorder.code, time.Since(start)/time.Millisecond, r.Host, r.RemoteAddr)
		}
	}
}
