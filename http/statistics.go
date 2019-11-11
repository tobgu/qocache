package http

import (
	"github.com/tobgu/qocache/statistics"
	"net/http"
)

func withStatistics(stats *statistics.Statistics) middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := stats.Init(r.Context())
			next.ServeHTTP(w, r.WithContext(ctx))
			stats.Register(ctx)
		}
	}
}
