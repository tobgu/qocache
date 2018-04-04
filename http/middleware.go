package http

import (
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
