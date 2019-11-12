package http

import (
	"crypto/subtle"
	"github.com/tobgu/qocache/qlog"
	"net/http"
)

func withBasicAuth(logger qlog.Logger, confUser, confPassword string) middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			reqUser, reqPass, ok := r.BasicAuth()
			if !ok || subtle.ConstantTimeCompare([]byte(reqUser), []byte(confUser)) != 1 || subtle.ConstantTimeCompare([]byte(reqPass), []byte(confPassword)) != 1 {
				w.Header().Set("WWW-Authenticate", `Basic realm="Qocache"`)
				w.WriteHeader(401)
				if _, err := w.Write([]byte("Unauthorised")); err != nil {
					logger.Printf("Error writing unauthorized response")
				}

				return
			}

			next(w, r)
		}
	}
}
