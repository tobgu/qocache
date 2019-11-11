package http

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"runtime/debug"
)

func hash(s string) uint64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func withRecover(logger *log.Logger) middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				err := recover()
				if err != nil {
					stackString := string(debug.Stack())
					stackHash := hash(stackString)
					logger.Printf("Panic while executing %s %s: %v (code %d)\n%s", r.Method, r.URL.Path, err, stackHash, stackString)
					http.Error(w, fmt.Sprintf("Internal server error %d", stackHash), http.StatusInternalServerError)
				}
			}()

			next.ServeHTTP(w, r)
		}
	}
}
