package http

import (
	"bufio"
	"io"
	"net/http"
	"strings"

	"github.com/pierrec/lz4"
)

type lz4ReaderCloserWrapper struct {
	io.Reader
	io.Closer
}

type lz4WriterWrapper struct {
	http.ResponseWriter
	lz4Writer io.Writer
}

func (w lz4WriterWrapper) Write(p []byte) (int, error) {
	return w.lz4Writer.Write(p)
}

func withLz4(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") == "lz4" {
			r.Body = lz4ReaderCloserWrapper{Reader: lz4.NewReader(r.Body), Closer: r.Body}
		}

		if strings.Contains(r.Header.Get("Accept-Encoding"), "lz4") {
			w.Header().Set("Content-Encoding", "lz4")

			// Want to buffer this to avoid calling CompressBlock on every write
			const lz4MaxBlockSize = 4 << 20
			lz4Writer := bufio.NewWriterSize(lz4.NewWriter(w), lz4MaxBlockSize)
			w = lz4WriterWrapper{ResponseWriter: w, lz4Writer: lz4Writer}
			defer lz4Writer.Flush()
		}

		next.ServeHTTP(w, r)
	}
}
