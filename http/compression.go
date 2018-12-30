package http

import (
	"bufio"
	"github.com/pierrec/lz4"
	"github.com/tobgu/qframe/errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

const lz4MaxBlockSize = 4 << 20

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

type lz4BlockWriter struct {
	http.ResponseWriter
	buf []byte
}

func (w *lz4BlockWriter) Write(b []byte) (int, error) {
	w.buf = append(w.buf, b...)
	return len(b), nil
}

func (w *lz4BlockWriter) Close() error {
	if len(w.buf) > 0 {
		var ht [1 << 16]int
		dst := make([]byte, lz4.CompressBlockBound(len(w.buf))+lz4BlockHeaderLen)
		bufLen, err := lz4.CompressBlock(w.buf, dst[lz4BlockHeaderLen:], ht[:])
		if err != nil {
			return err
		}

		// Store the len as a preamble to the data for now. This limits the uncompressed size to
		// 4 Gb but is interoperable with the Python lib. Could be changed to some out of band
		// transmission (an HTTP header?) to allow for bigger sizes and potentially interop with
		// other libraries.
		storeLen(dst, uint32(len(w.buf)))
		_, err = w.ResponseWriter.Write(dst[:bufLen+lz4BlockHeaderLen])
		return err
	}

	return nil
}

type lz4BlockReaderCloser struct {
	io.ReadCloser
	uncompressedBuf  []byte
	compressedBufLen int
	bytesRead        int
}

func NewLz4BlockReaderCloser(contentLength int, rc io.ReadCloser) *lz4BlockReaderCloser {
	return &lz4BlockReaderCloser{ReadCloser: rc, compressedBufLen: contentLength - lz4BlockHeaderLen}
}

func (r *lz4BlockReaderCloser) BufLen() (uint32, error) {
	var header [lz4BlockHeaderLen]byte
	bytesRead := 0
	for bytesRead < lz4BlockHeaderLen {
		n, err := r.ReadCloser.Read(header[bytesRead:])
		bytesRead += n
		if err != nil {
			return 0, err
		}
	}

	l := loadLen(header[:])
	return l, nil
}

func (r *lz4BlockReaderCloser) Read(b []byte) (int, error) {
	if r.uncompressedBuf == nil {
		l, err := r.BufLen()
		if err != nil {
			return 0, err
		}

		r.uncompressedBuf = make([]byte, int(l))
		compressedBuf := make([]byte, r.compressedBufLen)
		bytesRead := 0
		for bytesRead < r.compressedBufLen {
			n, err := r.ReadCloser.Read(compressedBuf[bytesRead:])
			bytesRead += n
			if err != nil && err != io.EOF {
				return 0, err
			}
		}

		size, err := lz4.UncompressBlock(compressedBuf, r.uncompressedBuf)
		if err != nil {
			return 0, err
		}

		if size != len(r.uncompressedBuf) {
			return 0, errors.New("Uncompress lz4", "Unexpected uncompressed size, was: %d, expected: %d", size, len(r.uncompressedBuf))
		}
	}

	l := copy(b, r.uncompressedBuf[r.bytesRead:])
	r.bytesRead += l
	var err error
	if r.bytesRead == len(r.uncompressedBuf) {
		err = io.EOF
	}

	return l, err
}

// Consistent with how the Python library stores uncompressed len
const lz4BlockHeaderLen = 4

func loadLen(c []byte) uint32 {
	return uint32(c[0]) | uint32(c[1])<<8 | uint32(c[2])<<16 | uint32(c[3])<<24
}

func storeLen(c []byte, l uint32) {
	c[0] = byte(l & 0xff)
	c[1] = byte((l >> 8) & 0xff)
	c[2] = byte((l >> 16) & 0xff)
	c[3] = byte((l >> 24) & 0xff)
}

func withLz4(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") == "lz4-frame" {
			r.Body = lz4ReaderCloserWrapper{Reader: lz4.NewReader(r.Body), Closer: r.Body}
		} else if r.Header.Get("Content-Encoding") == "lz4" {
			cl, err := strconv.Atoi(r.Header.Get("Content-Length"))
			if err != nil {
				// TODO: Don't panic here, return 400
				log.Fatalf("Invalid content length: %s", err)
			}
			r.Body = NewLz4BlockReaderCloser(cl, r.Body)
		}

		if strings.Contains(r.Header.Get("Accept-Encoding"), "lz4-frame") {
			w.Header().Set("Content-Encoding", "lz4-frame")

			// Want to buffer this to avoid calling CompressBlock on every write
			lz4Writer := lz4.NewWriter(w)
			bufferedWriter := bufio.NewWriterSize(lz4Writer, lz4MaxBlockSize)
			w = lz4WriterWrapper{ResponseWriter: w, lz4Writer: bufferedWriter}

			defer lz4Writer.Close()
			defer bufferedWriter.Flush()
		} else if strings.Contains(r.Header.Get("Accept-Encoding"), "lz4") {
			w.Header().Set("Content-Encoding", "lz4")
			blockWriter := &lz4BlockWriter{ResponseWriter: w}
			w = blockWriter
			defer blockWriter.Close()
		}

		next.ServeHTTP(w, r)
	}
}

// TODO: Better error handling
// TODO: Tests Go
// TODO: Tests Python interop
// TODO: General code cleanup
// TODO: Profile and optimize read CSV + write JSON
// TODO: Compare insert perf with qcache
