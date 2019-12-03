package http

import (
	"bytes"
	"fmt"
	"github.com/pierrec/lz4"
	"github.com/tobgu/qocache/qlog"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type lz4ReaderCloserWrapper struct {
	io.Reader
	io.Closer
}

type lz4FrameWriter struct {
	http.ResponseWriter
	buf    *bytes.Buffer
	logger qlog.Logger
}

func (w *lz4FrameWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *lz4FrameWriter) Close() error {
	lz4Writer := lz4.NewWriter(w.ResponseWriter)
	lz4Writer.Size = uint64(w.buf.Len())

	_, err := lz4Writer.Write(w.buf.Bytes())
	if err != nil {
		w.logger.Printf("Error lz4 compressing: %v", err)
		return err
	}

	return lz4Writer.Close()
}

type lz4BlockWriter struct {
	http.ResponseWriter
	buf    *bytes.Buffer
	logger qlog.Logger
}

func (w *lz4BlockWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

func (w *lz4BlockWriter) Close() error {
	if w.buf.Len() > 0 {
		var ht [1 << 16]int
		dst := make([]byte, lz4.CompressBlockBound(w.buf.Len()+lz4BlockHeaderLen))
		bufLen, err := lz4.CompressBlock(w.buf.Bytes(), dst[lz4BlockHeaderLen:], ht[:])
		if err != nil {
			w.logger.Printf("Error lz4 block compressing: %v", err)
			return err
		}

		if bufLen == 0 || bufLen >= w.buf.Len() {
			// Content uncompressible, return as is
			w.logger.Printf("Uncompressible content!")
			w.ResponseWriter.Header().Del("Content-Encoding")
			_, err := w.ResponseWriter.Write(w.buf.Bytes())
			if err != nil {
				w.logger.Printf("Error writing lz4 block uncompressed: %v", err)
				return err
			}

			return nil
		}

		// Store the len as a preamble to the data for now. This limits the uncompressed size to
		// 4 Gb but is interoperable with the Python lib. Could be changed to some out of band
		// transmission (an HTTP header?) to allow for bigger sizes and potentially interop with
		// other libraries.
		storeLen(dst, uint32(w.buf.Len()))
		_, err = w.ResponseWriter.Write(dst[:bufLen+lz4BlockHeaderLen])
		if err != nil {
			w.logger.Printf("Error writing lz4 block compressed: %v", err)
			return err
		}

		return nil
	}

	return nil
}

type lz4BlockReaderCloser struct {
	io.ReadCloser
	uncompressedBuf  []byte
	compressedBufLen int
	bytesRead        int
}

func newLz4BlockReaderCloser(contentLength int, rc io.ReadCloser) *lz4BlockReaderCloser {
	return &lz4BlockReaderCloser{ReadCloser: rc, compressedBufLen: contentLength - lz4BlockHeaderLen}
}

func (r *lz4BlockReaderCloser) bufLen() (uint32, error) {
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
		l, err := r.bufLen()
		if err != nil {
			return 0, fmt.Errorf("LZ4 block read buffer len: %w", err)
		}

		r.uncompressedBuf = make([]byte, int(l))
		compressedBuf := make([]byte, r.compressedBufLen)
		bytesRead := 0
		for bytesRead < r.compressedBufLen {
			n, err := r.ReadCloser.Read(compressedBuf[bytesRead:])
			bytesRead += n
			if err != nil && err != io.EOF {
				return 0, fmt.Errorf("LZ4 block read buffer: %w", err)
			}
		}

		size, err := lz4.UncompressBlock(compressedBuf, r.uncompressedBuf)
		if err != nil {
			return 0, fmt.Errorf("LZ4 block uncompress: %w", err)
		}

		if size != len(r.uncompressedBuf) {
			return 0, fmt.Errorf("unexpected uncompressed size, was: %d, expected: %d", size, len(r.uncompressedBuf))
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

func withLz4(app *application) middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Content-Encoding") == "lz4-frame" {
				r.Body = lz4ReaderCloserWrapper{Reader: lz4.NewReader(r.Body), Closer: r.Body}
			} else if r.Header.Get("Content-Encoding") == "lz4" {
				cl, err := strconv.Atoi(r.Header.Get("Content-Length"))
				if err != nil || cl < 0 {
					app.badRequest(w, "Invalid content length: %s", r.Header.Get("Content-Length"))
					return
				}
				r.Body = newLz4BlockReaderCloser(cl, r.Body)
			}

			if strings.Contains(r.Header.Get("Accept-Encoding"), "lz4-frame") {
				w.Header().Set("Content-Encoding", "lz4-frame")
				frameWriter := &lz4FrameWriter{ResponseWriter: w, buf: &bytes.Buffer{}, logger: app.logger}
				w = frameWriter
				defer frameWriter.Close()
			} else if strings.Contains(r.Header.Get("Accept-Encoding"), "lz4") {
				w.Header().Set("Content-Encoding", "lz4")
				blockWriter := &lz4BlockWriter{ResponseWriter: w, buf: &bytes.Buffer{}, logger: app.logger}
				w = blockWriter
				defer blockWriter.Close()
			}

			next.ServeHTTP(w, r)
		}
	}
}
