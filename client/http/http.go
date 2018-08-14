package client

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/config/newqf"
	"github.com/tobgu/qocache/config"
	"github.com/tobgu/qocache/query"
)

func buildURL(endpoint string, args ...string) (*url.URL, error) {
	var builder strings.Builder
	builder.WriteString(endpoint)
	if endpoint[len(endpoint)-1] != '/' {
		builder.WriteString("/")
	}
	builder.WriteString("qocache/")
	for i, arg := range args {
		if i+1 == len(args) {
			builder.WriteString(arg)
		} else {
			builder.WriteString(arg)
			builder.WriteString("/")
		}
	}
	return url.Parse(builder.String())
}

type HTTPClient struct {
	httpcli  *http.Client
	endpoint string
}

func (c HTTPClient) build(body io.Reader, method string, args ...string) (*http.Request, error) {
	u, err := buildURL(c.endpoint, args...)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(config.JsonEncoding.AcceptHeader())
	req.Header.Set(config.JsonEncoding.ContentTypeHeader())
	return req, nil
}

func (c HTTPClient) Create(name string, qf qframe.QFrame) error {
	reader, writer := io.Pipe()
	req, err := c.build(reader, http.MethodPost, "dataset", name)
	if err != nil {
		return err
	}
	go func() {
		writer.CloseWithError(qf.ToJSON(writer))
	}()
	_, err = c.httpcli.Do(req)
	return err
}

func (c HTTPClient) Read(name string, confs ...newqf.ConfigFunc) qframe.QFrame {
	req, err := c.build(nil, http.MethodGet, "dataset", name)
	if err != nil {
		return qframe.QFrame{Err: err}
	}
	resp, err := c.httpcli.Do(req)
	if err != nil {
		return qframe.QFrame{Err: err}
	}
	defer resp.Body.Close()
	return qframe.ReadJSON(resp.Body, confs...)
}

func (c HTTPClient) Query(name string, qry query.Query, confs ...newqf.ConfigFunc) qframe.QFrame {
	buf := bytes.NewBuffer(nil)
	err := json.NewEncoder(buf).Encode(qry)
	if err != nil {
		return qframe.QFrame{Err: err}
	}
	req, err := c.build(buf, http.MethodPost, "dataset", name, "q")
	if err != nil {
		return qframe.QFrame{Err: err}
	}
	resp, err := c.httpcli.Do(req)
	if err != nil {
		return qframe.QFrame{Err: err}
	}
	defer resp.Body.Close()
	return qframe.ReadJSON(resp.Body, confs...)
}

func New(funcs ...ConfigFunc) HTTPClient {
	cfg := NewConfig(funcs...)
	return HTTPClient{
		httpcli:  http.DefaultClient,
		endpoint: cfg.Endpoint,
	}
}
