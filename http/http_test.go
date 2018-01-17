package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/gorilla/mux"
	h "github.com/tobgu/qocache/http"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

type TestData struct {
	S string
	I int
	F float64
	B bool
}

type testCache struct {
	t   *testing.T
	app *mux.Router
}

func newTestCache(t *testing.T) *testCache {
	return &testCache{t: t, app: h.Application()}
}

func (c *testCache) insertDataset(key string, headers map[string]string, body io.Reader) *httptest.ResponseRecorder {
	req, err := http.NewRequest("POST", fmt.Sprintf("/qocache/dataset/%s", key), body)
	if err != nil {
		c.t.Fatal(err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	c.app.ServeHTTP(rr, req)
	return rr
}

func (c *testCache) insertJson(key string, headers map[string]string, input interface{}) {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(input)
	headers["Content-Type"] = "application/json"
	rr := c.insertDataset("FOO", headers, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		c.t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}
}

func (c *testCache) queryDataset(key string, headers map[string]string, q string) *httptest.ResponseRecorder {
	q = url.QueryEscape(q)
	req, err := http.NewRequest("GET", fmt.Sprintf("/qocache/dataset/%s?q=%s", key, q), nil)
	if err != nil {
		c.t.Fatal(err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	c.app.ServeHTTP(rr, req)
	return rr
}

func (c *testCache) queryJson(key string, headers map[string]string, q string, output interface{}) *httptest.ResponseRecorder {
	headers["Accept"] = "application/json"
	rr := c.queryDataset(key, headers, q)
	if rr.Code != http.StatusOK {
		return rr
	}

	contentType := rr.Header().Get("Content-Type")
	if rr.Header().Get("Content-Type") != "application/json" {
		c.t.Errorf("Wrong Content-type: %s", contentType)
	}

	err := json.NewDecoder(rr.Body).Decode(output)
	if err != nil {
		c.t.Fatal("Failed to unmarshal JSON")
	}

	return rr
}

func compareTestData(t *testing.T, actual, expected []TestData) {
	if len(actual) == len(expected) {
		if actual[0] != expected[0] {
			t.Errorf("Wrong record content: got %v want %v", actual, expected)
		}
	} else {
		t.Errorf("Wrong record count: got %v want %v", actual, expected)
	}
}

func TestBasicInsertAndQueryCsv(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	b := new(bytes.Buffer)
	gocsv.Marshal(input, b)
	rr := cache.insertDataset("FOO", map[string]string{"Content-Type": "text/csv"}, b)

	if rr.Code != http.StatusCreated {
		t.Errorf("Wrong status code: got %v want %v", rr.Code, http.StatusCreated)
	}

	rr = cache.queryDataset("FOO", map[string]string{"Accept": "text/csv"}, "{}")
	if rr.Code != http.StatusOK {
		t.Errorf("Wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}

	contentType := rr.Header().Get("Content-Type")
	if rr.Header().Get("Content-Type") != "text/csv" {
		t.Errorf("Wrong Content-type: %s", contentType)
	}

	var output []TestData
	err := gocsv.Unmarshal(rr.Body, &output)
	if err != nil {
		t.Fatal("Failed to unmarshal CSV")
	}

	compareTestData(t, output, input)
}

func TestBasicInsertAndGetJson(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	output := []TestData{}
	cache.insertJson("FOO", map[string]string{}, input)
	cache.queryJson("FOO", map[string]string{}, "{}", &output)
	compareTestData(t, output, input)
}

func TestFilter(t *testing.T) {
	// TODO: Test error cases
	cache := newTestCache(t)
	input := []TestData{{I: 123}, {I: 200}, {I: 223}}
	output := []TestData{}
	cases := []struct {
		filter   string
		expected []TestData
	}{
		{
			filter:   `[">", "I", 200]`,
			expected: []TestData{{I: 223}},
		},
		{
			filter:   `["not", [">", "I", 199]]`,
			expected: []TestData{{I: 123}},
		},
		{
			filter:   `["and", [">", "I", 199], ["or", [">", "I", 199], ["<", "I", 20]]]`,
			expected: []TestData{{I: 200}, {I: 223}},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Filter %s", tc.filter), func(t *testing.T) {
			cache.insertJson("FOO", map[string]string{}, input)
			rr := cache.queryJson("FOO", map[string]string{}, fmt.Sprintf(`{"where": %s}`, tc.filter), &output)
			if rr.Code != http.StatusOK {
				t.Errorf("Unexpected status code: %v, body: %s", rr.Code, rr.Body.String())
			}

			compareTestData(t, output, tc.expected)
		})
	}
}

func TestQueryNonExistingKey(t *testing.T) {
	cache := newTestCache(t)
	rr := cache.queryJson("FOO", map[string]string{}, "{}", nil)
	if rr.Code != http.StatusNotFound {
		t.Errorf("Unexpected status code: %v", rr.Code)
	}
}

func xTestBasicInsertAndQueryWithProjection(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	output := []TestData{}
	cache.insertJson("FOO", map[string]string{}, input)
	cache.queryJson("FOO", map[string]string{}, "{}", &output)
	compareTestData(t, output, []TestData{{S: "Foo"}})
}
