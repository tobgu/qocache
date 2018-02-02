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
	t   testing.TB
	app *mux.Router
}

func newTestCache(t testing.TB) *testCache {
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

func TestBasicInsertAndQueryWithProjection(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	output := []TestData{}
	cache.insertJson("FOO", map[string]string{}, input)
	cache.queryJson("FOO", map[string]string{}, `{"select": ["S"]}`, &output)
	compareTestData(t, output, []TestData{{S: "Foo"}})
}

func TestQueryWithProjectionUnknownColumnError(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	output := []TestData{}
	cache.insertJson("FOO", map[string]string{}, input)
	rr := cache.queryJson("FOO", map[string]string{}, `{"select": ["NONEXISTING"]}`, &output)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("Unexpected status code: %v", rr.Code)
	}
}

func TestQueryWithOrderBy(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "A", I: 2}, {S: "A", I: 1}, {S: "B", I: 3}}

	cases := []struct {
		orderBy  string
		expected []TestData
	}{
		{
			orderBy:  `["S", "I"]`,
			expected: []TestData{{S: "A", I: 1}, {S: "A", I: 2}, {S: "B", I: 3}},
		},
		{
			orderBy:  `["-S", "I"]`,
			expected: []TestData{{S: "B", I: 3}, {S: "A", I: 1}, {S: "A", I: 2}},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Order by %s", tc.orderBy), func(t *testing.T) {
			output := []TestData{}
			cache.insertJson("FOO", map[string]string{}, input)
			cache.queryJson("FOO", map[string]string{}, fmt.Sprintf(`{"order_by": %s}`, tc.orderBy), &output)
			compareTestData(t, output, tc.expected)
		})
	}
}

func TestQueryWithSlice(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "A"}, {S: "B"}, {S: "C"}}

	cases := []struct {
		offset   int
		limit    int
		expected []TestData
	}{
		{offset: 0, limit: 0, expected: []TestData{{S: "A"}, {S: "B"}, {S: "C"}}},
		{offset: 0, limit: 5, expected: []TestData{{S: "A"}, {S: "B"}, {S: "C"}}},
		{offset: 1, limit: 0, expected: []TestData{{S: "B"}, {S: "C"}}},
		{offset: 0, limit: 2, expected: []TestData{{S: "A"}, {S: "B"}}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Slice %d %d", tc.offset, tc.limit), func(t *testing.T) {
			output := []TestData{}
			cache.insertJson("FOO", map[string]string{}, input)
			cache.queryJson("FOO", map[string]string{}, fmt.Sprintf(`{"offset": %d, "limit": %d}`, tc.offset, tc.limit), &output)
			compareTestData(t, output, tc.expected)
		})
	}
}

func TestQueryWithDistinct(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "A", I: 1}, {S: "A", I: 2}, {S: "A", I: 2}, {S: "C", I: 1}}
	expected := []TestData{{S: "A", I: 1}, {S: "A", I: 2}, {S: "C", I: 1}}
	output := []TestData{}

	cache.insertJson("FOO", map[string]string{}, input)
	rr := cache.queryJson("FOO", map[string]string{}, `{"distinct": ["S", "I"]}`, &output)
	if rr.Code != http.StatusOK {
		t.Errorf("Unexpected status code: %v", rr.Code)
	}

	compareTestData(t, output, expected)
}

func TestQueryWithGroupByWithoutAggregation(t *testing.T) {
	// The result in this case corresponds to a distinct over the selected columns
	cache := newTestCache(t)
	input := []TestData{{S: "C", I: 1}, {S: "A", I: 2}, {S: "A", I: 1}, {S: "A", I: 2}, {S: "C", I: 1}}
	expected := []TestData{{S: "A", I: 1}, {S: "A", I: 2}, {S: "C", I: 1}}
	output := []TestData{}

	cache.insertJson("FOO", map[string]string{}, input)
	rr := cache.queryJson("FOO", map[string]string{}, `{"group_by": ["S", "I"]}`, &output)
	if rr.Code != http.StatusOK {
		t.Errorf("Unexpected status code: %v, %s", rr.Code, rr.Body.String())
	}

	compareTestData(t, output, expected)
}

func TestQueryWithFrom(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{I: 1}, {I: 2}, {I: 3}}
	expected := []TestData{{I: 2}}
	output := []TestData{}

	cache.insertJson("FOO", map[string]string{}, input)
	rr := cache.queryJson("FOO", map[string]string{}, `{"where": [">", "I", 1], "from": {"where": ["<", "I", 3]}}`, &output)
	if rr.Code != http.StatusOK {
		t.Errorf("Unexpected status code: %v, %s", rr.Code, rr.Body.String())
	}

	compareTestData(t, output, expected)
}

// TODO
// - Aggregation/group by
// - Types and enums
// - Meta data response headers
// - Compression
// - Advanced select, applying functions, creating new columns, aliasing
// - Standin columns
// - Sub queries
// - Statistics
