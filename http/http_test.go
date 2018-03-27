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
	"strings"
	"testing"
)

func assertTrue(t *testing.T, val bool) {
	t.Helper()
	if !val {
		t.Errorf("Expected true, was false!")
	}
}

func assertEqualStrings(t *testing.T, expected, actual string) {
	t.Helper()
	if expected != actual {
		t.Errorf("%s != %s", expected, actual)
	}
}

func assertEqualInts(t *testing.T, expected, actual int) {
	t.Helper()
	if expected != actual {
		t.Errorf("%d != %d", expected, actual)
	}
}

type TestData struct {
	S  string
	I  int
	F  float64
	B  bool
	I2 int
	I3 int
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

func (c *testCache) insertCsv(key string, headers map[string]string, input interface{}) {
	if headers == nil {
		headers = make(map[string]string)
	}

	headers["Content-Type"] = "text/csv"

	b := new(bytes.Buffer)
	gocsv.Marshal(input, b)
	rr := c.insertDataset(key, headers, b)

	if rr.Code != http.StatusCreated {
		c.t.Errorf("handler returned wrong status code: got %v want %v: %s", rr.Code, http.StatusCreated, rr.Body.String())
	}
}

func (c *testCache) insertJson(key string, headers map[string]string, input interface{}) {
	if headers == nil {
		headers = make(map[string]string)
	}

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(input)
	headers["Content-Type"] = "application/json"
	rr := c.insertDataset("FOO", headers, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		c.t.Errorf("handler returned wrong status code: got %v want %v: %s", rr.Code, http.StatusCreated, rr.Body.String())
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
	if headers == nil {
		headers = make(map[string]string)
	}

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
		c.t.Fatalf("Failed to unmarshal JSON: %s", err.Error())
	}

	return rr
}

func compareTestData(t *testing.T, actual, expected []TestData) {
	if len(actual) == len(expected) {
		for i := range actual {
			if actual[i] != expected[i] {
				t.Errorf("Wrong record content in position %d: got %v want %v", i, actual, expected)
			}
		}
	} else {
		t.Errorf("Wrong record count: got %v want %v", actual, expected)
	}
}

func TestBasicInsertAndQueryCsv(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	cache.insertCsv("FOO", map[string]string{"Content-Type": "text/csv"}, input)

	rr := cache.queryDataset("FOO", map[string]string{"Accept": "text/csv"}, "{}")
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

func toKeyVals(kvs []keyValProperty, format string) string {
	if format == "json" {
		m := make(map[string]string)
		for _, kv := range kvs {
			m[kv.key] = kv.value
		}
		bResult, err := json.Marshal(m)
		if err != nil {
			panic(err)
		}
		return string(bResult)
	}

	s := make([]string, len(kvs))
	for i, kv := range kvs {
		s[i] = fmt.Sprintf("%s=%s", kv.key, kv.value)
	}
	return strings.Join(s, ";")
}

type keyValProperty struct {
	key      string
	value    string
	expected string
}

func TestInsertCsvWithTypes(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}

	cases := [][]keyValProperty{
		{{"I", "string", "123"}},
		{{"F", "string", "1.5"}, {"B", "string", "true"}},
		{{"I", "enum", "123"}},
		{{"F", "enum", "1.5"}, {"B", "enum", "true"}},
	}

	for _, format := range []string{"kv", "json"} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("Types %s", toKeyVals(tc, format)), func(t *testing.T) {
				cache.insertCsv("FOO", map[string]string{"X-QCache-types": toKeyVals(tc, format)}, input)
				output := make([]map[string]interface{}, 0)
				cache.queryJson("FOO", nil, "{}", &output)
				assertEqualInts(t, 1, len(output))
				for _, kv := range tc {
					sVal, ok := output[0][kv.key].(string)
					assertTrue(t, ok)
					assertEqualStrings(t, kv.expected, sVal)
				}
			})
		}
	}
}

func TestStandinColumns(t *testing.T) {
	// X-QCache-stand-in-columns: foo=10;bar=baz
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}

	cases := [][]keyValProperty{
		// No stand in, value exists
		{{"S", "'Bar'", "Foo"}},

		// Stand in from string constant to X and from S to Y
		{{"X", "'Bar'", "Bar"}, {"Y", "S", "Foo"}},
	}

	for _, format := range []string{"kv", "json"} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("Types %s", toKeyVals(tc, format)), func(t *testing.T) {
				cache.insertCsv("FOO", map[string]string{"X-QCache-stand-in-columns": toKeyVals(tc, format)}, input)
				output := make([]map[string]interface{}, 0)
				cache.queryJson("FOO", nil, "{}", &output)
				assertEqualInts(t, 1, len(output))
				for _, kv := range tc {
					sVal, ok := output[0][kv.key].(string)
					assertTrue(t, ok)
					assertEqualStrings(t, kv.expected, sVal)
				}
			})
		}
	}

}

func TestFilter(t *testing.T) {
	// TODO: Test error cases
	cache := newTestCache(t)
	input := []TestData{{I: 123, I2: 124}, {I: 200, I2: 124}, {I: 223, I2: 124}}
	output := []TestData{}
	cases := []struct {
		filter   string
		expected []TestData
	}{
		{
			filter:   `[">", "I", 200]`,
			expected: []TestData{{I: 223, I2: 124}},
		},
		{
			filter:   `["!", [">", "I", 199]]`,
			expected: []TestData{{I: 123, I2: 124}},
		},
		{
			filter:   `["&", [">", "I", 199], ["|", [">", "I", 199], ["<", "I", 20]]]`,
			expected: []TestData{{I: 200, I2: 124}, {I: 223, I2: 124}},
		},
		{
			filter:   `["<", "I", "I2"]`,
			expected: []TestData{{I: 123, I2: 124}},
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

func TestQuery(t *testing.T) {
	// Various basic query test cases following the same pattern
	cases := []struct {
		name         string
		input        []TestData
		query        string
		expected     []TestData
		expectedCode int
		headers      map[string]string
	}{
		{
			name:     "Basic insert and query with empty query",
			input:    []TestData{{S: "Foo", I: 123, F: 1.5, B: true}},
			query:    `{}`,
			expected: []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}},
		{
			name:     "Basic project",
			input:    []TestData{{S: "Foo", I: 123, F: 1.5, B: true}},
			query:    `{"select": ["S"]}`,
			expected: []TestData{{S: "Foo"}}},
		{
			name:         "Projection with unknown column",
			input:        []TestData{{S: "Foo", I: 123, F: 1.5, B: true}},
			query:        `{"select": ["NONEXISTING"]}`,
			expectedCode: http.StatusBadRequest},
		{
			name:     "Distinct",
			input:    []TestData{{S: "A", I: 1}, {S: "A", I: 2}, {S: "A", I: 2}, {S: "C", I: 1}},
			query:    `{"distinct": ["S", "I"]}`,
			expected: []TestData{{S: "A", I: 1}, {S: "A", I: 2}, {S: "C", I: 1}}},
		{
			name:     "Group by without aggregation",
			input:    []TestData{{S: "C", I: 1}, {S: "A", I: 2}, {S: "A", I: 1}, {S: "A", I: 2}, {S: "C", I: 1}},
			query:    `{"group_by": ["S", "I"], "order_by": ["S", "I"]}`,
			expected: []TestData{{S: "A", I: 1}, {S: "A", I: 2}, {S: "C", I: 1}}},
		{
			name:     "Aggregation with group by",
			input:    []TestData{{S: "A", I: 2}, {S: "C", I: 1}, {S: "A", I: 1}, {S: "A", I: 2}},
			query:    `{"select": ["S", ["sum", "I"]], "group_by": ["S"], "order_by": ["S"]}`,
			expected: []TestData{{S: "A", I: 5}, {S: "C", I: 1}}},
		{
			name:     "Aggregation without group by",
			input:    []TestData{{S: "A", I: 2}, {S: "C", I: 1}, {S: "A", I: 1}, {S: "A", I: 2}},
			query:    `{"select": [["sum", "I"]]}`,
			expected: []TestData{{I: 6}}},
		{
			name:     "Simple column alias",
			input:    []TestData{{I: 1}, {I: 2}},
			query:    `{"select": ["I", ["=", "I2", "I"]]}`,
			expected: []TestData{{I: 1, I2: 1}, {I: 2, I2: 2}}},
		{
			name:     "Simple constant alias",
			input:    []TestData{{I: 1}, {I: 2}},
			query:    `{"select": ["I", ["=", "I2", 22]]}`,
			expected: []TestData{{I: 1, I2: 22}, {I: 2, I2: 22}}},
		{
			name:     "Alias with operation",
			input:    []TestData{{I: 1, I2: 10}, {I: 2, I2: 20}},
			query:    `{"select": ["I", ["=", "I3", ["+", "I2", "I"]]]}`,
			expected: []TestData{{I: 1, I3: 11}, {I: 2, I3: 22}}},
		{
			name:     "Sub query",
			input:    []TestData{{I: 1}, {I: 2}, {I: 3}},
			query:    `{"where": [">", "I", 1], "from": {"where": ["<", "I", 3]}}`,
			expected: []TestData{{I: 2}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := newTestCache(t)
			cache.insertJson("FOO", tc.headers, tc.input)
			output := make([]TestData, 0)
			rr := cache.queryJson("FOO", map[string]string{}, tc.query, &output)

			// Assume OK if code left out from test definition
			if tc.expectedCode == 0 {
				tc.expectedCode = http.StatusOK
			}

			if rr.Code != tc.expectedCode {
				t.Errorf("Unexpected status code: %v, %s", rr.Code, rr.Body.String())
			}

			if tc.expectedCode == http.StatusOK {
				compareTestData(t, output, tc.expected)
			}
		})
	}
}

/* TODO
- Enum range specifications
- Meta data response headers (total length before slicing for example)
  X-QCache-unsliced-length
- Compression
- Standin columns
  X-QCache-stand-in-columns: foo=10;bar=baz
- Statistics, including memory stats
- In filter with sub query
- Viper for configuration management?
- logrus for logging?
*/
