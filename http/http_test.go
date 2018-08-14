package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/gocarina/gocsv"
	"github.com/gorilla/mux"
	"github.com/pierrec/lz4"

	"github.com/tobgu/qocache/config"
	h "github.com/tobgu/qocache/http"
	"github.com/tobgu/qocache/statistics"
)

func assertTrue(t *testing.T, val bool) {
	t.Helper()
	if !val {
		t.Errorf("Expected true, was false!")
	}
}

func assertEqual(t *testing.T, expected, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("%v != %v", expected, actual)
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
	return &testCache{t: t, app: h.Application(config.Config{Size: 1000000000, StatisticsBufferSize: 1000})}
}

func (c *testCache) insertDataset(key string, headers map[string]string, body io.Reader) *httptest.ResponseRecorder {
	if headers["Content-Encoding"] == "lz4" {
		pReader, pWriter := io.Pipe()
		lz4Writer := lz4.NewWriter(pWriter)
		origBody := body
		body = pReader

		go func() {
			defer pWriter.Close()
			lz4Writer.ReadFrom(origBody)
		}()
	}

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
	c.insertCsvWithExpectedCode(key, headers, input, http.StatusCreated)
}

func (c *testCache) insertCsvWithExpectedCode(key string, headers map[string]string, input interface{}, code int) {
	if headers == nil {
		headers = make(map[string]string)
	}

	if _, ok := headers["Content-Type"]; !ok {
		headers["Content-Type"] = "text/csv; charset=utf-8"
	}

	b := new(bytes.Buffer)
	gocsv.Marshal(input, b)
	rr := c.insertDataset(key, headers, b)

	if rr.Code != code {
		c.t.Errorf("handler returned wrong status code: got %v want %v: %s", rr.Code, code, rr.Body.String())
	}
}

func (c *testCache) insertJson(key string, headers map[string]string, input interface{}) {
	if headers == nil {
		headers = make(map[string]string)
	}

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(input)
	if _, ok := headers["Content-Type"]; !ok {
		headers["Content-Type"] = "application/json"
	}

	rr := c.insertDataset("FOO", headers, b)

	// Check the status code is what we expect.
	if rr.Code != http.StatusCreated {
		c.t.Errorf("handler returned wrong status code: got %v want %v: %s", rr.Code, http.StatusCreated, rr.Body.String())
	}
}

func (c *testCache) queryDataset(key string, headers map[string]string, q, method string) *httptest.ResponseRecorder {
	var urlString string
	var body io.Reader
	if method == "POST" {
		urlString = fmt.Sprintf("/qocache/dataset/%s/q", key)
		body = strings.NewReader(q)
	} else {
		// Assume GET
		urlString = fmt.Sprintf("/qocache/dataset/%s?q=%s", key, url.QueryEscape(q))
		body = nil
	}

	req, err := http.NewRequest(method, urlString, body)
	if err != nil {
		c.t.Fatal(err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	c.app.ServeHTTP(rr, req)

	if headers["Accept-Encoding"] == "lz4" {
		if rr.Header().Get("Content-Encoding") != "lz4" {
			c.t.Fatal("Expected content to be lz4 encoded, was not")
		}

		lz4Reader := lz4.NewReader(rr.Body)
		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(lz4Reader)
		if err != nil {
			c.t.Fatal(err)
		}

		rr.Body = buf
	}

	return rr
}

func (c *testCache) statistics() statistics.StatisticsData {
	req, err := http.NewRequest("GET", fmt.Sprintf("/qocache/statistics"), nil)
	if err != nil {
		c.t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	c.app.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		c.t.Errorf("Wrong status code for statistics: got %v want %v", rr.Code, http.StatusOK)
	}

	stats := statistics.StatisticsData{}
	err = json.Unmarshal(rr.Body.Bytes(), &stats)
	if err != nil {
		c.t.Fatal("Failed to unmarshal JSON stats")
	}

	return stats
}

func (c *testCache) status() {
	req, err := http.NewRequest("GET", fmt.Sprintf("/qocache/status"), nil)
	if err != nil {
		c.t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	c.app.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		c.t.Errorf("Wrong status code for status: got %v want %v", rr.Code, http.StatusOK)
	}

	if rr.Body.String() != "OK" {
		c.t.Fatalf("Unexpected status response: %s", rr.Body.String())
	}
}

func (c *testCache) queryJson(key string, headers map[string]string, q, method string, output interface{}) *httptest.ResponseRecorder {
	if headers == nil {
		headers = make(map[string]string)
	}

	headers["Accept"] = "application/json"
	rr := c.queryDataset(key, headers, q, method)
	if rr.Code != http.StatusOK {
		return rr
	}

	contentType := rr.Header().Get("Content-Type")
	if rr.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		c.t.Errorf("Wrong Content-type: %s", contentType)
	}

	err := json.NewDecoder(rr.Body).Decode(output)
	if err != nil {
		c.t.Fatalf("Failed to unmarshal JSON: %s", err.Error())
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
	input := []TestData{{S: "Foo«ταБЬℓσ»", I: 123, F: 1.5, B: true}}
	cache.insertCsv("FOO", nil, input)

	rr := cache.queryDataset("FOO", map[string]string{"Accept": "text/csv"}, "{}", "GET")
	if rr.Code != http.StatusOK {
		t.Errorf("Wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}

	contentType := rr.Header().Get("Content-Type")
	if rr.Header().Get("Content-Type") != "text/csv; charset=utf-8" {
		t.Errorf("Wrong Content-type: %s", contentType)
	}

	var output []TestData
	err := gocsv.Unmarshal(rr.Body, &output)
	if err != nil {
		t.Fatal("Failed to unmarshal CSV")
	}

	compareTestData(t, output, input)

	// Check statistics
	stats := cache.statistics()
	assertEqual(t, 1000, stats.StatisticsBufferSize)
	assertTrue(t, 0 < stats.StatisticsDuration && stats.StatisticsDuration < 1.0)
	assertEqual(t, 1, stats.DatasetCount)
	assertEqual(t, 1, stats.HitCount)
	assertEqual(t, 1, len(stats.QueryDurations))
	assertTrue(t, stats.QueryDurations[0] > 0)
	assertEqual(t, 1, len(stats.StoreDurations))
	assertTrue(t, stats.StoreDurations[0] > 0)
	assertEqual(t, 1, len(stats.StoreRowCounts))
	assertEqual(t, 1, stats.StoreRowCounts[0])
	assertTrue(t, stats.CacheSize > 0)
	assertEqual(t, 0, stats.MissCount)
}

func TestInsertAndQueryCsvLz4Compression(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}
	cache.insertCsv("FOO", map[string]string{"Content-Type": "text/csv", "Content-Encoding": "lz4"}, input)

	rr := cache.queryDataset("FOO", map[string]string{"Accept": "text/csv", "Accept-Encoding": "lz4"}, "{}", "GET")
	if rr.Code != http.StatusOK {
		t.Errorf("Wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}

	var output []TestData
	err := gocsv.Unmarshal(rr.Body, &output)
	if err != nil {
		t.Fatalf("Failed to unmarshal CSV: %s", err.Error())
	}

	compareTestData(t, output, input)
}

func TestStatus(t *testing.T) {
	cache := newTestCache(t)
	cache.status()
}

func toKeyVals(kvs []keyValProperty, format string) string {
	if format == "json" {
		m := make(map[string]interface{})
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
		s[i] = fmt.Sprintf("%s=%v", kv.key, kv.value)
	}
	return strings.Join(s, ";")
}

type keyValProperty struct {
	key      string
	value    interface{}
	expected interface{}
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
				cache.queryJson("FOO", nil, "{}", "GET", &output)
				assertEqual(t, 1, len(output))
				for _, kv := range tc {
					if len(output) > 0 {
						sVal, ok := output[0][kv.key].(string)
						assertTrue(t, ok)
						assertEqual(t, kv.expected, sVal)
					}
				}
			})
		}
	}
}

func TestStandinColumns(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 123, F: 1.5, B: true}}

	cases := [][]keyValProperty{
		// No stand in, value exists
		{{"S", "'Bar'", "Foo"}},

		// Stand in from string constant to X and from S to Y
		{{"X", "'Bar'", "Bar"}, {"Y", "S", "Foo"}},

		// Stand in from float constant
		{{"X", 1.5, 1.5}},

		// Stand in from int constant, expect float because of Go JSON decoding in test case, leave like this for now.
		{{"X", 2, 2.0}},
	}

	for _, format := range []string{"kv", "json"} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("Insert %s", toKeyVals(tc, format)), func(t *testing.T) {
				cache.insertCsv("FOO", map[string]string{"X-QCache-stand-in-columns": toKeyVals(tc, format)}, input)
				output := make([]map[string]interface{}, 0)
				cache.queryJson("FOO", nil, "{}", "GET", &output)
				assertEqual(t, 1, len(output))
				for _, kv := range tc {
					assertEqual(t, kv.expected, output[0][kv.key])
				}
			})

			t.Run(fmt.Sprintf("Query %s", toKeyVals(tc, format)), func(t *testing.T) {
				cache.insertCsv("FOO", nil, input)
				output := make([]map[string]interface{}, 0)
				cache.queryJson("FOO", map[string]string{"X-QCache-stand-in-columns": toKeyVals(tc, format)}, "{}", "GET", &output)
				assertEqual(t, 1, len(output))
				for _, kv := range tc {
					assertEqual(t, kv.expected, output[0][kv.key])
				}

				// Same query again but this time without stand in columns,
				// columns from previous query should remain.
				output = make([]map[string]interface{}, 0)
				cache.queryJson("FOO", nil, "{}", "GET", &output)
				assertEqual(t, 1, len(output))
				for _, kv := range tc {
					assertEqual(t, kv.expected, output[0][kv.key])
				}
			})
		}
	}
}

func enumTypes(enumVals map[string][]string) map[string]string {
	result := make(map[string]string)
	for k := range enumVals {
		result[k] = "enum"
	}
	return result
}

func TestEnumSpecifications(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo", I: 1}, {S: "Bar", I: 2}}

	cases := []struct {
		name     string
		spec     map[string][]string
		orderBy  string
		expected []map[string]interface{}
		skipJson bool
	}{
		{
			name:     "No spec",
			spec:     map[string][]string{},
			orderBy:  "S",
			expected: []map[string]interface{}{{"S": "Bar", "I": 2.0}, {"S": "Foo", "I": 1.0}}},
		{
			name:     "Spec string enum with non natural key order",
			spec:     map[string][]string{"S": {"Foo", "Bar"}},
			orderBy:  "S",
			expected: []map[string]interface{}{{"S": "Foo", "I": 1.0}, {"S": "Bar", "I": 2.0}}},
		{
			name:     "Spec int enum with non natural key order",
			spec:     map[string][]string{"I": {"2", "1"}},
			orderBy:  "I",
			expected: []map[string]interface{}{{"S": "Bar", "I": "2"}, {"S": "Foo", "I": "1"}},
			// Making enums of integers does not (currently) work and since you cannot type
			// spec JSON input the same way that you can with CSV there is no turn integers
			// into a string/enum.
			skipJson: true},
	}

	for _, inputFormat := range []string{"csv", "json"} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("Format: %s: %s", inputFormat, tc.name), func(t *testing.T) {
				jsonSpec, err := json.Marshal(tc.spec)
				assertEqual(t, nil, err)

				// For CSV the enum columns must be part of type specification
				// in addition to the enum spec.
				jsonTyp, err := json.Marshal(enumTypes(tc.spec))
				assertEqual(t, nil, err)

				headers := map[string]string{
					"X-QCache-enum-specs": string(jsonSpec),
					"X-QCache-types":      string(jsonTyp)}

				key := "FOO"
				if inputFormat == "json" {
					if tc.skipJson {
						return
					}
					cache.insertJson(key, headers, input)
				} else {
					cache.insertCsv(key, headers, input)
				}

				output := make([]map[string]interface{}, 0)
				cache.queryJson(key, nil, fmt.Sprintf(`{"select": ["S", "I"], "order_by": ["%s"]}`, tc.orderBy), "GET", &output)
				assertEqual(t, tc.expected, output)
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
		input    []TestData
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
		{
			filter:   `["in", "I", [123, 200, 17]]`,
			expected: []TestData{{I: 123, I2: 124}, {I: 200, I2: 124}},
		},
		{
			filter:   `["in", "S", ["", "A", "B"]]`,
			expected: []TestData{{S: ""}, {S: "A"}, {S: "B"}},
			input:    []TestData{{S: ""}, {S: "A"}, {S: "B"}, {S: "C"}},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Filter %s", tc.filter), func(t *testing.T) {
			if tc.input == nil {
				tc.input = input
			}
			cache.insertCsv("FOO", map[string]string{"X-QCache-types": "S=string"}, tc.input)
			rr := cache.queryJson("FOO", map[string]string{}, fmt.Sprintf(`{"where": %s}`, tc.filter), "GET", &output)
			if rr.Code != http.StatusOK {
				t.Errorf("Unexpected status code: %v, body: %s", rr.Code, rr.Body.String())
			}

			compareTestData(t, output, tc.expected)
		})
	}
}

func TestQueryNonExistingKey(t *testing.T) {
	cache := newTestCache(t)
	rr := cache.queryJson("FOO", map[string]string{}, "{}", "GET", nil)
	if rr.Code != http.StatusNotFound {
		t.Errorf("Unexpected status code: %v", rr.Code)
	}

	stats := cache.statistics()
	assertEqual(t, 1, stats.MissCount)
}

func TestInsertWithContentType(t *testing.T) {
	cache := newTestCache(t)
	input := []TestData{{S: "Foo"}}
	cases := []struct {
		contentType  string
		expectedCode int
	}{
		// Success
		{contentType: "text/csv", expectedCode: http.StatusCreated},
		{contentType: "text/csv; charset=utf-8", expectedCode: http.StatusCreated},
		{contentType: "text/csv; otherparam=Foo", expectedCode: http.StatusCreated},

		// Error
		{contentType: "text/FOO", expectedCode: http.StatusBadRequest},
		{contentType: "text/csv; charset=ISO-8859-1", expectedCode: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Insert with content type %s", tc.contentType), func(t *testing.T) {
			cache.insertCsvWithExpectedCode("FOO", map[string]string{"Content-Type": tc.contentType}, input, tc.expectedCode)
		})
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
			cache.queryJson("FOO", map[string]string{}, fmt.Sprintf(`{"order_by": %s}`, tc.orderBy), "GET", &output)
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
			rr := cache.queryJson("FOO", map[string]string{}, fmt.Sprintf(`{"offset": %d, "limit": %d}`, tc.offset, tc.limit), "GET", &output)
			assertEqual(t, fmt.Sprintf("%d", len(input)), rr.HeaderMap.Get("X-QCache-unsliced-length"))
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
		method       string
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
			query:    `{"distinct": ["S", "I"], "order_by": ["S", "I"]}`,
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
			name:     "alias with operation",
			input:    []TestData{{I: 1, I2: 10}, {I: 2, I2: 20}},
			query:    `{"select": ["I", ["=", "I3", ["+", "I2", "I"]]]}`,
			expected: []TestData{{I: 1, I3: 11}, {I: 2, I3: 22}}},
		{
			name:     "Sub query",
			input:    []TestData{{I: 1}, {I: 2}, {I: 3}},
			query:    `{"where": [">", "I", 1], "from": {"where": ["<", "I", 3]}}`,
			expected: []TestData{{I: 2}},
		},
		{
			name:     "Sub query in POST",
			input:    []TestData{{I: 1}, {I: 2}, {I: 3}},
			query:    `{"where": [">", "I", 1], "from": {"where": ["<", "I", 3]}}`,
			expected: []TestData{{I: 2}},
			method:   "POST",
		},
		{
			name:     "Unicode GET",
			input:    []TestData{{S: "ÅÄÖ"}, {S: "«ταБЬℓσ»"}, {S: "ABC"}},
			query:    `{"where": ["=", "S", "'«ταБЬℓσ»'"]}`,
			expected: []TestData{{S: "«ταБЬℓσ»"}},
			method:   "GET",
		},
		{
			name:     "Unicode POST",
			input:    []TestData{{S: "ÅÄÖ"}, {S: "«ταБЬℓσ»"}, {S: "ABC"}},
			query:    `{"where": ["=", "S", "'«ταБЬℓσ»'"]}`,
			expected: []TestData{{S: "«ταБЬℓσ»"}},
			method:   "POST",
		},
		// TODO: Test "in" with subexpression
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := newTestCache(t)
			cache.insertJson("FOO", tc.headers, tc.input)
			output := make([]TestData, 0)
			if tc.method == "" {
				tc.method = "GET"
			}
			rr := cache.queryJson("FOO", map[string]string{}, tc.query, tc.method, &output)

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

func init() { config.SetupLogger("debug") }

/* TODO
- Fix integer JSON parsing for generic maps in tests, right now they become floats
- Null stand ins?
- In filter with sub query
- Logging?
- Set and read charset (UTF-8)
- Dependencies
- README
*/
