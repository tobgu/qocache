package query_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tobgu/qocache/query"
)

func TestQuery(t *testing.T) {
	// taken from https://github.com/tobgu/qcache
	// TODO: Test the validity of each query
	var sampleQueries = [][]byte{
		[]byte(`{}`),
		[]byte(`{"select": [["=", "foo", "bar"]]}`),
		[]byte(`{"select": [["=", "baz", ["+", ["*", "bar", 2], "foo"]]]}`),
		[]byte(`{"select": [["=", "baz", 55]]}`),
		[]byte(`{"where": ["<", "foo", 1]}`),
		[]byte(`{"where": ["in", "foo", [1, 2]]}`),
		[]byte(`{"where": ["like", "foo", "'%bar%'"]}`),
		[]byte(`{"where": ["any_bits", "foo", 31]}`),
		[]byte(`{"where": ["&", [">", "foo", 1],["==", "bar", 2]]}`),
		[]byte(`{"where": ["!", ["==", "foo",  1]]}`),
		[]byte(`{"order_by": ["foo"]}`),
		[]byte(`{"order_by": ["-foo"]}`),
		[]byte(`{"offset": 5}`),
		[]byte(`{"limit": 10}`),
		[]byte(`{"group_by": ["foo"]}`),
		[]byte(`{"select": ["foo", ["sum", "bar"]],"group_by": ["foo"]}`),
		[]byte(`{"select": ["foo", "bar"],"distinct": ["foo"]}`),
		[]byte(`{"select": [["=", "foo_pct", ["*", 100, ["/", "foo", "bar"]]]],"from": {"select": ["foo", ["sum", "bar"]],"group_by": ["foo"]}}`),
		[]byte(`{"where": ["in", "foo", {"where": ["==", "bar", 10]}]}`),
		[]byte(`{"select": ["foo", ["sum", "bar"]],"where": [">", "bar", 0],"order_by": ["-bar"],"group_by": ["foo"],"limit": 10}`),
	}
	queries := make([]*query.Query, len(sampleQueries))
	for i, raw := range sampleQueries {
		queries[i] = &query.Query{}
		err := json.Unmarshal(raw, queries[i])
		if err != nil {
			t.Errorf("cannot unmarshal query %d: %s (%s)", i, string(sampleQueries[i]), err)
			assert.NoError(t, err)
		}
	}
}

func TestQuerySelect(t *testing.T) {
	// TODO
}

func TestQueryWhere(t *testing.T) {
	// TODO
}

func TestQueryOrderBy(t *testing.T) {
	// TODO
}

func TestQueryGroupBy(t *testing.T) {
	// TODO
}

func TestQueryDistinct(t *testing.T) {
	// TODO
}

func TestQueryOffset(t *testing.T) {
	// TODO
}

func TestQueryLimit(t *testing.T) {
	// TODO
}

func TestQueryFrom(t *testing.T) {
	// TODO
}
