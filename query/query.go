package query

import (
	"encoding/json"
	"fmt"
	qf "github.com/tobgu/qframe"
	"github.com/tobgu/qframe/config/groupby"
	"github.com/tobgu/qframe/filter"
	qostrings "github.com/tobgu/qocache/strings"
	"strings"
)

// TODO: It is possible that most of the functionality here would actually fit better in the QFrame
//       Or even in an own, query, repository together with some of the query related functionality
//       in QFrame.

type query struct {
	Select   interface{} `json:"select,omitempty"`
	Where    interface{} `json:"where,omitempty"`
	OrderBy  []string    `json:"order_by,omitempty"`
	GroupBy  []string    `json:"group_by,omitempty"`
	Distinct []string    `json:"distinct,omitempty"`
	Offset   int         `json:"offset,omitempty"`
	Limit    int         `json:"limit,omitempty"`
	From     *query      `json:"from,omitempty"`
}

type QueryResult struct {
	Qframe      qf.QFrame
	Err         error
	UnslicedLen int
}

func unMarshalFilterClauses(input []interface{}) ([]qf.FilterClause, error) {
	result := make([]qf.FilterClause, 0, len(input))
	for _, x := range input {
		c, err := unMarshalFilterClause(x)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	return result, nil
}

func unMarshalFilterClause(input interface{}) (qf.FilterClause, error) {
	var c qf.FilterClause = qf.Null()
	if input == nil {
		return c, c.Err()
	}

	clause, ok := input.([]interface{})
	if !ok {
		return c, fmt.Errorf("malformed filter clause, expected list of clauses, was: %v", input)
	}

	if len(clause) < 2 {
		return c, fmt.Errorf("malformed filter clause, too short: %v", clause)
	}

	operator, ok := clause[0].(string)
	if !ok {
		return c, fmt.Errorf("malformed filter clause, expected operator string, was: %v", clause[0])
	}

	switch operator {
	case "&":
		subClauses, err := unMarshalFilterClauses(clause[1:])
		if err != nil {
			return c, err
		}
		c = qf.And(subClauses...)
	case "|":
		subClauses, err := unMarshalFilterClauses(clause[1:])
		if err != nil {
			return c, err
		}
		c = qf.Or(subClauses...)
	case "!":
		if len(clause) != 2 {
			return c, fmt.Errorf(`invalid 'not' filter clause length, expected ["!", [...]], was: %v`, clause)
		}

		subClause, err := unMarshalFilterClause(clause[1])
		if err != nil {
			return c, err
		}
		c = qf.Not(subClause)
	default: // Comparisons: <, >, =, ...
		if len(clause) != 3 {
			return c, fmt.Errorf("invalid filter clause length, expected [operator, column, value], was: %v", clause)
		}

		colName, ok := clause[1].(string)
		if !ok {
			return c, fmt.Errorf("invalid column name, expected string, was: %v", clause[1])
		}

		var arg = clause[2]
		if s, ok := arg.(string); ok {
			// Quoted strings are string constants, other strings are column names
			if qostrings.IsQuoted(s) {
				arg = qostrings.TrimQuotes(s)
			} else {
				arg = filter.ColumnName(s)
			}
		}
		c = qf.Filter(filter.Filter{Comparator: operator, Column: colName, Arg: arg})
	}

	return c, c.Err()
}

type selectClause struct {
	columns []string
	aliases []alias
	aggregations
}

func (c selectClause) doSelect(f qf.QFrame) qf.QFrame {
	for _, a := range c.aliases {
		f = a.execute(f)
	}

	if len(c.columns) > 0 {
		return f.Select(c.columns...)
	}

	return f
}

type alias struct {
	dstCol string
	expr   qf.Expression
}

func (a alias) execute(f qf.QFrame) qf.QFrame {
	return f.Eval(a.dstCol, a.expr)
}

func (a alias) column() string {
	return a.dstCol
}

type aggregations []qf.Aggregation

func (as aggregations) Execute(grouper qf.Grouper) qf.QFrame {
	return grouper.Aggregate(as...)
}

func unMarshalSelectClause(input interface{}) (selectClause, error) {
	emptySelect := selectClause{}
	if input == nil {
		return emptySelect, nil
	}

	inputSlice, ok := input.([]interface{})
	if !ok {
		return emptySelect, fmt.Errorf("malformed select, must be a list, was: %v", inputSlice)
	}

	columns := make([]string, 0, len(inputSlice))
	aggregations := make(aggregations, 0)
	aliases := make([]alias, 0)
	for _, part := range inputSlice {
		switch p := part.(type) {
		case []interface{}:
			if len(p) < 2 {
				return emptySelect, fmt.Errorf("malformed expression in select, too short: %v", p)
			}

			op, ok := p[0].(string)
			if !ok {
				return emptySelect, fmt.Errorf("malformed expression in select, expected a string in first position: %v", p)
			}

			if op == "=" {
				// alias expression
				a, err := createAlias(p[1:])
				if err != nil {
					return emptySelect, err
				}
				aliases = append(aliases, a)
				columns = append(columns, a.column())
			} else {
				// Assume aggregation expression
				a, err := createAggregation(p)
				if err != nil {
					return emptySelect, err
				}
				aggregations = append(aggregations, a)
				columns = append(columns, a.Column)
			}
		case string:
			columns = append(columns, p)
		default:
			return selectClause{}, fmt.Errorf("unknown expression in select: %v", p)
		}
	}

	return selectClause{columns: columns, aggregations: aggregations, aliases: aliases}, nil
}

// Takes an alias expression as parsed from JSON and transforms it into a data
// structure that can be interpreted as a QFrame expression. This mainly consists
// of deciding which strings that represent columns and which that represent string
// constants.
func prepareAlias(a *interface{}) {
	switch t := (*a).(type) {
	case []interface{}:
		for i := range t {
			if i > 0 {
				// Let the first element remain as is since it is the operator
				prepareAlias(&t[i])
			}
		}
	case string:
		if qostrings.IsQuoted(t) {
			// String constant
			s := qostrings.TrimQuotes(t)
			*a = s
		} else {
			*a = filter.ColumnName(t)
		}
	default:
		// No need to do anything here
	}
}

func createAlias(aliasExpr []interface{}) (alias, error) {
	if len(aliasExpr) != 2 {
		return alias{}, fmt.Errorf("invalid alias argument length, expected destination column and src expression, was: %v", aliasExpr)
	}

	dstCol, ok := aliasExpr[0].(string)
	if !ok {
		return alias{}, fmt.Errorf("invalid alias destination column, was: %v", aliasExpr[0])
	}

	prepareAlias(&aliasExpr[1])
	expr := qf.Val(aliasExpr[1])
	return alias{dstCol: dstCol, expr: expr}, expr.Err()
}

func createAggregation(expr []interface{}) (qf.Aggregation, error) {
	noAgg := qf.Aggregation{}
	if len(expr) != 2 {
		return noAgg, fmt.Errorf("invalid aggregation expression, expected length 2, was: %v", expr)
	}

	aggFn, ok := expr[0].(string)
	if !ok {
		return noAgg, fmt.Errorf("aggregation function name must be a string, was: %v", expr[0])
	}

	aggCol, ok := expr[1].(string)
	if !ok {
		return noAgg, fmt.Errorf("aggregation column name must be a string, was: %v", expr[1])
	}

	return qf.Aggregation{Fn: aggFn, Column: aggCol}, nil
}

func unMarshalOrderByClause(input []string) []qf.Order {
	result := make([]qf.Order, len(input))
	for i, s := range input {
		if strings.HasPrefix(s, "-") {
			result[i] = qf.Order{Column: s[1:], Reverse: true}
		} else {
			result[i] = qf.Order{Column: s, Reverse: false}
		}
	}

	return result
}

func newQuery(qString string) (query, error) {
	q := query{}
	err := json.Unmarshal([]byte(qString), &q)
	return q, err
}

func Query(f qf.QFrame, qString string) QueryResult {
	q, err := newQuery(qString)
	if err != nil {
		return QueryResult{Err: err}
	}

	return q.query(f)
}

func intMin(x, y int) int {
	if x < y {
		return x
	}

	return y
}

func (q query) slice(f qf.QFrame) qf.QFrame {
	stop := f.Len()
	if q.Limit > 0 {
		stop = intMin(stop, q.Offset+q.Limit)
	}

	return f.Slice(q.Offset, stop)
}

func (q query) query(f qf.QFrame) QueryResult {
	var err error
	if q.From != nil {
		result := q.From.query(f)
		if result.Err != nil {
			return result
		}
		f = result.Qframe
	}

	if len(q.GroupBy) > 0 && len(q.Distinct) > 0 {
		// Don'ẗ really know what this combination would mean at the moment
		// therefor it is currently banned.
		// If a good use case comes up this may be reconsidered.
		return QueryResult{Err: fmt.Errorf("cannot combine group by and distinct in the same query")}
	}

	filterClause, err := unMarshalFilterClause(q.Where)
	if err != nil {
		return QueryResult{Err: err}
	}

	selectClause, err := unMarshalSelectClause(q.Select)
	if err != nil {
		return QueryResult{Err: err}
	}

	newF := f.Filter(filterClause)
	if len(q.GroupBy) > 0 || len(selectClause.aggregations) > 0 {
		grouper := newF.GroupBy(groupby.Columns(q.GroupBy...))
		newF = selectClause.aggregations.Execute(grouper)
	}

	if q.Distinct != nil {
		newF = newF.Distinct(groupby.Columns(q.Distinct...))
	}

	newF = newF.Sort(unMarshalOrderByClause(q.OrderBy)...)
	newF = selectClause.doSelect(newF)
	unslicedLen := newF.Len()
	newF = q.slice(newF)
	return QueryResult{Qframe: newF, UnslicedLen: unslicedLen, Err: newF.Err}
}
