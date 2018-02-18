package query

import (
	"encoding/json"
	"fmt"
	qf "github.com/tobgu/qframe"
	"github.com/tobgu/qframe/aggregation"
	"github.com/tobgu/qframe/filter"
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

func unMarshalFilterClauses(input []interface{}) ([]qf.Clause, error) {
	result := make([]qf.Clause, 0, len(input))
	for _, x := range input {
		c, err := unMarshalFilterClause(x)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	return result, nil
}

func unMarshalFilterClause(input interface{}) (qf.Clause, error) {
	var c qf.Clause = qf.Null()
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
	case "and", "AND":
		subClauses, err := unMarshalFilterClauses(clause[1:])
		if err != nil {
			return c, err
		}
		c = qf.And(subClauses...)
	case "or", "OR":
		subClauses, err := unMarshalFilterClauses(clause[1:])
		if err != nil {
			return c, err
		}
		c = qf.Or(subClauses...)
	case "not", "NOT":
		if len(clause) != 2 {
			return c, fmt.Errorf("invalid 'not' filter clause length, expected [not, [...]], was: %v", clause)
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

		c = qf.Filter(filter.Filter{Comparator: filter.Comparator(operator), Column: colName, Arg: clause[2]})
	}

	return c, c.Err()
}

type selectClause struct {
	columns []string
	aliases []Alias
	aggregations
}

func (c selectClause) Select(f qf.QFrame) qf.QFrame {
	for _, a := range c.aliases {
		f = a.execute(f)
	}

	if len(c.columns) > 0 {
		return f.Select(c.columns...)
	}

	return f
}

type Alias interface {
	execute(f qf.QFrame) qf.QFrame
	column() string
}

type simpleAlias struct {
	dstCol string
	srcCol string
}

func (a simpleAlias) execute(f qf.QFrame) qf.QFrame {
	return f.Copy(a.dstCol, a.srcCol)
}

func (a simpleAlias) column() string {
	return a.dstCol
}

// TODO: How should this be executed?
type complexAlias struct {
	dstCol string
	expr   []interface{}
}

type aggregations []aggregation.Aggregation

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
	aliases := make([]Alias, 0)
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
				// Alias expression
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

func createAlias(expr []interface{}) (Alias, error) {
	if len(expr) != 2 {
		return nil, fmt.Errorf("invalid alias argument length, expected destination column and src expression, was: %v", expr)
	}

	dstCol, ok := expr[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid alias destination column, was: %v", expr[0])
	}

	srcCol, ok := expr[1].(string)

	// TODO: Handle complex aliases
	if !ok {
		return nil, fmt.Errorf("invalid alias source column, was: %v", expr[1])
	}

	return simpleAlias{dstCol: dstCol, srcCol: srcCol}, nil
}

func createAggregation(expr []interface{}) (aggregation.Aggregation, error) {
	noAgg := aggregation.Aggregation{}
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

	return aggregation.New(aggFn, aggCol), nil
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

func Query(f qf.QFrame, qString string) (qf.QFrame, error) {
	q, err := newQuery(qString)
	if err != nil {
		return qf.QFrame{}, err
	}

	return q.Query(f)
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

func (q query) Query(f qf.QFrame) (qf.QFrame, error) {
	var err error
	if q.From != nil {
		f, err = q.From.Query(f)
		if err != nil {
			return f, err
		}
	}

	if len(q.GroupBy) > 0 && len(q.Distinct) > 0 {
		// Don'ẗ really know what this combination would mean at the moment
		// therefor it is currently banned.
		// If a good use case comes up this may be reconsidered.
		return f, fmt.Errorf("cannot combine group by and distinct in the same query")
	}

	filterClause, err := unMarshalFilterClause(q.Where)
	if err != nil {
		return f, err
	}

	selectClause, err := unMarshalSelectClause(q.Select)
	if err != nil {
		return f, err
	}

	newF := filterClause.Filter(f)
	if len(q.GroupBy) > 0 || len(selectClause.aggregations) > 0 {
		grouper := newF.GroupBy(q.GroupBy...)
		newF = selectClause.aggregations.Execute(grouper)
	}

	newF = newF.Distinct(q.Distinct...)
	newF = newF.Sort(unMarshalOrderByClause(q.OrderBy)...)
	newF = selectClause.Select(newF)

	// TODO: Add info about frame length before slicing
	newF = q.slice(newF)
	return newF, newF.Err
}
