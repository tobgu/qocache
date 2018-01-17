package query

import (
	"encoding/json"
	"fmt"
	qf "github.com/tobgu/qframe"
)

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

func unMarshalFilter(input interface{}) (qf.Clause, error) {
	fmt.Println("Input: ", input)
	c := qf.Null()
	return c, c.Err()
}

func Query(f qf.QFrame, qString string) (qf.QFrame, error) {
	query := query{}
	err := json.Unmarshal([]byte(qString), &query)
	if err != nil {
		return qf.QFrame{}, err
	}

	/* TODO
	- Filter
	- Group and aggregate
	- Distinct
	- Project
	- Sort
	- Slice/paginate
	*/

	c, err := unMarshalFilter(query.Where)
	if err != nil {
		return f, err
	}

	newF := c.Filter(f)
	return newF, newF.Err
}
