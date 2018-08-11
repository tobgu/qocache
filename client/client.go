package client

import (
	"github.com/tobgu/qframe"
	"github.com/tobgu/qocache/query"
)

type Client interface {
	Create(name string, qf qframe.QFrame) error
	Read(name string) (qframe.QFrame, error)
	Query(query string) (query.QueryResult, error)
}
