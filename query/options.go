package query

// QueryOpt is a functional option for
// constructing a new Query type.
type QueryOpt func(*Query)

// NewQuery returns a new Query configured
// with the provided options.
// TODO: The typing of these options can be
// tightened up significantly.
func NewQuery(opts ...QueryOpt) Query {
	query := Query{}
	for _, opt := range opts {
		opt(&query)
	}
	return query
}

func Select(opts interface{}) QueryOpt {
	return func(query *Query) {
		query.Select = opts
	}
}

func Where(opts interface{}) QueryOpt {
	return func(query *Query) {
		query.Where = opts
	}
}

func OrderBy(opts []string) QueryOpt {
	return func(query *Query) {
		query.OrderBy = opts
	}
}

func GroupBy(opts []string) QueryOpt {
	return func(query *Query) {
		query.GroupBy = opts
	}
}

func Distinct(opts []string) QueryOpt {
	return func(query *Query) {
		query.Distinct = opts
	}
}

func Offset(n int) QueryOpt {
	return func(query *Query) {
		query.Offset = n
	}
}

func Limit(n int) QueryOpt {
	return func(query *Query) {
		query.Limit = n
	}
}

func From(query *Query) QueryOpt {
	return func(query *Query) {
		query.From = query
	}
}
