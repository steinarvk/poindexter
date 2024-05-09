package poindexterdb

import (
	"errors"

	"github.com/gibson042/canonicaljson-go"
	"github.com/steinarvk/poindexter/lib/dexapi"
)

// This is NOT the real query API; it's low-level.

type QueryOrder string

const (
	OrderByTimeAscending  = QueryOrder("time-ascending")
	OrderByTimeDescending = QueryOrder("time-descending")
	OrderRandom           = QueryOrder("random")
)

type CompiledQuery struct {
	dexapi.RawQuery
	dexapi.QueryOptions
	dexapi.OrderBy
	dexapi.TimestampFilter

	Limit int
}

func (d *DB) CompileQuery(query *dexapi.Query) (*CompiledQuery, error) {
	if query == nil {
		return nil, errors.New("nil query")
	}

	flattener := d.makeFlattener()

	wildcard := "null"

	rq := dexapi.RawQuery{
		Filter: &dexapi.RawFilter{
			FieldValues: map[string][]string{},
		},
		Exclude: &dexapi.RawFilter{
			FieldValues: map[string][]string{},
		},
	}

	if query.Wildcard != nil {
		cj, err := canonicaljson.Marshal(query.Wildcard)
		if err != nil {
			return nil, err
		}
		wildcard = string(cj)
	}

	if query.Filter != nil {
		fieldvalues, err := flattener.FlattenObjectToFields(query.Filter)
		if err != nil {
			return nil, err
		}

		for key, values := range fieldvalues {
			for _, value := range values {
				value := string(value)
				wild := value == wildcard
				if wild {
					rq.Filter.FieldsPresent = append(rq.Filter.FieldsPresent, key)
				} else {
					rq.Filter.FieldValues[key] = append(rq.Filter.FieldValues[key], value)
				}
			}
		}
	}

	if query.Exclude != nil {
		fieldvalues, err := flattener.FlattenObjectToFields(query.Exclude)
		if err != nil {
			return nil, err
		}

		for key, values := range fieldvalues {
			for _, value := range values {
				value := string(value)
				wild := value == wildcard
				if wild {
					rq.Exclude.FieldsPresent = append(rq.Exclude.FieldsPresent, key)
				} else {
					rq.Exclude.FieldValues[key] = append(rq.Exclude.FieldValues[key], value)
				}
			}
		}
	}

	if raw := query.Raw; raw != nil {
		if raw.Filter != nil {
			rq.Filter.FieldsPresent = append(rq.Filter.FieldsPresent, raw.Filter.FieldsPresent...)
			for key, values := range raw.Filter.FieldValues {
				rq.Filter.FieldValues[key] = append(rq.Filter.FieldValues[key], values...)
			}
		}
		if raw.Exclude != nil {
			rq.Exclude.FieldsPresent = append(rq.Exclude.FieldsPresent, raw.Exclude.FieldsPresent...)
			for key, values := range raw.Exclude.FieldValues {
				rq.Exclude.FieldValues[key] = append(rq.Exclude.FieldValues[key], values...)
			}
		}
	}

	return &CompiledQuery{
		RawQuery:        rq,
		QueryOptions:    query.GetOptions(),
		OrderBy:         query.GetOrderBy(),
		TimestampFilter: query.GetTimestampFilter(),
		Limit:           query.GetLimit(),
	}, nil
}
