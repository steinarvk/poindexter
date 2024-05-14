package dexapi

import "time"

type RawFilter struct {
	FieldsPresent []string            `json:"fields_present"`
	FieldValues   map[string][]string `json:"field_values"`
}

type RawQuery struct {
	Filter  *RawFilter `json:"filter"`
	Exclude *RawFilter `json:"exclude"`
}

type QueryOptions struct {
	OmitSuperseded     bool
	OmitHidden         bool
	OmitLocked         bool
	TreatNullsAsAbsent bool
}

type QueryOptionalOptions struct {
	OmitSuperseded     *bool `json:"omit_superseded"`
	OmitHidden         *bool `json:"omit_hidden"`
	OmitLocked         *bool `json:"omit_locked"`
	TreatNullsAsAbsent *bool `json:"treat_nulls_as_absent"`
}

type TimestampFilter struct {
	TimestampStart *time.Time
	TimestampEnd   *time.Time
}

type Query struct {
	Wildcard interface{} `json:"wildcard"`

	Filter  map[string]interface{} `json:"filter"`
	Exclude map[string]interface{} `json:"exclude"`

	Raw *RawQuery `json:"raw"`

	QueryOptionalOptions

	TimestampStart *Timestamp `json:"timestamp_start"`
	TimestampEnd   *Timestamp `json:"timestamp_end"`

	OrderBy *OrderBy `json:"order_by"`

	Limit *int `json:"limit"`
}

func (q Query) GetOptions() QueryOptions {
	opts := QueryOptions{
		OmitSuperseded:     true,
		OmitHidden:         true,
		OmitLocked:         false,
		TreatNullsAsAbsent: true,
	}
	if q.OmitSuperseded != nil {
		opts.OmitSuperseded = *q.OmitSuperseded
	}
	if q.OmitHidden != nil {
		opts.OmitHidden = *q.OmitHidden
	}
	if q.OmitLocked != nil {
		opts.OmitLocked = *q.OmitLocked
	}
	if q.TreatNullsAsAbsent != nil {
		opts.TreatNullsAsAbsent = *q.TreatNullsAsAbsent
	}
	return opts
}

var (
	defaultLimit = 1000
)

func (q Query) GetLimit() int {
	if q.Limit == nil {
		return defaultLimit
	}
	return *q.Limit
}

var (
	defaultOrderBy = OrderBy{
		Variant:    OrderByTime,
		Descending: false,
	}
)

func (q Query) GetOrderBy() OrderBy {
	if q.OrderBy == nil {
		return defaultOrderBy
	}
	return *q.OrderBy
}

func (q Query) GetTimestampFilter() TimestampFilter {
	rv := TimestampFilter{}

	if q.TimestampStart != nil {
		rv.TimestampStart = &q.TimestampStart.Value
	}

	if q.TimestampEnd != nil {
		rv.TimestampEnd = &q.TimestampEnd.Value
	}

	return rv
}
