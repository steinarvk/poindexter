package recdexdb

import "time"

type QueryOrder string

const (
	OrderByTimeAscending  = QueryOrder("time-ascending")
	OrderByTimeDescending = QueryOrder("time-descending")
	OrderRandom           = QueryOrder("random")
)

type Query struct {
	Namespace string

	OmitSuperseded bool
	OmitLocked     bool

	TimestampBefore *time.Time
	TimestampAfter  *time.Time

	FieldsPresent []string
	FieldValues   map[string]interface{}

	Order QueryOrder
	Limit int
}
