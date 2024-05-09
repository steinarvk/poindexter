package dexapi

import "fmt"

type OrderByVariant string

const (
	OrderByID     = OrderByVariant("id")
	OrderByTime   = OrderByVariant("time")
	OrderByRandom = OrderByVariant("random")
	// TODO later: OrderByField
)

type OrderBy struct {
	Variant    OrderByVariant
	Descending bool
}

func (o *OrderBy) MarshalJSON() ([]byte, error) {
	prefix := ""
	if o.Descending {
		prefix = "-"
	}
	return []byte(prefix + string(o.Variant)), nil
}

func (o *OrderBy) UnmarshalJSON(data []byte) error {
	s := string(data)
	if len(data) == 0 || s == "null" {
		return nil
	}
	if s[0] == '-' {
		o.Descending = true
		s = s[1:]
	}
	switch {
	case s == "id":
		o.Variant = OrderByID
	case s == "time":
		o.Variant = OrderByTime
	case s == "random":
		o.Variant = OrderByRandom
	default:
		return fmt.Errorf("unknown order-by variant: %s", s)
	}
	return nil
}
