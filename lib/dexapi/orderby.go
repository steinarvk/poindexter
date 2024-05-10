package dexapi

import (
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
)

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
	zap.L().Sugar().Infof("unmarshalling order-by: %q", s)
	if len(data) == 0 || s == "null" {
		return nil
	}

	var stringValue string
	if err := json.Unmarshal(data, &stringValue); err != nil {
		return err
	}
	s = stringValue

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
