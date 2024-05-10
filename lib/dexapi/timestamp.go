package dexapi

import (
	"encoding/json"
	"time"

	"github.com/steinarvk/poindexter/lib/flatten"
)

type Timestamp struct {
	Value time.Time
}

func (t *Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(t.Value.Format(time.RFC3339)), nil
}

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}

	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	tt, err := flatten.InterpretTimestamp(value)
	if err != nil {
		return err
	}
	t.Value = tt

	return nil
}
