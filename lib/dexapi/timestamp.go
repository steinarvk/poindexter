package dexapi

import (
	"time"
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

	tt, err := time.Parse(time.RFC3339, string(data))
	if err != nil {
		return err
	}
	t.Value = tt
	return nil
}
