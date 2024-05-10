package integrationtest

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func insertRecords(records []string) error {
	for _, record := range records {
		var unmarshalled map[string]interface{}

		if err := json.Unmarshal([]byte(record), &unmarshalled); err != nil {
			return fmt.Errorf("insertRecords: JSON provided as string is invalid: %v\nJSON provided as string was: %s", err, record)
		}

		if _, ok := unmarshalled["timestamp"]; !ok {
			unmarshalled["timestamp"] = float64(time.Now().UnixNano()) / 1e9
		}

		if _, ok := unmarshalled["id"]; !ok {
			unmarshalled["id"] = uuid.New().String()
		}

		marshalled, err := json.Marshal(unmarshalled)
		if err != nil {
			return fmt.Errorf("insertRecords: JSON marshalling failed: %v\nOriginal JSON was: %s", err, record)
		}

		if _, err := postRequest("api/write/record/", WithJSON(string(marshalled))); err != nil {
			return err
		}
	}
	return nil
}

func TestTimestampFilter(t *testing.T) {
	if err := insertRecords([]string{
		`{
			"timestamp": 1700000000,
			"category": "timestamp-filter",
			"message": "first message"
		}`,
		`{
			"timestamp": 1700000100,
			"category": "timestamp-filter",
			"message": "second message"
		}`,
		`{
			"timestamp": 1700000200,
			"category": "timestamp-filter",
			"message": "third message"
		}`,
	}); err != nil {
		t.Fatal(err)
	}

	resp, err := postRequest("api/read/records/", WithJSON(`
		{
			"filter": {
				"category": "timestamp-filter"
			}
		}
	`))
	if err != nil {
		t.Fatal(err)
	}

	records := resp.Body["records"].([]interface{})
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	resp, err = postRequest("api/read/records/", WithJSON(`
		{
			"filter": {
				"category": "timestamp-filter"
			},
			"timestamp_start": 1700000100,
			"timestamp_end": 1700000200
		}
	`))
	if err != nil {
		t.Fatal(err)
	}

	records = resp.Body["records"].([]interface{})
	if len(records) != 1 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	resp, err = postRequest("api/read/records/", WithJSON(`
		{
			"filter": {
				"category": "timestamp-filter"
			},
			"timestamp_start": 1700000100,
			"timestamp_end": 1700000201
		}
	`))
	if err != nil {
		t.Fatal(err)
	}

	records = resp.Body["records"].([]interface{})
	if len(records) != 2 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
}
