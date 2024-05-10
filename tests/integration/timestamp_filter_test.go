package integrationtest

import (
	"testing"
)

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

	resp, err := postRequest("api/query/main/records/", WithJSON(`
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

	resp, err = postRequest("api/query/main/records/", WithJSON(`
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

	resp, err = postRequest("api/query/main/records/", WithJSON(`
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
