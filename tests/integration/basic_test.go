package integrationtest

import (
	"testing"
)

func TestQueryRecord(t *testing.T) {
	resp, err := postRequest("api/read/records/", WithJSON(`
		{
			"filter": {
				"category": "initial-test-records",
				"object": {
					"integer": 42
				}
			}
		}
	`))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	if n := len(resp.Body["records"].([]interface{})); n != 1 {
		t.Fatalf("expected 1 record, got %d", n)
	}
}

func TestQueryRecordWithoutOmitSuperseded(t *testing.T) {
	resp, err := postRequest("api/read/records/", WithJSON(`
		{
			"filter": {
				"category": "initial-test-records",
				"object": {
					"integer": 42
				}
			},
			"omit_superseded": false
		}
	`))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	if n := len(resp.Body["records"].([]interface{})); n != 1 {
		t.Fatalf("expected 1 record, got %d", n)
	}
}
