package integrationtest

import (
	"testing"

	"go.uber.org/zap"
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

func TestWriteRecordsJSONL(t *testing.T) {
	resp, err := postRequest("api/write/jsonl/", ExpectStatus(200), WithBinaryBody([]byte(`
		{"id": "e3202a40-3b01-4865-9d8f-89aeaf29c08b", "timestamp": 1715316569, "message": "hello world"}
		{"id": "a4984998-4fc1-4c58-ac5e-1937d4f821dc", "timestamp": 1715316569, "message": "hello world"}
		{"id": "f8db9d0d-2ea8-4c6e-81c5-f4bf290dc756", "timestamp": 1715316569, "message": "hello world"}
	`)))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	zap.L().Sugar().Infof("raw response body: %v", string(resp.RawBody))

	// TODO: the response here needs a proper API type, which should then be tested.
}
