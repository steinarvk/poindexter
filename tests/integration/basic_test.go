package integrationtest

import (
	"encoding/json"
	"testing"

	"github.com/steinarvk/poindexter/lib/dexapi"
	"go.uber.org/zap"
)

func TestQueryRecord(t *testing.T) {
	resp, err := postRequest("api/query/main/records/", WithJSON(`
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
	resp, err := postRequest("api/query/main/records/", WithJSON(`
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
	resp, err := postRequest("api/ingest/main/jsonl/", ExpectStatus(200), WithBinaryBody([]byte(`
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

func TestAPI404(t *testing.T) {
	resp, err := postRequest("api/does-not-exist/", ExpectStatus(404), WithJSON("{}"))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	var respdata dexapi.ErrorResponse
	if err := json.Unmarshal(resp.RawBody, &respdata); err != nil {
		t.Fatalf("error unmarshalling response: %v", err)
	}

	if respdata.Error.Message != "No such API endpoint" {
		t.Fatalf("unexpected error message: %v", respdata.Error.Message)
	}
}
