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

func TestLookupRecord(t *testing.T) {
	resp, err := getRequest("api/query/main/records/577255cd-d2b0-4ba5-abac-2373b8475801/")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}

	var respdata dexapi.LookupRecordResponse
	if err := json.Unmarshal(resp.RawBody, &respdata); err != nil {
		t.Fatalf("error unmarshalling response: %v", err)
	}

	if respdata.Record["message"] != "hello world" {
		t.Fatalf("unexpected record message: %v", respdata.Record["message"])
	}
}

func TestLookupNonexistentRecord(t *testing.T) {
	resp, err := getRequest("api/query/main/records/577255cd-d2b0-4ba5-abac-2373b8475802/", ExpectStatus(404))
	if err != nil {
		t.Fatalf("request error: %v", err)
	}

	var respdata dexapi.ErrorResponse
	if err := json.Unmarshal(resp.RawBody, &respdata); err != nil {
		t.Fatalf("error unmarshalling response: %v", err)
	}
}

func TestLookupByField(t *testing.T) {
	if err := insertRecords([]string{
		`{"my-unique-field": "this-is-unique", "message": "hello world"}`,
		`{"my-unique-field": "this-is-not-unique", "message": "hello universe"}`,
		`{"my-unique-field": "this-is-not-unique", "message": "hello there"}`,
	}); err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	if _, err := getRequest("api/query/main/records/by/my-unique-field/this-is-not-unique/", ExpectStatus(404)); err != nil {
		t.Fatalf("request error: %v", err)
	}

	resp, err := getRequest("api/query/main/records/by/my-unique-field/this-is-unique/", ExpectStatus(200))
	if err != nil {
		t.Fatalf("request error: %v", err)
	}

	var respdata dexapi.LookupRecordResponse
	if err := json.Unmarshal(resp.RawBody, &respdata); err != nil {
		t.Fatalf("error unmarshalling response: %v", err)
	}

	if respdata.Record["message"] != "hello world" {
		t.Fatalf("unexpected record message: %v", respdata.Record["message"])
	}
}

func TestLookupByFieldSuperseded(t *testing.T) {
	if err := insertRecords([]string{
		`{"id": "33cc00ee-ba1a-4b0f-8257-833b33aa16cf", "my-new-unique-field": "this-is-unique", "message": "hello world"}`,
		`{"my-new-unique-field": "this-is-not-unique", "message": "hello universe"}`,
		`{"my-new-unique-field": "this-is-not-unique", "message": "hello there"}`,
		`{"supersedes_id": "33cc00ee-ba1a-4b0f-8257-833b33aa16cf", "my-new-unique-field": "this-is-unique", "message": "hello multiverse"}`,
	}); err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	if _, err := getRequest("api/query/main/records/by/my-new-unique-field/this-is-not-unique/", ExpectStatus(404)); err != nil {
		t.Fatalf("request error: %v", err)
	}

	resp, err := getRequest("api/query/main/records/by/my-new-unique-field/this-is-unique/", ExpectStatus(200))
	if err != nil {
		t.Fatalf("request error: %v", err)
	}

	var respdata dexapi.LookupRecordResponse
	if err := json.Unmarshal(resp.RawBody, &respdata); err != nil {
		t.Fatalf("error unmarshalling response: %v", err)
	}

	if respdata.Record["message"] != "hello multiverse" {
		t.Fatalf("unexpected record message: %v", respdata.Record["message"])
	}
}
