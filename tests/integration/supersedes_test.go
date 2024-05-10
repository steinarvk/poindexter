package integrationtest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/steinarvk/poindexter/lib/dexapi"
	"go.uber.org/zap"
)

func TestSupersededRecords(t *testing.T) {
	if _, err := postRequest("api/write/record/", WithJSON(`
		{
			"id": "914452d1-4e23-480d-8034-6b52f6a970e8",
			"timestamp": 1715316569,
			"category": "supersedes-test-records",
			"message": "message one"
		}
	`)); err != nil {
		t.Fatal(err)
	}

	if _, err := postRequest("api/write/record/", WithJSON(`
		{
			"id": "11a95a34-135d-48e1-b4c7-356333424337",
			"supersedes_id": "914452d1-4e23-480d-8034-6b52f6a970e8",
			"timestamp": 1715316569,
			"category": "supersedes-test-records",
			"message": "message two"
		}
	`)); err != nil {
		t.Fatal(err)
	}

	resp, err := postRequest("api/read/records/", WithJSON(`
		{
			"filter": {
				"category": "supersedes-test-records"
			},
			"omit_superseded": true
		}
	`))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	records := resp.Body["records"].([]interface{})
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	firstEntry := records[0].(map[string]interface{})
	firstRecord := firstEntry["record"].(map[string]interface{})
	firstMessage := firstRecord["message"].(string)

	if firstMessage != "message two" {
		t.Fatalf("expected message two, got %s", firstMessage)
	}
}
func TestSupersededRecordsWithoutRespecting(t *testing.T) {
	if _, err := postRequest("api/write/record/", WithJSON(`
		{
			"id": "7a3bb522-7abe-491b-abfc-9d8192dfdcb0",
			"timestamp": 1715316569,
			"category": "supersedes-test-records-without-respect",
			"message": "message hello"
		}
	`)); err != nil {
		t.Fatal(err)
	}

	if _, err := postRequest("api/write/record/", WithJSON(`
		{
			"id": "c9f9ccb0-5544-477f-85be-e35bd51059ba",
			"supersedes_id": "7a3bb522-7abe-491b-abfc-9d8192dfdcb0",
			"timestamp": 1715316569,
			"category": "supersedes-test-records-without-respect",
			"message": "message world"
		}
	`)); err != nil {
		t.Fatal(err)
	}

	resp, err := postRequest("api/read/records/", WithJSON(`
		{
			"filter": {
				"category": "supersedes-test-records-without-respect"
			},
			"omit_superseded": false
		}
	`))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	records := resp.Body["records"].([]interface{})
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestSupersedingNonexistentFails(t *testing.T) {
	resp, err := postRequest("api/write/record/", ExpectStatus(400), WithJSON(`
		{
			"id": "60fd20a1-cf39-4f98-af43-80c6361d4dd0",
			"timestamp": 1715316569,
			"supersedes_id": "0b826f1e-fc26-44ca-87d2-684783c6d231"
		}
	`))
	if err != nil {
		t.Fatal(err)
	}

	zap.L().Sugar().Infof("error response was: %v", string(resp.RawBody))

	var errorResp dexapi.ErrorResponse
	if err := json.Unmarshal(resp.RawBody, &errorResp); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}

	if !strings.Contains(errorResp.Error.Message, "record to be superseded does not exist") {
		t.Error("unexpected error message")
	}

	if errorResp.Error.Data["supersedes_id"] != "0b826f1e-fc26-44ca-87d2-684783c6d231" {
		t.Error("unexpected error data")
	}
}
