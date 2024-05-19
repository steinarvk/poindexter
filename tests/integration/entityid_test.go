package integrationtest

import (
	"encoding/json"
	"testing"

	"github.com/steinarvk/poindexter/lib/dexapi"
)

func TestImplicitEntityID(t *testing.T) {
	// Because no entity ID is specified, the initial entity ID becomes the record ID.

	if _, err := postRequest("api/ingest/main/record/", WithJSON(`
		{
			"id": "7d1d4443-fcf8-42a2-9ea9-df46cdf15d24",
			"timestamp": 1715316569,
			"message": "version one"
		}
	`)); err != nil {
		t.Fatal(err)
	}

	if _, err := getRequest("api/query/main/records/7d1d4443-fcf8-42a2-9ea9-df46cdf15d24/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	}

	if _, err := getRequest("api/query/main/entities/7d1d4443-fcf8-42a2-9ea9-df46cdf15d24/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	}

	if _, err := postRequest("api/ingest/main/record/", WithJSON(`
		{
			"id": "f4c8c610-34d3-4ae1-ba07-010a89b19a96",
			"supersedes_id": "7d1d4443-fcf8-42a2-9ea9-df46cdf15d24",
			"timestamp": 1715316569,
			"message": "version two"
		}
	`)); err != nil {
		t.Fatal(err)
	}

	if recordResp, err := getRequest("api/query/main/records/7d1d4443-fcf8-42a2-9ea9-df46cdf15d24/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	} else {
		var recordRespData dexapi.LookupRecordResponse
		if err := json.Unmarshal(recordResp.RawBody, &recordRespData); err != nil {
			t.Fatalf("error unmarshalling response: %v", err)
		}

		if recordRespData.Record["message"] != "version one" {
			t.Errorf("unexpected record message: %v", recordRespData.Record["message"])
		}
	}

	if recordResp, err := getRequest("api/query/main/entities/7d1d4443-fcf8-42a2-9ea9-df46cdf15d24/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	} else {
		var recordRespData dexapi.LookupRecordResponse
		if err := json.Unmarshal(recordResp.RawBody, &recordRespData); err != nil {
			t.Fatalf("error unmarshalling response: %v", err)
		}

		if recordRespData.Record["message"] != "version two" {
			t.Errorf("unexpected record message: %v", recordRespData.Record["message"])
		}
	}
}

func TestEntityUpsert(t *testing.T) {
	// Because no entity ID is specified, the initial entity ID becomes the record ID.

	if _, err := postRequest("api/ingest/main/record/", WithJSON(`
		{
			"id": "4aa432cc-d5e8-495a-928e-cbb9bbb3b1d6",
			"timestamp": "2021-11-15T14:45:28.969412345Z",
			"message": "version one"
		}
	`)); err != nil {
		t.Fatal(err)
	}

	if _, err := getRequest("api/query/main/records/4aa432cc-d5e8-495a-928e-cbb9bbb3b1d6/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	}

	if _, err := getRequest("api/query/main/entities/4aa432cc-d5e8-495a-928e-cbb9bbb3b1d6/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	}

	// First request: because there is a change (of the message field), a new superseding record gets inserted.

	if _, err := postRequest("api/ingest/main/upsert/", WithJSON(`
		{
			"record_id": "188a903e-2fe6-468f-8dd2-be2704400a4e",
			"entity_id": "4aa432cc-d5e8-495a-928e-cbb9bbb3b1d6",
			"timestamp": "2022-11-15T14:45:28.969412345Z",
			"message": "version two"
		}
	`), ExpectStatus(201)); err != nil {
		t.Fatal(err)
	}

	if recordResp, err := getRequest("api/query/main/entities/4aa432cc-d5e8-495a-928e-cbb9bbb3b1d6/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	} else {
		var recordRespData dexapi.LookupRecordResponse
		if err := json.Unmarshal(recordResp.RawBody, &recordRespData); err != nil {
			t.Fatalf("error unmarshalling response: %v", err)
		}

		if recordRespData.Record["timestamp"] != "2022-11-15T14:45:28.969412345Z" {
			t.Errorf("unexpected record message: %v", recordRespData.Record["timestamp"])
		}

		if recordRespData.Record["message"] != "version two" {
			t.Errorf("unexpected record message: %v", recordRespData.Record["message"])
		}
	}

	// Second request: because there is no change, there's no insert.

	if _, err := postRequest("api/ingest/main/upsert/", WithJSON(`
		{
			"record_id": "4c1bfe5a-cd1f-417d-9918-b3966528fb0e",
			"entity_id": "4aa432cc-d5e8-495a-928e-cbb9bbb3b1d6",
			"timestamp": "2023-11-15T14:45:28.969412345Z",
			"message": "version two"
		}
	`), ExpectStatus(200)); err != nil {
		t.Fatal(err)
	}

	if recordResp, err := getRequest("api/query/main/entities/4aa432cc-d5e8-495a-928e-cbb9bbb3b1d6/", ExpectStatus(200)); err != nil {
		t.Fatal(err)
	} else {
		var recordRespData dexapi.LookupRecordResponse
		if err := json.Unmarshal(recordResp.RawBody, &recordRespData); err != nil {
			t.Fatalf("error unmarshalling response: %v", err)
		}

		// Timestamp should still be the old one
		if recordRespData.Record["timestamp"] != "2022-11-15T14:45:28.969412345Z" {
			t.Errorf("unexpected record timestamp (should not have changed): %v", recordRespData.Record["timestamp"])
		}

		if recordRespData.Record["message"] != "version two" {
			t.Errorf("unexpected record message (should not have changed): %v", recordRespData.Record["message"])
		}
	}
}
