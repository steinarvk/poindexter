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
