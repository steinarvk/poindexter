package integrationtest

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/steinarvk/poindexter/lib/dexapi"
)

func TestBasicBatching(t *testing.T) {
	if _, err := getRequest("api/ingest/main/batches/mybatch/", ExpectStatus(404)); err != nil {
		t.Fatalf("request error: %v", err)
	}

	if _, err := postRequest("api/ingest/main/batches/mybatch/jsonl/", ExpectStatus(200), WithBinaryBody([]byte(`
		{"id": "b3b3aa59-409c-40ba-ad2b-36ad83f72d71", "timestamp": 1715316569, "message": "hello world"}
	`))); err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	if _, err := getRequest("api/ingest/main/batches/mybatch/", ExpectStatus(200)); err != nil {
		t.Fatalf("request error: %v", err)
	}
}

func TestBatchChecking(t *testing.T) {
	if _, err := postRequest("api/ingest/main/batches/testbatch-1/jsonl/", ExpectStatus(200), WithBinaryBody([]byte(`
		{"id": "fe4add31-8bf0-4638-98d5-702ef7284dbf", "timestamp": 1715316569, "message": "hello world"}
	`))); err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	if _, err := postRequest("api/ingest/main/batches/testbatch-2/jsonl/", ExpectStatus(200), WithBinaryBody([]byte(`
		{"id": "88e9275e-5e7a-49a8-9d18-005b9c6e2b92", "timestamp": 1715316569, "message": "hello world"}
	`))); err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	resp, err := postRequest("api/ingest/main/batches/check/", ExpectStatus(200), WithBinaryBody([]byte(`
		{
			"batch_names": ["testbatch-1", "testbatch-2", "testbatch-3"]
		}
	`)))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	var checkBatchResponse dexapi.CheckBatchesResponse
	if err := json.Unmarshal(resp.RawBody, &checkBatchResponse); err != nil {
		t.Fatalf("json.Unmarshal error: %v", err)
	}

	want := `{
		"batches": [
			{
				"namespace": "main",
				"batch_name": "testbatch-1",
				"processed": true
			},
			{
				"namespace": "main",
				"batch_name": "testbatch-2",
				"processed": true
			},
			{
				"namespace": "main",
				"batch_name": "testbatch-3",
				"processed": false
			}
		]
	}`

	var wantResponse dexapi.CheckBatchesResponse
	if err := json.Unmarshal([]byte(want), &wantResponse); err != nil {
		t.Fatalf("json.Unmarshal error: %v", err)
	}

	if !reflect.DeepEqual(checkBatchResponse, wantResponse) {
		t.Fatalf("got: %v, want: %v", checkBatchResponse, wantResponse)
	}
}

func TestBatchCheckingInvalidRequest(t *testing.T) {
	if _, err := postRequest("api/ingest/main/batches/check/", ExpectStatus(400), WithBinaryBody([]byte(`
		{
			"this_is_not_a_valid_request": "nope",
			"batch_names": ["testbatch-1", "testbatch-2", "testbatch-3"]
		}
	`))); err != nil {
		t.Fatalf("postRequest error: %v", err)
	}
}
