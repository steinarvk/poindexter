package integrationtest

import "testing"

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
