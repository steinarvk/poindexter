package integrationtest

import (
	"context"
	"os"
	"regexp"
	"testing"

	"github.com/steinarvk/poindexter/lib/syncdir"
)

func TestSyncDirSimpleTestFile(t *testing.T) {
	dexclient := syncdir.ClientConfig{
		Scheme:    "http",
		Host:      "localhost",
		Port:      15244,
		Namespace: "main",
		User:      "alice",
		Password:  testPasswordsByUsername["alice"],
	}

	databaseFilename := "./testdata/simple-test-file/poindexter.sqlite3"
	if _, err := os.Stat(databaseFilename); err == nil {
		if err := os.Remove(databaseFilename); err != nil {
			t.Fatalf("os.Remove failed: %v", err)
		}
	}

	dircfg := syncdir.DirectoryConfig{
		RootDirectory:  "./testdata/simple-test-file/",
		BaseNameRegexp: regexp.MustCompile(`^*.(jsonl|jsonlines)$`),
		ClientConfig:   dexclient,
	}

	if _, err := getRequest("api/query/main/records/c1c1682c-fcfe-40f5-9e62-1808750df22d/", ExpectStatus(404)); err != nil {
		t.Fatalf("pre-check request error: %v", err)
	}

	if err := syncdir.SyncDir(context.Background(), dircfg); err != nil {
		t.Fatalf("syncdir.SyncDir failed: %v", err)
	}

	if _, err := getRequest("api/query/main/records/c1c1682c-fcfe-40f5-9e62-1808750df22d/", ExpectStatus(200)); err != nil {
		t.Fatalf("request error: %v", err)
	}
}
