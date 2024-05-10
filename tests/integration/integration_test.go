package integrationtest

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/steinarvk/poindexter/lib/config"
	"github.com/steinarvk/poindexter/lib/poindexterdb"
	"github.com/steinarvk/poindexter/lib/server"
	"go.uber.org/zap"
)

func TestGetHTTP(t *testing.T) {
	resp, err := http.Get("http://localhost:15244/")
	if err != nil {
		t.Fatal(err)
	}
	zap.L().Sugar().Infof("status %v", resp.StatusCode)
}

func TestInsertAndQuery(t *testing.T) {
	myUUID := uuid.New().String()

	resp, err := postRequest("api/ingest/main/record/", WithJSON(map[string]interface{}{
		"id":        myUUID,
		"timestamp": float64(time.Now().UnixNano()) / 1e9,
		"message":   "hello",
		"testfield": myUUID,
	}))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	zap.L().Sugar().Infof("status %v", resp.Response.StatusCode)
	zap.L().Sugar().Infof("response %v", resp.Body)

	resp, err = postRequest("api/query/main/records/", WithJSON(map[string]interface{}{
		"filter": map[string]interface{}{
			"testfield": myUUID,
		},
		"omit_superseded": false,
	}))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	zap.L().Sugar().Infof("status %v", resp.Response.StatusCode)
	zap.L().Sugar().Infof("response %v", resp.Body)
}

func testSetupRecords() error {
	if _, err := postRequest("api/ingest/main/record/", WithJSON(`
		{
			"id": "577255cd-d2b0-4ba5-abac-2373b8475801",
			"timestamp": 1715316569,
			"message": "hello world"
		}
	`)); err != nil {
		return err
	}

	if _, err := postRequest("api/ingest/main/record/", WithJSON(map[string]interface{}{
		"id":        uuid.New().String(),
		"timestamp": float64(time.Now().UnixNano()) / 1e9,
		"category":  "initial-test-records",
		"foo":       "bar",
		"object": map[string]interface{}{
			"string":           "helloworld",
			"array_of_strings": []string{"foo", "bar", "baz"},
			"integer":          42,
			"float":            3.14159,
		},
	})); err != nil {
		return err
	}

	if _, err := postRequest("api/ingest/main/record/", WithJSON(map[string]interface{}{
		"id":        uuid.New().String(),
		"timestamp": float64(time.Now().UnixNano()) / 1e9,
		"category":  "initial-test-records",
		"another":   "record",
	})); err != nil {
		return err
	}

	return nil
}

func TestMain(m *testing.M) {
	cmd := exec.Command("/bin/bash", "setup.sh")
	cmd.Dir = "./config"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("setup.sh: %v", err)
	}

	configFilename := "./config/poindexter-config.yaml"

	cfg, err := config.Load(configFilename)
	if err != nil {
		log.Fatal(err)
	}

	postgresCreds := poindexterdb.PostgresConfig{
		PostgresHost: "localhost",
		PostgresUser: "poindexter_test",
		PostgresPass: "testpassword",
		PostgresDB:   "poindexter_test",
	}
	os.Setenv("PGPORT", "15433")

	serv, err := server.New(
		server.WithConfig(*cfg),
		server.WithPostgresCreds(postgresCreds),
		server.WithHost("localhost"),
		server.WithPort(15244),
	)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := serv.Run(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server.Run(): %v", err)
		}
	}()

	time.Sleep(1 * time.Second)
	if err := testSetupRecords(); err != nil {
		log.Fatalf("testSetupRecords(): %v", err)
	}

	code := m.Run()

	if err := serv.HTTPServer().Shutdown(context.Background()); err != nil {
		log.Fatalf("HTTPServer().Shutdown(): %v", err)
	}

	wg.Wait()

	teardownCmd := exec.Command("/bin/bash", "teardown.sh")
	teardownCmd.Dir = "./config"
	teardownCmd.Stdout = os.Stdout
	teardownCmd.Stderr = os.Stderr
	if err := teardownCmd.Run(); err != nil {
		log.Fatalf("teardownCmd.Run(): %v", err)
	}

	os.Exit(code)
}
