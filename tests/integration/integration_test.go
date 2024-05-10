package integrationtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/steinarvk/poindexter/lib/config"
	"github.com/steinarvk/poindexter/lib/server"
)

var (
	testPasswordsByUsername = map[string]string{
		"alice": "hunter2hunter2hunter2hunter2hunter2hunter2hunter2hunter2hunter2hunter2",
	}
)

func TestGetHTTP(t *testing.T) {
	resp, err := http.Get("http://localhost:15244/")
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("status %v", resp.StatusCode)
}

type requestOptions struct {
	host             string
	body             *bytes.Buffer
	user             string
	password         string
	namespace        string
	contentType      string
	allowError       bool
	expectStatusCode int
}

func newDefaultRequestOptions() requestOptions {
	return requestOptions{
		host:      "http://localhost:15244/",
		user:      "alice",
		password:  testPasswordsByUsername["alice"],
		namespace: "main",
	}
}

func ExpectStatus(status int) RequestOption {
	return func(opts *requestOptions) error {
		opts.allowError = true
		opts.expectStatusCode = status
		return nil
	}
}

func WithJSON(payload interface{}) RequestOption {
	stringValue, ok := payload.(string)
	if ok {
		var unmarshalled map[string]interface{}
		if err := json.Unmarshal([]byte(stringValue), &unmarshalled); err != nil {
			return func(opts *requestOptions) error {
				return fmt.Errorf("WithJSON: JSON provided as string is invalid: %v\nJSON provided as string was: %s", err, stringValue)
			}
		}

		return func(opts *requestOptions) error {
			opts.body = bytes.NewBuffer([]byte(stringValue))
			opts.contentType = "application/json"
			return nil
		}
	}

	marshalled, err := json.Marshal(payload)

	return func(opts *requestOptions) error {
		if err != nil {
			return fmt.Errorf("marshal error: %v", err)
		}
		opts.body = bytes.NewBuffer(marshalled)
		opts.contentType = "application/json"
		return nil
	}
}

type RequestOption func(*requestOptions) error

type Response struct {
	Response *http.Response
	Body     map[string]interface{}
}

func postRequest(endpoint string, opts ...RequestOption) (*Response, error) {
	reqOpts := newDefaultRequestOptions()
	for _, opt := range opts {
		if err := opt(&reqOpts); err != nil {
			return nil, err
		}
	}

	url := strings.TrimRight(reqOpts.host, "/") + "/" + strings.TrimLeft(endpoint, "/")

	req, err := http.NewRequest("POST", url, reqOpts.body)
	if err != nil {
		return nil, fmt.Errorf("NewRequest error: %v", err)
	}
	req.Header.Set("Content-Type", reqOpts.contentType)
	req.Header.Set("X-Namespace", reqOpts.namespace)
	req.SetBasicAuth(reqOpts.user, reqOpts.password)

	t0 := time.Now()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request.Do error: %v", err)
	}

	duration := time.Since(t0)

	responseContentType := resp.Header.Get("Content-Type")

	var responseBody []byte
	if resp.Body != nil {
		defer resp.Body.Close()
		responseBody, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("ReadAll error: %v", err)
		}
	}

	log.Printf("==> POST %v [%v %v] %v", url, resp.StatusCode, responseContentType, duration)

	if reqOpts.expectStatusCode != 0 && resp.StatusCode != reqOpts.expectStatusCode {
		return nil, fmt.Errorf("unexpected status code %v (wanted %v)\nresponse body:\n%s", resp.StatusCode, reqOpts.expectStatusCode, (responseBody))
	}

	if !reqOpts.allowError && (resp.StatusCode < 200 || resp.StatusCode >= 400) {
		return nil, fmt.Errorf("unexpected status code %v\nresponse body:\n%s", resp.StatusCode, string(responseBody))
	}

	wrappedResp := &Response{
		Response: resp,
	}

	if err := json.Unmarshal(responseBody, &wrappedResp.Body); err != nil {
		return wrappedResp, fmt.Errorf("unmarshal API response error: %v\nraw value was:\n%s", err, string(responseBody))
	}

	return wrappedResp, nil
}

func TestInsertAndQuery(t *testing.T) {
	myUUID := uuid.New().String()

	resp, err := postRequest("api/write/record/", WithJSON(map[string]interface{}{
		"id":        myUUID,
		"timestamp": float64(time.Now().UnixNano()) / 1e9,
		"message":   "hello",
		"testfield": myUUID,
	}))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	log.Printf("status %v", resp.Response.StatusCode)
	log.Printf("response %v", resp.Body)

	resp, err = postRequest("api/read/records/", WithJSON(map[string]interface{}{
		"filter": map[string]interface{}{
			"testfield": myUUID,
		},
		"omit_superseded": false,
	}))
	if err != nil {
		t.Fatalf("postRequest error: %v", err)
	}

	log.Printf("status %v", resp.Response.StatusCode)
	log.Printf("response %v", resp.Body)
}

func testSetupRecords() error {
	if _, err := postRequest("api/write/record/", WithJSON(`
		{
			"id": "577255cd-d2b0-4ba5-abac-2373b8475801",
			"timestamp": 1715316569,
			"message": "hello world"
		}
	`)); err != nil {
		return err
	}

	if _, err := postRequest("api/write/record/", WithJSON(map[string]interface{}{
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

	if _, err := postRequest("api/write/record/", WithJSON(map[string]interface{}{
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

	os.Setenv("PGHOST", "localhost")
	os.Setenv("PGUSER", "poindexter_test")
	os.Setenv("PGPASSWORD", "testpassword")
	os.Setenv("PGDATABASE", "poindexter_test")
	os.Setenv("PGPORT", "15433")

	serv, err := server.New(
		server.WithConfig(*cfg),
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
