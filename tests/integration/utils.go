package integrationtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	testPasswordsByUsername = map[string]string{
		"alice": "hunter2hunter2hunter2hunter2hunter2hunter2hunter2hunter2hunter2hunter2",
	}
)

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
	RawBody  []byte
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
		RawBody:  responseBody,
	}

	if err := json.Unmarshal(responseBody, &wrappedResp.Body); err != nil {
		return wrappedResp, fmt.Errorf("unmarshal API response error: %v\nraw value was:\n%s", err, string(responseBody))
	}

	return wrappedResp, nil
}

func insertRecords(records []string) error {
	for _, record := range records {
		var unmarshalled map[string]interface{}

		if err := json.Unmarshal([]byte(record), &unmarshalled); err != nil {
			return fmt.Errorf("insertRecords: JSON provided as string is invalid: %v\nJSON provided as string was: %s", err, record)
		}

		if _, ok := unmarshalled["timestamp"]; !ok {
			unmarshalled["timestamp"] = float64(time.Now().UnixNano()) / 1e9
		}

		if _, ok := unmarshalled["id"]; !ok {
			unmarshalled["id"] = uuid.New().String()
		}

		marshalled, err := json.Marshal(unmarshalled)
		if err != nil {
			return fmt.Errorf("insertRecords: JSON marshalling failed: %v\nOriginal JSON was: %s", err, record)
		}

		if _, err := postRequest("api/write/record/", WithJSON(string(marshalled))); err != nil {
			return err
		}
	}
	return nil
}

func WithBinaryBody(payload []byte) RequestOption {
	return func(opts *requestOptions) error {
		opts.body = bytes.NewBuffer(payload)
		return nil
	}
}
