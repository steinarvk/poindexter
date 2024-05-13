package dexclient

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type Request struct {
	Method  string
	Path    string
	Request *http.Request
}

func (c *Client) APIBase() string {
	scheme := c.Scheme
	if scheme == "" {
		scheme = "https"
	}

	if c.Port != 0 {
		return fmt.Sprintf("%s://%s:%d/api", scheme, c.Host, c.Port)
	} else {
		return fmt.Sprintf("%s://%s/api", scheme, c.Host)
	}
}

func (c *Client) APIPath(suffix string) string {
	base := c.APIBase()
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(suffix, "/")
}

func (c *Client) NewRequest(ctx context.Context, method string, path string) (*Request, error) {
	if !strings.Contains(path, "://") {
		path = c.APIPath(path)
	}

	req, err := http.NewRequestWithContext(ctx, method, path, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.User, c.Password)

	return &Request{
		Method:  method,
		Path:    path,
		Request: req,
	}, nil
}

func (c *Client) Do(ctx context.Context, req *Request) (*http.Response, error) {
	client := &http.Client{}

	return client.Do(req.Request)
}
