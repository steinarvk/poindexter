package syncdir

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/steinarvk/poindexter/lib/logging"
	"go.uber.org/zap"
)

type PoindexterClient struct {
	Scheme string
	Host   string
	Port   int

	User     string
	Password string

	Namespace string
}

func (c *PoindexterClient) SyncBatch(ctx context.Context, batch *Batch) error {
	logger := logging.FromContext(ctx)

	if c.Host == "" {
		return fmt.Errorf("no host specified")
	}
	if c.Namespace == "" {
		return fmt.Errorf("no namespace specified")
	}
	if c.User == "" {
		return fmt.Errorf("no user specified")
	}
	if c.Password == "" {
		return fmt.Errorf("no password specified")
	}

	scheme := c.Scheme
	if scheme == "" {
		scheme = "https"
	}

	var prefix string
	if c.Port != 0 {
		prefix = fmt.Sprintf("%s://%s:%d/api", scheme, c.Host, c.Port)
	} else {
		prefix = fmt.Sprintf("%s://%s/api", scheme, c.Host)
	}
	prefix += "/ingest/" + c.Namespace
	prefix += "/batches/" + batch.Header.Digest

	client := &http.Client{}

	checkURL := prefix + "/"

	logger.Info("checking batch", zap.String("digest", batch.Header.Digest), zap.String("url", checkURL))
	checkRequest, err := http.NewRequest("GET", checkURL, nil)
	if err != nil {
		return err
	}
	checkRequest.SetBasicAuth(c.User, c.Password)
	checkResp, err := client.Do(checkRequest)
	if err != nil {
		logger.Info("error checking batch", zap.String("digest", batch.Header.Digest), zap.String("url", checkURL))
		return err
	}

	switch checkResp.StatusCode {
	case 200:
		logger.Info("batch already exists", zap.String("digest", batch.Header.Digest))
		return nil
	case 404:
		logger.Info("batch does not exist", zap.String("digest", batch.Header.Digest))
	default:
		logger.Warn("unexpected status code", zap.String("digest", batch.Header.Digest), zap.Int("status", checkResp.StatusCode), zap.String("url", checkURL))
		return fmt.Errorf("unexpected status code %v", checkResp.StatusCode)
	}

	postURL := prefix + "/jsonl/"

	logger.Info("posting batch", zap.String("digest", batch.Header.Digest), zap.String("url", postURL))

	postRequest, err := http.NewRequest("POST", postURL, bytes.NewReader(batch.Data))
	if err != nil {
		return err
	}
	postRequest.SetBasicAuth(c.User, c.Password)

	postResp, err := client.Do(postRequest)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}

	switch postResp.StatusCode {
	case 202:
		fallthrough
	case 201:
		fallthrough
	case 200:
		logger.Info("successfully posted batch", zap.String("digest", batch.Header.Digest), zap.Int("status", postResp.StatusCode), zap.Int("size", len(batch.Data)))
	default:
		logger.Warn("unexpected status code", zap.String("digest", batch.Header.Digest), zap.Int("status", checkResp.StatusCode), zap.String("url", postURL))
		return fmt.Errorf("unexpected status code %v", checkResp.StatusCode)
	}

	return nil
}
