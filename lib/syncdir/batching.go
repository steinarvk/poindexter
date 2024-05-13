package syncdir

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"

	"github.com/steinarvk/poindexter/lib/logging"
	"go.uber.org/zap"
)

const (
	maxBatchByteSize = 1024 * 1024
	maxLineLength    = 1024 * 1024
)

func readBatchesInFile(ctx context.Context, sf SyncableFile, output chan<- *Batch) error {
	logger := logging.FromContext(ctx)
	logger.Debug("reading batches in file", zap.String("filename", sf.Filename))

	f, err := os.Open(sf.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, maxLineLength)
	scanner.Buffer(buf, maxLineLength)

	batchbuf := bytes.NewBuffer(nil)
	var numBatches int
	var numLines int

	flush := func(final bool) error {
		if batchbuf.Len() == 0 && !final {
			return nil
		}

		digest := fmt.Sprintf("%x", sha256.Sum256(batchbuf.Bytes()))

		batch := &Batch{
			Header: BatchHeader{
				File:      sf,
				Index:     numBatches,
				Final:     final,
				Digest:    digest,
				ItemCount: int64(numLines),
			},
			Data: batchbuf.Bytes(),
		}

		output <- batch

		batchbuf = bytes.NewBuffer(nil)
		numBatches++
		numLines = 0

		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		newSize := batchbuf.Len() + len(line) + 1
		if newSize > maxBatchByteSize {
			if err := flush(false); err != nil {
				return err
			}
		}

		// TODO parse -- verify JSON, deal with incomplete lines?
		// (incomplete final line is not an error, but incomplete other line should be)

		if batchbuf.Len() > 0 {
			batchbuf.WriteByte('\n')
		}
		batchbuf.Write([]byte(line))
		numLines++
	}

	if err := flush(true); err != nil {
		return err
	}

	return nil
}

func readBatchesInFileStream(ctx context.Context, input <-chan SyncableFile, output chan<- *Batch) error {
	for sf := range input {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := readBatchesInFile(ctx, sf, output); err != nil {
			return err
		}
	}

	return nil
}
