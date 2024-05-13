package syncdir

import (
	"context"
	"sync"
	"time"

	"github.com/steinarvk/poindexter/lib/logging"
	"go.uber.org/zap"
)

// Keep a sqlite database in the directory with, for each file:
//   - filename
//   - mtime at the beginning of last (successfully completed) sync
//   - file size at the beginning of last (successfully completed) sync
// Scan through the actual directory to find files that have changed since last sync.
// Now produce a stream of batches of JSONL data (size-one channel).
// For each file that has changed:
// - One batch should be up to a certain limit (one megabyte?) of JSONL data.
// Check whether the server already has the batch.
// Otherwise, submit the batch identified by digest.

type SyncableFile struct {
	Filename string
	Mtime    time.Time
	Size     int64
}

type BatchHeader struct {
	File      SyncableFile
	Index     int
	Final     bool
	Digest    string
	ItemCount int64
}

type Batch struct {
	Header BatchHeader
	Data   []byte
}

func SyncDirFromConfig(ctx context.Context, configSource string) error {
	cfg, err := ReadConfig(configSource)
	if err != nil {
		return err
	}

	return SyncDir(ctx, *cfg)
}

func SyncDir(ctx context.Context, config DirectoryConfig) error {
	client := PoindexterClient{config.ClientConfig}
	logger := logging.FromContext(ctx)
	logger.Sugar().Infof("syncing directory %q", config.RootDirectory)
	err := syncDir(ctx, config, func(ctx context.Context, batch *Batch) error {
		logger := logging.FromContext(ctx)
		logger.Info("syncing batch",
			zap.String("filename", batch.Header.File.Filename),
			zap.String("digest", batch.Header.Digest),
			zap.Int("index", batch.Header.Index),
			zap.Bool("final", batch.Header.Final),
			zap.Int64("item_count", batch.Header.ItemCount),
		)
		return client.SyncBatch(ctx, batch)
	})
	logger.Info("result of syncing directory", zap.String("root", config.RootDirectory), zap.Error(err))
	return err
}

func syncDir(ctx context.Context, config DirectoryConfig, syncBatch func(context.Context, *Batch) error) error {
	logger := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	localdb, err := openDirectoryLocalDatabase(config)
	if err != nil {
		return err
	}
	defer localdb.Close()

	errCh := make(chan error, 1)
	candidateSyncableFileCh := make(chan SyncableFile, 100)
	confirmedSyncableCh := make(chan SyncableFile, 100)
	candidateBatchCh := make(chan *Batch, 1)
	confirmedBatchCh := make(chan *Batch, 1)
	syncedBatchHeaderCh := make(chan BatchHeader, 100)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer close(candidateSyncableFileCh)
		defer wg.Done()

		_, err := walkDirectory(ctx, config, candidateSyncableFileCh)
		if err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer close(confirmedSyncableCh)
		defer wg.Done()

		if err := filterSyncableFileStream(ctx, localdb, candidateSyncableFileCh, confirmedSyncableCh); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer close(candidateBatchCh)
		defer wg.Done()

		if err := readBatchesInFileStream(ctx, confirmedSyncableCh, candidateBatchCh); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer close(confirmedBatchCh)
		defer wg.Done()

		if err := filterBatchStream(ctx, localdb, candidateBatchCh, confirmedBatchCh); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer close(syncedBatchHeaderCh)
		defer wg.Done()

		for batch := range confirmedBatchCh {
			if err := syncBatch(ctx, batch); err != nil {
				errCh <- err
				return
			}

			if err := localdb.MarkBatchAsSynced(ctx, batch.Header.Digest); err != nil {
				errCh <- err
				return
			}

			syncedBatchHeaderCh <- batch.Header
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		countByFilename := map[string]int{}
		numBatchesByFilename := map[string]int{}

		for header := range syncedBatchHeaderCh {
			k := header.File.Filename
			countByFilename[k]++
			if header.Final {
				numBatchesByFilename[k] = header.Index + 1
			}

			if countByFilename[k] == numBatchesByFilename[k] {
				if err := localdb.MarkFileAsSynced(ctx, header.File); err != nil {
					errCh <- err
					return
				}

				logger.Info("marked file as synced", zap.String("filename", header.File.Filename))
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		cancel()
		return err
	}

	return nil
}
