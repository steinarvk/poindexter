package syncdir

import (
	"context"
)

// TODO replace dummy implementation with real one

type LocalDatabase struct {
}

func openDirectoryLocalDatabase(cfg DirectoryConfig) (*LocalDatabase, error) {
	return nil, nil
}

func (l *LocalDatabase) Close() error {
	return nil
}

func (l *LocalDatabase) HasAlreadySyncedFile(ctx context.Context, syncable SyncableFile) (bool, error) {
	return false, nil
}

func (l *LocalDatabase) HasAlreadySyncedBatch(ctx context.Context, batchName string) (bool, error) {
	return false, nil
}

func (l *LocalDatabase) MarkFileAsSynced(ctx context.Context, syncable SyncableFile) error {
	return nil
}

func (l *LocalDatabase) MarkBatchAsSynced(ctx context.Context, batchName string) error {
	return nil
}
