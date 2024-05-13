package syncdir

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

type LocalDatabase struct {
	db             *sql.DB
	targetHostname string
}

func openDirectoryLocalDatabase(cfg DirectoryConfig) (*LocalDatabase, error) {
	databasePath := filepath.Join(cfg.RootDirectory, "poindexter.sqlite3")
	db, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		return nil, err
	}

	// Check if the database file exists, if not, create it and initialize tables
	if _, err := os.Stat(databasePath); os.IsNotExist(err) {
		if err = createTables(db); err != nil {
			return nil, err
		}
	}

	return &LocalDatabase{
		db:             db,
		targetHostname: cfg.ClientConfig.Host,
	}, nil
}

func createTables(db *sql.DB) error {
	createSyncedFiles := `
	CREATE TABLE IF NOT EXISTS synced_files (
		filename TEXT,
		mtime DATETIME,
		filesize INTEGER,
		sync_target TEXT,
		sync_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		UNIQUE (filename, mtime, filesize, sync_target)
	);`

	createSyncedBatches := `
	CREATE TABLE IF NOT EXISTS synced_batches (
		batch_name TEXT,
		sync_target TEXT,
		sync_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		UNIQUE (batch_name, sync_target)
	);`

	_, err := db.Exec(createSyncedFiles)
	if err != nil {
		return err
	}

	_, err = db.Exec(createSyncedBatches)
	if err != nil {
		return err
	}

	return nil
}

func (l *LocalDatabase) Close() error {
	return l.db.Close()
}

func (l *LocalDatabase) HasAlreadySyncedFile(ctx context.Context, syncable SyncableFile) (bool, error) {
	var exists int
	query := `
		SELECT COUNT(*)
		FROM synced_files
		WHERE filename = ?
		AND mtime = ?
		AND filesize = ?
		AND sync_target = ?;
	`
	err := l.db.QueryRowContext(
		ctx,
		query,
		syncable.Filename,
		syncable.Mtime,
		syncable.Size,
		l.targetHostname,
	).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

func (l *LocalDatabase) HasAlreadySyncedBatch(ctx context.Context, batchName string) (bool, error) {
	var exists int
	query := `
		SELECT COUNT(*)
		FROM synced_batches
		WHERE batch_name = ?
		AND   sync_target = ?
	`
	err := l.db.QueryRowContext(ctx, query, batchName, l.targetHostname).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

func (l *LocalDatabase) MarkFileAsSynced(ctx context.Context, syncable SyncableFile) error {
	query := `
		INSERT OR IGNORE INTO synced_files
			(filename, mtime, filesize, sync_target)
		VALUES
			(?, ?, ?, ?)
	`
	_, err := l.db.ExecContext(
		ctx,
		query,
		syncable.Filename,
		syncable.Mtime,
		syncable.Size,
		l.targetHostname,
	)
	return err
}

func (l *LocalDatabase) MarkBatchAsSynced(ctx context.Context, batchName string) error {
	query := `
		INSERT OR IGNORE INTO synced_batches
			(batch_name, sync_target)
		VALUES
			(?, ?)
		`
	_, err := l.db.ExecContext(ctx, query, batchName, l.targetHostname)
	return err
}
