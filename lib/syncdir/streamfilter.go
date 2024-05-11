package syncdir

import "context"

func filterSyncableFileStream(ctx context.Context, db *LocalDatabase, input <-chan SyncableFile, output chan<- SyncableFile) error {
	for sf := range input {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		yes, err := db.HasAlreadySyncedFile(ctx, sf)
		if err != nil {
			return err
		}

		if !yes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case output <- sf:
			}
		}
	}

	return nil
}

func filterBatchStream(ctx context.Context, db *LocalDatabase, input <-chan *Batch, output chan<- *Batch) error {
	for sb := range input {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		yes, err := db.HasAlreadySyncedBatch(ctx, sb.Header.Digest)
		if err != nil {
			return err
		}

		if !yes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case output <- sb:
			}
		}
	}

	return nil
}
