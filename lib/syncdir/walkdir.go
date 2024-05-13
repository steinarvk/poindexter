package syncdir

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/steinarvk/poindexter/lib/logging"
)

func walkDirectory(ctx context.Context, config DirectoryConfig, output chan<- SyncableFile) (int64, error) {
	var numProcessed int64

	logger := logging.FromContext(ctx)

	err := filepath.WalkDir(config.RootDirectory, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if d.IsDir() {
			return nil
		}

		if !config.BaseNameRegexp.MatchString(d.Name()) {
			return nil
		}

		alwaysExcludedSuffixes := []string{
			".tmp",
			".sqlite3",
			".sqlite",
			".yaml",
			".yml",
		}
		for _, suffix := range alwaysExcludedSuffixes {
			if strings.HasSuffix(strings.ToLower(d.Name()), suffix) {
				logger.Sugar().Infof("skipping file %q even though it is matched by regexp pattern", path)
				return nil
			}
		}

		fullPath := path
		info, err := d.Info()
		if err != nil {
			return err
		}

		logger.Sugar().Infof("checking file %q", fullPath)

		item := SyncableFile{
			Filename: fullPath,
			Mtime:    info.ModTime(),
			Size:     info.Size(),
		}
		numProcessed++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case output <- item:
		}

		return nil
	})

	return numProcessed, err
}
