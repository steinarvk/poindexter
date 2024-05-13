package syncdir

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rjeczalik/notify"
	"github.com/steinarvk/poindexter/lib/logging"
	"go.uber.org/zap"
)

func watchDir(ctx context.Context, dir string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := logging.FromContext(ctx)

	recwatch := filepath.Join(dir, "...")
	logger.Info("watching directory", zap.String("dir", dir))

	watcherEventCh := make(chan notify.EventInfo, 10)
	if err := notify.Watch(recwatch, watcherEventCh, notify.All); err != nil {
		return err
	}

	type syncRequest struct {
		time   time.Time
		reason string
	}
	syncRequests := make(chan syncRequest, 10)
	syncRequests <- syncRequest{
		time:   time.Now(),
		reason: "startup",
	}

	debounceTime := 60 * time.Second

	go func() {
		var lastTrigger time.Time
		var nextTrigger time.Time
		var nextTriggerReason string
		var alreadySentSyncRequest time.Time

		var delayedTriggerInProgress bool
		delayedTriggerCh := make(chan struct{}, 1)
		defer close(delayedTriggerCh)

		for {
			select {
			case <-ctx.Done():
				return
			case <-delayedTriggerCh:
				if nextTrigger.After(alreadySentSyncRequest) {
					syncreq := syncRequest{
						time:   time.Now(),
						reason: nextTriggerReason,
					}
					alreadySentSyncRequest = syncreq.time
					nextTriggerReason = "???"
					delayedTriggerInProgress = false
					syncRequests <- syncreq
				}
			case evt := <-watcherEventCh:
				path := evt.Path()
				basename := filepath.Base(path)
				ignoreReason := ""
				for _, suffix := range alwaysExcludedSuffixes {
					if strings.HasSuffix(basename, suffix) {
						ignoreReason = "excluded_suffix"
						break
					}
				}
				if ignoreReason != "" {
					continue
				}

				// logger.Info("event", zap.String("path", evt.Path()), zap.String("event", evt.Event().String()))

				now := time.Now()
				var shouldTriggerNow bool
				if lastTrigger.IsZero() {
					shouldTriggerNow = true
				} else if timeLeft := now.Sub(lastTrigger); timeLeft > debounceTime {
					shouldTriggerNow = true
				} else {
					if delayedTriggerInProgress && now.After(nextTrigger) {
						// Update the existing delayedtrigger
						nextTrigger = now
						nextTriggerReason = fmt.Sprintf("[delayed] updated %q at %v", path, now.Format(time.RFC3339))
					} else if !delayedTriggerInProgress {
						// Start a new delayedtrigger
						delayedTriggerInProgress = true
						time.AfterFunc(timeLeft, func() {
							if ctx.Err() != nil {
								return
							}
							delayedTriggerCh <- struct{}{}
						})
					}
				}
				if shouldTriggerNow {
					syncRequests <- syncRequest{
						time:   now,
						reason: fmt.Sprintf("[immediate] updated %q at %v", path, now.Format(time.RFC3339)),
					}
					lastTrigger = now
				}
			}
		}
	}()

	var lastSync time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case syncreq := <-syncRequests:
			if syncreq.time.Before(lastSync) {
				continue
			}
			lastSync = time.Now()
			logger.Info("syncing", zap.Time("requested_time", syncreq.time), zap.String("reason", syncreq.reason))
			if err := SyncDirFromConfig(ctx, dir); err != nil {
				return err
			}
		}
	}
}

func WatchDirs(ctx context.Context, dirs []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(dirs) == 0 {
		return errors.New("no directories to watch")
	}

	errCh := make(chan error, 10)

	wg := sync.WaitGroup{}

	for _, dir := range dirs {
		wg.Add(1)
		go func(dir string) {
			defer wg.Done()
			if err := watchDir(ctx, dir); err != nil {
				errCh <- err
			}
		}(dir)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		return err
	}

	return nil
}
