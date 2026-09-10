package collector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/rah-0/slogx-collector/internal/journal"
	"github.com/rah-0/slogx-collector/internal/jsonx"
)

const journalSyncInterval = time.Second

// ingest syncs journal writes while readInput runs independently of the timer.
// stopInput cancels collection and unblocks the reader if syncing fails.
func ingest(ctx context.Context, input io.Reader, store *journal.Journal, fields map[string]json.RawMessage, stopInput func()) error {
	readDone := make(chan error, 1)
	go func() { readDone <- readInput(ctx, input, store, fields) }()

	ticker := time.NewTicker(journalSyncInterval)
	defer ticker.Stop()
	var err error
	for {
		select {
		case err = <-readDone:
		case <-ticker.C:
			syncErr := store.Sync()
			if syncErr == nil {
				continue
			}
			stopInput()
			err = errors.Join(&terminalError{syncErr}, <-readDone)
		}
		break
	}

	// The reader has stopped. Commit its last complete records before Run can
	// observe EOF or begin its shutdown drain, preserving any earlier sync error.
	if syncErr := store.Sync(); syncErr != nil {
		err = errors.Join(err, &terminalError{syncErr})
	}
	return err
}

func readInput(ctx context.Context, input io.Reader, store *journal.Journal, fields map[string]json.RawMessage) error {
	reader := jsonx.NewReader(input)
	for ctx.Err() == nil {
		record, err := reader.Next()
		if ctx.Err() != nil || errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(fields) != 0 {
			record, err = jsonx.AddFields(record, fields)
			if err != nil {
				return err
			}
		}
		if err := store.Append(record); err != nil {
			return &terminalError{err}
		}
	}
	return nil
}
