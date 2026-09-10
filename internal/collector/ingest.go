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

// ingest commits the last complete records before reporting EOF or a read error.
func ingest(ctx context.Context, input io.Reader, store *journal.Journal, fields map[string]json.RawMessage) error {
	err := readInput(ctx, input, store, fields)
	if syncErr := store.Sync(); syncErr != nil {
		err = errors.Join(err, &terminalError{syncErr})
	}
	return err
}

// syncJournal bounds the wait for partial write groups and acknowledgements.
// It remains active while ingestion, delivery, or the shutdown drain is running.
func syncJournal(ctx context.Context, store *journal.Journal) error {
	ticker := time.NewTicker(journalSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := store.Sync(); err != nil {
				return &terminalError{err}
			}
		}
	}
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
