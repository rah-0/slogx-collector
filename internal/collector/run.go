package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rah-0/slogx-collector/internal/journal"
	"github.com/rah-0/slogx-collector/internal/jsonx"
)

// Default collection settings are shared by Options and the command flags.
const (
	DefaultBatchSize        = 500
	DefaultBatchBytes       = 4 << 20
	DefaultFlushInterval    = time.Second
	DefaultRetryInterval    = time.Second
	DefaultMaxRetryInterval = 30 * time.Second
	DefaultShutdownTimeout  = 10 * time.Second
)

const retryBackoffMultiplier = 2

// Options configures collection. Zero numeric values and durations use the
// defaults described below. Negative values are invalid.
type Options struct {
	// JournalDir is required and stores records until delivery is acknowledged.
	// Only one collector may use a journal directory at a time.
	JournalDir string
	// JournalKey optionally binds the journal to a destination identity.
	JournalKey string
	// Fields adds string fields to new records, replacing matching input keys.
	// Fields are captured once per Run and persisted with the record; replayed
	// records retain their original fields. An empty map leaves records unchanged.
	Fields map[string]string
	// BatchSize defaults to 500 records.
	BatchSize int
	// BatchBytes defaults to 4 MiB of raw JSON, excluding transport framing.
	// A record larger than this target is delivered alone.
	BatchBytes int
	// FlushInterval defaults to one second after a batch starts accumulating.
	FlushInterval time.Duration
	// RetryInterval defaults to one second and grows exponentially.
	RetryInterval time.Duration
	// MaxRetryInterval caps the exponential backoff at 30 seconds by default.
	// A destination's RetryError.After can require a longer delay.
	MaxRetryInterval time.Duration
	// MaxRetries limits retries after the initial attempt. Zero is unlimited.
	MaxRetries int
	// ShutdownTimeout defaults to ten seconds to deliver journaled records
	// after cancellation. Undelivered records remain in the journal.
	ShutdownTimeout time.Duration
	// OnReady runs once after the journal opens, before ingestion or delivery.
	// It must return promptly. An error aborts collection and closes the journal.
	// Readiness does not verify destination availability or authentication.
	OnReady func() error
	// OnRetry is called before a retry delay. It must return promptly.
	OnRetry func(error, time.Duration)
}

func (o Options) normalized() (Options, error) {
	if o.JournalDir == "" {
		return o, journal.ErrDirRequired
	}
	if o.BatchSize < 0 || o.BatchBytes < 0 || o.FlushInterval < 0 ||
		o.RetryInterval < 0 || o.MaxRetryInterval < 0 || o.MaxRetries < 0 || o.ShutdownTimeout < 0 {
		return o, ErrNegativeOption
	}
	if o.BatchSize == 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.BatchBytes == 0 {
		o.BatchBytes = DefaultBatchBytes
	}
	if o.FlushInterval == 0 {
		o.FlushInterval = DefaultFlushInterval
	}
	if o.RetryInterval == 0 {
		o.RetryInterval = DefaultRetryInterval
	}
	if o.MaxRetryInterval == 0 {
		o.MaxRetryInterval = DefaultMaxRetryInterval
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = DefaultShutdownTimeout
	}
	if o.RetryInterval > o.MaxRetryInterval {
		return o, ErrRetryIntervalExceedsMax
	}
	return o, nil
}

// Run durably journals newline-delimited JSON objects and delivers ordered
// batches. Ingestion continues while delivery retries; disk use grows with the
// undelivered backlog. Memory holds the current input record and delivery batch;
// the journal also keeps small metadata entries for its disk segments.
// Journal writes are synced at 500 records, 4 MiB, or the one-second tick before
// delivery can read them. Successful sends are checkpointed in groups of up to
// 16 batches or on the one-second tick, including while EOF delivery drains.
// EOF and cancellation sync pending writes before draining; abrupt termination
// can lose writes since the last successful sync. Close checkpoints successful
// sends; a crash can replay every batch since the last durable checkpoint.
// Delivery is at least once: retries or a crash before acknowledgement can cause
// duplicates. An input error allows ShutdownTimeout to drain preceding records.
// A journal write failure stops collection immediately, preserving the backlog.
//
// Run owns input and closes it on every return, joining close failures with any
// collection error. Its Close method must unblock Read. Cancellation stops
// ingestion and allows ShutdownTimeout to drain the journal; any remaining
// records are replayed by the next Run using that journal.
func Run(ctx context.Context, input io.ReadCloser, destination Destination, options Options) (result error) {
	if input == nil {
		return ErrInputRequired
	}
	closeInput := sync.OnceValue(input.Close)
	defer func() {
		if err := closeInput(); err != nil {
			result = errors.Join(result, err)
		}
	}()
	if destination == nil {
		return ErrDestinationRequired
	}
	options, err := options.normalized()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	fields := jsonx.EncodeFields(options.Fields)
	store, err := journal.Open(options.JournalDir, options.JournalKey)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, store.Close()) }()
	if options.OnReady != nil {
		if err := options.OnReady(); err != nil {
			return fmt.Errorf("collector: signal readiness: %w", err)
		}
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopInput := func() {
		cancel()
		closeInput() // The deferred call above joins the saved close error.
	}
	// Journal maintenance outlives input EOF and caller cancellation so the
	// shutdown drain can keep checkpointing. A sync failure cancels both paths.
	journalCtx, stopJournal := context.WithCancelCause(context.WithoutCancel(ctx))
	journalDone := make(chan error, 1)
	go func() {
		err := syncJournal(journalCtx, store)
		if err != nil {
			stopJournal(err)
			stopInput()
		}
		journalDone <- err
	}()
	defer func() {
		stopJournal(nil)
		result = errors.Join(result, <-journalDone)
	}()

	// Receiving the result also marks EOF for delivery; nil disables the case.
	producerDone := make(chan error, 1)
	go func(done chan<- error) {
		err := ingest(workCtx, input, store, fields)
		if err != nil {
			cancel()
		}
		done <- err
	}(producerDone)
	var producerErr error
	stopProducer := func() {
		stopInput()
		if producerDone != nil {
			producerErr = <-producerDone
			producerDone = nil
		}
	}
	defer stopProducer()

	var batch []json.RawMessage
	batchBytes := 0
	var batchStarted time.Time
	flush := func(sendCtx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		if err := sendBatch(sendCtx, destination, batch, options); err != nil {
			return err
		}
		if err := store.Ack(); err != nil {
			return &terminalError{err}
		}
		clear(batch)
		batch = batch[:0]
		batchBytes = 0
		batchStarted = time.Time{}
		return nil
	}

	consume := func(sendCtx context.Context) error {
		timer := time.NewTimer(options.FlushInterval)
		timer.Stop()
		defer timer.Stop()
		for {
			if err := sendCtx.Err(); err != nil {
				return err
			}
			remainingBytes := 0
			if len(batch) != 0 {
				remainingBytes = options.BatchBytes - batchBytes
			}
			record, err := store.Next(remainingBytes)
			if err == nil {
				if len(batch) == 0 {
					batchStarted = time.Now()
				}
				batch = append(batch, record)
				batchBytes += len(record)
				if len(batch) >= options.BatchSize || batchBytes >= options.BatchBytes ||
					time.Since(batchStarted) >= options.FlushInterval {
					if err := flush(sendCtx); err != nil {
						return err
					}
				}
				continue
			}
			if errors.Is(err, journal.ErrBatchFull) {
				if err := flush(sendCtx); err != nil {
					return err
				}
				continue
			}
			if !errors.Is(err, io.EOF) {
				return &terminalError{err}
			}
			if producerDone == nil {
				return flush(sendCtx)
			}
			var tick <-chan time.Time
			if len(batch) > 0 {
				timer.Reset(max(0, options.FlushInterval-time.Since(batchStarted)))
				tick = timer.C
			}
			select {
			case <-sendCtx.Done():
				return sendCtx.Err()
			case producerErr = <-producerDone:
				producerDone = nil
			case <-store.Notify():
			case <-tick:
				if err := flush(sendCtx); err != nil {
					return err
				}
			}
		}
	}

	err = consume(workCtx)
	stopProducer()
	if _, ok := errors.AsType[*terminalError](err); ok {
		return errors.Join(ctx.Err(), producerErr, err)
	}
	if _, ok := errors.AsType[*terminalError](producerErr); ok {
		return errors.Join(ctx.Err(), producerErr)
	}
	if ctx.Err() != nil || producerErr != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(journalCtx, options.ShutdownTimeout)
		defer cancelShutdown()
		// A failed send leaves this batch in memory and advances the journal's
		// read cursor. Flush it before reading any more records during shutdown.
		drainErr := flush(shutdownCtx)
		if drainErr == nil {
			drainErr = consume(shutdownCtx)
		}
		return errors.Join(ctx.Err(), producerErr, drainErr)
	}
	return err
}

func sendBatch(ctx context.Context, destination Destination, batch []json.RawMessage, options Options) error {
	backoff := options.RetryInterval
	for retries := 0; ; retries++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := destination.Send(ctx, batch)
		if err == nil {
			return nil
		}
		retry, ok := errors.AsType[*RetryError](err)
		if !ok {
			if ctx.Err() != nil && cancellationOnly(err, ctx.Err()) {
				return errors.Join(ctx.Err(), err)
			}
			return &terminalError{fmt.Errorf("collector: send batch: %w", err)}
		}
		if options.MaxRetries > 0 && retries >= options.MaxRetries {
			return &terminalError{fmt.Errorf("collector: send batch: %w", err)}
		}
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}
		delay := max(backoff, retry.After)
		if options.OnRetry != nil {
			options.OnRetry(err, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff > options.MaxRetryInterval/retryBackoffMultiplier {
			backoff = options.MaxRetryInterval
		} else {
			backoff *= retryBackoffMultiplier
		}
	}
}
