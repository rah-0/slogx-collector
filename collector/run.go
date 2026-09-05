package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Options configures collection. Zero numeric values and durations use the
// defaults described below. Negative values are invalid.
type Options struct {
	// JournalDir is required and stores records until delivery is acknowledged.
	// Only one collector may use a journal directory at a time.
	JournalDir string
	// JournalKey optionally binds the journal to a destination identity.
	JournalKey string
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
	// OnRetry is called before a retry delay. It must return promptly.
	OnRetry func(error, time.Duration)
}

func (o Options) normalized() (Options, error) {
	if o.JournalDir == "" {
		return o, ErrJournalDirRequired
	}
	if o.BatchSize < 0 || o.BatchBytes < 0 || o.FlushInterval < 0 ||
		o.RetryInterval < 0 || o.MaxRetryInterval < 0 || o.MaxRetries < 0 || o.ShutdownTimeout < 0 {
		return o, ErrNegativeOption
	}
	if o.BatchSize == 0 {
		o.BatchSize = 500
	}
	if o.BatchBytes == 0 {
		o.BatchBytes = 4 * 1024 * 1024
	}
	if o.FlushInterval == 0 {
		o.FlushInterval = time.Second
	}
	if o.RetryInterval == 0 {
		o.RetryInterval = time.Second
	}
	if o.MaxRetryInterval == 0 {
		o.MaxRetryInterval = 30 * time.Second
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = 10 * time.Second
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
// A successful Send is acknowledged on disk before the next batch is read.
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
	journal, err := openJournal(options.JournalDir, options.JournalKey)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, journal.close()) }()

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Receiving the result also marks EOF for delivery; nil disables the case.
	producerDone := make(chan error, 1)
	go func(done chan<- error) {
		err := ingest(workCtx, input, journal)
		if err != nil {
			cancel()
		}
		done <- err
	}(producerDone)
	var producerErr error
	stopProducer := func() {
		cancel()
		closeInput() // The deferred call above joins the saved close error.
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
		if err := journal.ack(); err != nil {
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
			// Readiness must be captured before inspecting the journal so an
			// append between next and select cannot leave the consumer asleep.
			changed := journal.changed()
			remainingBytes := 0
			if len(batch) != 0 {
				remainingBytes = options.BatchBytes - batchBytes
			}
			record, err := journal.next(remainingBytes)
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
			if errors.Is(err, errBatchFull) {
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
			case <-changed:
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
		shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), options.ShutdownTimeout)
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

func ingest(ctx context.Context, input io.Reader, journal *journal) error {
	reader := newObjectReader(input)
	for ctx.Err() == nil {
		record, err := reader.next()
		if ctx.Err() != nil || errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := journal.append(record); err != nil {
			return &terminalError{err}
		}
	}
	return nil
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
		if backoff > options.MaxRetryInterval-backoff {
			backoff = options.MaxRetryInterval
		} else {
			backoff *= 2
		}
	}
}
