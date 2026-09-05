package collector

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// Option validation errors.
var (
	ErrJournalDirRequired      = errors.New("collector: journal directory is required")
	ErrNegativeOption          = errors.New("collector: limits and durations must not be negative")
	ErrRetryIntervalExceedsMax = errors.New("collector: retry interval must not exceed maximum retry interval")
	ErrInputRequired           = errors.New("collector: input is required")
	ErrDestinationRequired     = errors.New("collector: destination is required")
)

// Input errors are wrapped with the input line number.
var (
	ErrInvalidJSONRecord = errors.New("record must contain one complete JSON object")
	ErrInputRead         = errors.New("input read failure")
)

// Journal validation and integrity errors.
var (
	ErrJournalKeyTooLong                 = errors.New("collector: journal key must not exceed 4096 bytes")
	ErrJournalNonRegularEntry            = errors.New("collector: journal contains a non-regular entry")
	ErrJournalUnrecognizedEntry          = errors.New("collector: journal directory contains an unrecognized entry; use a dedicated directory")
	ErrJournalInvalidSegmentName         = errors.New("collector: journal contains an invalid segment name")
	ErrJournalInvalidCheckpoint          = errors.New("collector: invalid journal checkpoint")
	ErrJournalDestinationMismatch        = errors.New("collector: journal belongs to a different destination; use a separate directory")
	ErrJournalCheckpointMissing          = errors.New("collector: journal checkpoint is missing")
	ErrJournalCheckpointReferenceMissing = errors.New("collector: journal checkpoint refers to a missing segment")
	ErrJournalCheckpointSegmentMissing   = errors.New("collector: journal checkpoint segment is missing")
	ErrJournalSegmentGap                 = errors.New("collector: journal contains a gap between segments")
	ErrJournalPathNotDirectory           = errors.New("collector: journal path contains a non-directory")
	ErrJournalCorruptFrame               = errors.New("collector: corrupt journal frame")
	ErrJournalChecksum                   = errors.New("collector: journal checksum mismatch")
	ErrJournalCheckpointBeyondEnd        = errors.New("collector: journal checkpoint is past the segment end")
	ErrJournalInvalidCheckpointFrame     = errors.New("collector: invalid journal checkpoint frame")
	ErrJournalCheckpointBoundary         = errors.New("collector: journal checkpoint is not on a record boundary")
	ErrJournalClosed                     = errors.New("collector: journal is closed")
	ErrJournalInvalidRecordSize          = errors.New("collector: invalid journal record size")
	ErrJournalSequenceExhausted          = errors.New("collector: journal segment sequence exhausted")
	ErrJournalInvalidJSONRecord          = errors.New("collector: journal frame is not a JSON object")
)

// Journal platform support errors.
var (
	ErrJournalLockUnsupported = errors.New("collector: durable journal locking requires Linux, macOS, or BSD")
	ErrJournalSyncUnsupported = errors.New("collector: durable journal directory sync is unsupported on this platform")
)

// ErrTemporaryDelivery is the default cause of a RetryError without an Err.
var ErrTemporaryDelivery = errors.New("collector: temporary delivery failure")

var errBatchFull = errors.New("collector: batch byte limit reached")

// RetryError reports a transient delivery failure. After optionally specifies a
// minimum delay before the next attempt. The collector also applies its backoff.
type RetryError struct {
	Err   error
	After time.Duration
}

func (e *RetryError) Error() string {
	return e.Unwrap().Error()
}

func (e *RetryError) Unwrap() error {
	if e.Err == nil {
		return ErrTemporaryDelivery
	}
	return e.Err
}

// terminalError prevents cancellation or an input failure from causing a
// shutdown retry after delivery or journal operations have already failed.
type terminalError struct{ error }

func (e *terminalError) Unwrap() error { return e.error }

// A joined error can contain both cancellation and a permanent failure. Only
// errors whose complete cause is cancellation permit resuming an interrupted send.
func cancellationOnly(err, cancellation error) bool {
	if err == cancellation {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return cancellationOnly(wrapped.Unwrap(), cancellation)
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !cancellationOnly(cause, cancellation) {
				return false
			}
		}
		return true
	}
	return false
}

func journalError(operation string, err error) error {
	if pathError, ok := errors.AsType[*os.PathError](err); ok {
		err = pathError.Err
	} else if linkError, ok := errors.AsType[*os.LinkError](err); ok {
		err = linkError.Err
	}
	return fmt.Errorf("collector: journal %s: %w", operation, err)
}
