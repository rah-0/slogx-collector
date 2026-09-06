package journal

import (
	"errors"
	"fmt"
	"os"
)

// Validation and integrity errors.
var (
	ErrDirRequired                = errors.New("journal: directory is required")
	ErrKeyTooLong                 = errors.New("journal: key must not exceed 4096 bytes")
	ErrNonRegularEntry            = errors.New("journal: contains a non-regular entry")
	ErrUnrecognizedEntry          = errors.New("journal: directory contains an unrecognized entry; use a dedicated directory")
	ErrInvalidSegmentName         = errors.New("journal: contains an invalid segment name")
	ErrInvalidCheckpoint          = errors.New("journal: invalid checkpoint")
	ErrDestinationMismatch        = errors.New("journal: belongs to a different destination; use a separate directory")
	ErrCheckpointMissing          = errors.New("journal: checkpoint is missing")
	ErrCheckpointReferenceMissing = errors.New("journal: checkpoint refers to a missing segment")
	ErrCheckpointSegmentMissing   = errors.New("journal: checkpoint segment is missing")
	ErrSegmentGap                 = errors.New("journal: contains a gap between segments")
	ErrPathNotDirectory           = errors.New("journal: path contains a non-directory")
	ErrCorruptFrame               = errors.New("journal: corrupt frame")
	ErrChecksum                   = errors.New("journal: checksum mismatch")
	ErrCheckpointBeyondEnd        = errors.New("journal: checkpoint is past the segment end")
	ErrInvalidCheckpointFrame     = errors.New("journal: invalid checkpoint frame")
	ErrCheckpointBoundary         = errors.New("journal: checkpoint is not on a record boundary")
	ErrClosed                     = errors.New("journal: is closed")
	ErrInvalidRecordSize          = errors.New("journal: invalid record size")
	ErrSequenceExhausted          = errors.New("journal: segment sequence exhausted")
	ErrInvalidJSONRecord          = errors.New("journal: frame is not a JSON object")
)

// Platform support errors.
var (
	ErrLockUnsupported = errors.New("journal: durable locking requires Linux, macOS, or BSD")
	ErrSyncUnsupported = errors.New("journal: durable directory sync is unsupported on this platform")
)

// ErrBatchFull means the next record exceeds the positive byte budget passed to Next.
var ErrBatchFull = errors.New("journal: batch byte limit reached")

func journalError(operation string, err error) error {
	if pathError, ok := errors.AsType[*os.PathError](err); ok {
		err = pathError.Err
	} else if linkError, ok := errors.AsType[*os.LinkError](err); ok {
		err = linkError.Err
	}
	return fmt.Errorf("journal: %s: %w", operation, err)
}
