package journal

import (
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	maxJournalKeyBytes   = 4 << 10
	journalLockName      = "journal.lock"
	journalDirectoryMode = 0o700
)

// Open exclusively locks dir, validates its checkpoint and segment layout,
// and recovers an incomplete final frame. It creates dir when needed and binds
// the journal to key until all records have been durably acknowledged.
// Records without a durable acknowledgement remain available to Next.
func Open(dir, key string) (_ *Journal, err error) {
	if dir == "" {
		return nil, ErrDirRequired
	}
	if len(key) > maxJournalKeyBytes {
		return nil, ErrKeyTooLong
	}
	if err := createJournalDirectory(dir); err != nil {
		return nil, err
	}
	lock, err := lockJournal(filepath.Join(dir, journalLockName))
	if err != nil {
		return nil, err
	}
	j := &Journal{dir: dir, key: key, lock: lock, notify: make(chan struct{}, 1)}
	defer func() {
		if err != nil {
			err = errors.Join(err, j.Close())
		}
	}()

	if err := j.loadSegments(); err != nil {
		return nil, err
	}
	state, err := j.loadState()
	if err != nil {
		return nil, err
	}
	if state.Key != key && !j.canRebind(state) {
		return nil, ErrDestinationMismatch
	}
	if err := os.Remove(filepath.Join(dir, journalCheckpointTempName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, journalError("remove incomplete checkpoint", err)
	}
	if err := j.restoreSegments(state); err != nil {
		return nil, err
	}
	if err := j.validateCursor(); err != nil {
		return nil, err
	}
	if state.Key != key {
		state.Key = key
		if err := j.saveState(state); err != nil {
			return nil, err
		}
	}
	// Validate the checkpoint before removing any files it claims were sent.
	if err := j.removeBefore(state.Segment); err != nil {
		return nil, err
	}
	return j, nil
}

// Ack normalizes a drained journal to offset zero in its final empty segment.
// Require that durable state before recovery can alter any files for a new key;
// an in-memory read cursor or an empty newer segment cannot prove delivery.
func (j *Journal) canRebind(state journalState) bool {
	if state.Offset != 0 {
		return false
	}
	if len(j.segments) == 0 {
		return state.Segment == journalFirstSegmentID
	}
	last := j.segments[len(j.segments)-1]
	return state.Segment == last.id && last.size == 0
}

func (j *Journal) loadSegments() error {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return journalError("list directory", err)
	}
	// ReadDir sorts the fixed-width names into segment order.
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return ErrNonRegularEntry
		}
		name := entry.Name()
		switch name {
		case journalLockName, journalCheckpointName, journalCheckpointTempName:
			continue
		}
		if len(name) != journalSegmentNameBytes || !strings.HasSuffix(name, journalSegmentSuffix) {
			return ErrUnrecognizedEntry
		}
		id, err := strconv.ParseUint(name[:journalSegmentIDDigits], 10, 64)
		if err != nil || id == 0 || name != segmentName(id) {
			return ErrInvalidSegmentName
		}
		info, err := entry.Info()
		if err != nil {
			return journalError("inspect segment", err)
		}
		j.segments = append(j.segments, journalSegment{id: id, size: info.Size()})
	}
	return nil
}

func (j *Journal) restoreSegments(state journalState) error {
	j.readID, j.readAt = state.Segment, state.Offset
	if len(j.segments) == 0 {
		if state.Offset != 0 || state.Segment != journalFirstSegmentID {
			return ErrCheckpointReferenceMissing
		}
		return j.createSegment(state.Segment)
	}

	checkpointIndex := 0
	for checkpointIndex < len(j.segments) && j.segments[checkpointIndex].id < state.Segment {
		checkpointIndex++
	}
	if checkpointIndex == len(j.segments) || j.segments[checkpointIndex].id != state.Segment {
		return ErrCheckpointSegmentMissing
	}
	// Gaps among already acknowledged segments are harmless; pending ones must be contiguous.
	for i := checkpointIndex + 1; i < len(j.segments); i++ {
		if j.segments[i-1].id == math.MaxUint64 || j.segments[i].id != j.segments[i-1].id+1 {
			return ErrSegmentGap
		}
	}
	last := &j.segments[len(j.segments)-1]
	var err error
	j.writer, err = os.OpenFile(filepath.Join(j.dir, segmentName(last.id)), os.O_RDWR, journalFileMode)
	if err != nil {
		return journalError("open active segment", err)
	}
	last.size, err = recoverSegment(j.writer, last.size)
	return err
}

// Persist each newly created directory entry in its parent before accepting
// records. The new directory's name belongs to its parent; sync that parent too.
func createJournalDirectory(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return journalError("resolve directory", err)
	}
	var missing []string
	for current := dir; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return ErrPathNotDirectory
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return journalError("inspect directory", err)
		}
		missing = append(missing, current)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], journalDirectoryMode); err != nil && !errors.Is(err, os.ErrExist) {
			return journalError("create directory", err)
		}
		if err := syncJournalDirectory(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

// recoverSegment discards only an incomplete final frame, which could not have
// completed a successful Append. A checksum mismatch fails closed as corruption.
func recoverSegment(file *os.File, size int64) (int64, error) {
	var offset int64
	for offset < size {
		length, checksum, headerBytes, err := readJournalHeader(file, offset)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, journalError("read active segment", err)
		}
		if length > size-offset-headerBytes {
			break
		}
		hash := crc32.NewIEEE()
		if _, err := io.Copy(hash, io.NewSectionReader(file, offset+headerBytes, length)); err != nil {
			return 0, journalError("verify active segment", err)
		}
		if hash.Sum32() != checksum {
			return 0, ErrChecksum
		}
		offset += headerBytes + length
	}
	if offset != size {
		if err := file.Truncate(offset); err != nil {
			return 0, journalError("recover incomplete frame", err)
		}
		if err := file.Sync(); err != nil {
			return 0, journalError("sync recovered segment", err)
		}
	}
	return offset, nil
}
