package collector

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

func openJournal(dir, key string) (_ *journal, err error) {
	if dir == "" {
		return nil, ErrJournalDirRequired
	}
	if len(key) > 4096 {
		return nil, ErrJournalKeyTooLong
	}
	if err := createJournalDirectory(dir); err != nil {
		return nil, err
	}
	lock, err := lockJournal(filepath.Join(dir, "journal.lock"))
	if err != nil {
		return nil, err
	}
	j := &journal{dir: dir, key: key, lock: lock, notify: make(chan struct{})}
	defer func() {
		if err != nil {
			err = errors.Join(err, j.close())
		}
	}()

	if err := j.loadSegments(); err != nil {
		return nil, err
	}
	state, err := j.loadState()
	if err != nil {
		return nil, err
	}
	if err := os.Remove(filepath.Join(dir, "state.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, journalError("remove incomplete checkpoint", err)
	}
	if err := j.restoreSegments(state); err != nil {
		return nil, err
	}
	if err := j.validateCursor(); err != nil {
		return nil, err
	}
	// Validate the checkpoint before removing any files it claims were sent.
	if err := j.removeBefore(state.Segment); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *journal) loadSegments() error {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return journalError("list directory", err)
	}
	// ReadDir sorts the fixed-width names into segment order.
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return ErrJournalNonRegularEntry
		}
		name := entry.Name()
		switch name {
		case "journal.lock", "state.json", "state.tmp":
			continue
		}
		if len(name) != 28 || !strings.HasSuffix(name, ".segment") {
			return ErrJournalUnrecognizedEntry
		}
		id, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || id == 0 || name != segmentName(id) {
			return ErrJournalInvalidSegmentName
		}
		info, err := entry.Info()
		if err != nil {
			return journalError("inspect segment", err)
		}
		j.segments = append(j.segments, journalSegment{id: id, size: info.Size()})
	}
	return nil
}

func (j *journal) restoreSegments(state journalState) error {
	j.readID, j.readAt = state.Segment, state.Offset
	if len(j.segments) == 0 {
		if state.Offset != 0 || state.Segment != 1 {
			return ErrJournalCheckpointReferenceMissing
		}
		return j.createSegment(state.Segment)
	}

	checkpointIndex := 0
	for checkpointIndex < len(j.segments) && j.segments[checkpointIndex].id < state.Segment {
		checkpointIndex++
	}
	if checkpointIndex == len(j.segments) || j.segments[checkpointIndex].id != state.Segment {
		return ErrJournalCheckpointSegmentMissing
	}
	// Gaps among already acknowledged segments are harmless; pending ones must be contiguous.
	for i := checkpointIndex + 1; i < len(j.segments); i++ {
		if j.segments[i-1].id == math.MaxUint64 || j.segments[i].id != j.segments[i-1].id+1 {
			return ErrJournalSegmentGap
		}
	}
	last := &j.segments[len(j.segments)-1]
	var err error
	j.writer, err = os.OpenFile(filepath.Join(j.dir, segmentName(last.id)), os.O_RDWR, 0o600)
	if err != nil {
		return journalError("open active segment", err)
	}
	last.size, err = recoverSegment(j.writer, last.size)
	return err
}

// Persist each newly created directory entry in its parent before accepting
// records. Syncing only the final directory does not persist its own name.
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
				return ErrJournalPathNotDirectory
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return journalError("inspect directory", err)
		}
		missing = append(missing, current)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return journalError("create directory", err)
		}
		if err := syncJournalDirectory(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

// recoverSegment discards only an incomplete final frame, which could not have
// been acknowledged by append. A checksum mismatch fails closed as corruption.
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
			return 0, ErrJournalChecksum
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
