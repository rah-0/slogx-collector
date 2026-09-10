package journal

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const (
	journalCheckpointVersion  = 1
	journalCheckpointName     = "state.json"
	journalCheckpointTempName = "state.tmp"
	maxJournalCheckpointBytes = 64 << 10
)

type journalState struct {
	Version int    `json:"version"`
	Key     string `json:"key"`
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
}

func (j *Journal) loadState() (journalState, error) {
	file, err := os.Open(filepath.Join(j.dir, journalCheckpointName))
	if errors.Is(err, os.ErrNotExist) {
		if len(j.segments) != 0 {
			return journalState{}, ErrCheckpointMissing
		}
		state := journalState{Version: journalCheckpointVersion, Key: j.key, Segment: journalFirstSegmentID}
		return state, j.saveState(state)
	}
	if err != nil {
		return journalState{}, journalError("open checkpoint", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxJournalCheckpointBytes+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return journalState{}, journalError("read checkpoint", err)
	}
	var state journalState
	if len(data) > maxJournalCheckpointBytes {
		return state, ErrInvalidCheckpoint
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, ErrInvalidCheckpoint
	}
	if state.Version != journalCheckpointVersion || state.Segment == 0 || state.Offset < 0 {
		return state, ErrInvalidCheckpoint
	}
	return state, nil
}

func (j *Journal) validateCursor() error {
	var segment journalSegment
	for _, candidate := range j.segments {
		if candidate.id == j.readID {
			segment = candidate
			break
		}
	}
	if segment.id == 0 {
		return ErrCheckpointSegmentMissing
	}
	if j.readAt > segment.durableSize {
		return ErrCheckpointBeyondEnd
	}
	file, err := os.Open(filepath.Join(j.dir, segmentName(segment.id)))
	if err != nil {
		return journalError("validate checkpoint", err)
	}
	defer file.Close()
	var offset int64
	for offset < j.readAt {
		length, _, headerBytes, err := readJournalHeader(file, offset)
		if err != nil {
			return journalError("validate checkpoint frame", err)
		}
		if length > segment.durableSize-offset-headerBytes {
			return ErrInvalidCheckpointFrame
		}
		offset += headerBytes + length
	}
	if offset != j.readAt {
		return ErrCheckpointBoundary
	}
	return nil
}

func (j *Journal) saveState(state journalState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return journalError("encode checkpoint", err)
	}
	file, err := os.OpenFile(filepath.Join(j.dir, journalCheckpointTempName), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, journalFileMode)
	if err != nil {
		return journalError("create checkpoint", err)
	}
	_, writeErr := file.Write(data)
	var syncErr error
	if writeErr == nil {
		syncErr = file.Sync()
	}
	if err := errors.Join(writeErr, syncErr, file.Close()); err != nil {
		return journalError("persist checkpoint", err)
	}
	if err := os.Rename(filepath.Join(j.dir, journalCheckpointTempName), filepath.Join(j.dir, journalCheckpointName)); err != nil {
		return journalError("replace checkpoint", err)
	}
	return syncJournalDirectory(j.dir)
}
