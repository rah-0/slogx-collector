package collector

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type journalState struct {
	Version int    `json:"version"`
	Key     string `json:"key"`
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
}

func (j *journal) loadState() (journalState, error) {
	file, err := os.Open(filepath.Join(j.dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		if len(j.segments) != 0 {
			return journalState{}, ErrJournalCheckpointMissing
		}
		state := journalState{Version: 1, Key: j.key, Segment: 1}
		return state, j.saveState(state)
	}
	if err != nil {
		return journalState{}, journalError("open checkpoint", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 64*1024+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return journalState{}, journalError("read checkpoint", err)
	}
	var state journalState
	if len(data) > 64*1024 {
		return state, ErrJournalInvalidCheckpoint
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, ErrJournalInvalidCheckpoint
	}
	if state.Version != 1 || state.Segment == 0 || state.Offset < 0 {
		return state, ErrJournalInvalidCheckpoint
	}
	if state.Key != j.key {
		return state, ErrJournalDestinationMismatch
	}
	return state, nil
}

func (j *journal) validateCursor() error {
	var segment journalSegment
	for _, candidate := range j.segments {
		if candidate.id == j.readID {
			segment = candidate
			break
		}
	}
	if segment.id == 0 {
		return ErrJournalCheckpointSegmentMissing
	}
	if j.readAt > segment.size {
		return ErrJournalCheckpointBeyondEnd
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
		if length > segment.size-offset-headerBytes {
			return ErrJournalInvalidCheckpointFrame
		}
		offset += headerBytes + length
	}
	if offset != j.readAt {
		return ErrJournalCheckpointBoundary
	}
	return nil
}

func (j *journal) saveState(state journalState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return journalError("encode checkpoint", err)
	}
	file, err := os.OpenFile(filepath.Join(j.dir, "state.tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
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
	if err := os.Rename(filepath.Join(j.dir, "state.tmp"), filepath.Join(j.dir, "state.json")); err != nil {
		return journalError("replace checkpoint", err)
	}
	return syncJournalDirectory(j.dir)
}
