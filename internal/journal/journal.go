// Package journal stores JSON records durably until their delivery is acknowledged.
package journal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	journalSegmentBytes        = 16 << 20
	journalLengthBytes         = 4
	journalHeaderBytes         = journalLengthBytes + crc32.Size
	journalExtendedLengthBytes = 8
	journalExtendedHeaderBytes = journalHeaderBytes + journalExtendedLengthBytes
	journalFirstSegmentID      = 1
	journalSegmentIDDigits     = 20
	journalSegmentSuffix       = ".segment"
	journalSegmentNameBytes    = journalSegmentIDDigits + len(journalSegmentSuffix)
	journalFileMode            = 0o600
)

type journalSegment struct {
	id   uint64
	size int64
}

// Journal keeps payloads on disk. Memory grows by one small entry per segment,
// not by one entry per record. Only one producer and one consumer are supported.
// A Journal must be created with Open.
type Journal struct {
	mu       sync.Mutex
	dir      string
	key      string
	lock     *os.File
	writer   *os.File
	reader   *os.File
	readerID uint64
	segments []journalSegment
	readID   uint64
	readAt   int64
	notify   chan struct{}
	closed   bool
}

// Append writes and syncs a record before making it available to Next and Notify.
// The caller must supply a complete JSON object.
func (j *Journal) Append(record json.RawMessage) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	if len(record) == 0 {
		return ErrInvalidRecordSize
	}
	var header [journalExtendedHeaderBytes]byte
	headerBytes := journalHeaderBytes
	if uint64(len(record)) > math.MaxUint32 {
		binary.LittleEndian.PutUint64(header[journalHeaderBytes:], uint64(len(record)))
		headerBytes = journalExtendedHeaderBytes
	} else {
		binary.LittleEndian.PutUint32(header[:journalLengthBytes], uint32(len(record)))
	}
	binary.LittleEndian.PutUint32(header[journalLengthBytes:journalHeaderBytes], crc32.ChecksumIEEE(record))
	last := &j.segments[len(j.segments)-1]
	if last.size > 0 && int64(len(record)) > journalSegmentBytes-last.size-int64(headerBytes) {
		if last.id == math.MaxUint64 {
			return ErrSequenceExhausted
		}
		if err := j.createSegment(last.id + 1); err != nil {
			return err
		}
		last = &j.segments[len(j.segments)-1]
	}
	if _, err := j.writer.WriteAt(header[:headerBytes], last.size); err != nil {
		return journalError("write frame header", err)
	}
	if _, err := j.writer.WriteAt(record, last.size+int64(headerBytes)); err != nil {
		return journalError("write frame", err)
	}
	if err := j.writer.Sync(); err != nil {
		return journalError("sync frame", err)
	}
	last.size += int64(headerBytes) + int64(len(record))
	// One pending wake-up is enough: the consumer reads until the journal is empty.
	select {
	case j.notify <- struct{}{}:
	default:
	}
	return nil
}

// Next reads the next record without acknowledging it. It returns io.EOF when
// no unread records remain; a later Append can make more records available.
// A positive remainingBytes limits the record size. ErrBatchFull leaves the
// record unread. A non-positive budget accepts a record of any size.
func (j *Journal) Next(remainingBytes int) (json.RawMessage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, ErrClosed
	}
	for {
		index := j.readID - j.segments[0].id
		segment := j.segments[index]
		if j.readAt == segment.size {
			if index == uint64(len(j.segments)-1) {
				return nil, io.EOF
			}
			j.readID++
			j.readAt = 0
			continue
		}
		if j.reader == nil || j.readerID != j.readID {
			if j.reader != nil {
				if err := j.reader.Close(); err != nil {
					return nil, journalError("close read segment", err)
				}
				j.reader = nil
			}
			file, err := os.Open(filepath.Join(j.dir, segmentName(j.readID)))
			if err != nil {
				return nil, journalError("open read segment", err)
			}
			j.reader, j.readerID = file, j.readID
		}
		length, checksum, headerBytes, err := readJournalHeader(j.reader, j.readAt)
		if err != nil {
			return nil, journalError("read frame header", err)
		}
		if length > segment.size-j.readAt-headerBytes {
			return nil, ErrCorruptFrame
		}
		if remainingBytes > 0 && length > int64(remainingBytes) {
			return nil, ErrBatchFull
		}
		record := make(json.RawMessage, length)
		if _, err := j.reader.ReadAt(record, j.readAt+headerBytes); err != nil {
			return nil, journalError("read frame", err)
		}
		if crc32.ChecksumIEEE(record) != checksum {
			return nil, ErrChecksum
		}
		if record[0] != '{' || !json.Valid(record) {
			return nil, ErrInvalidJSONRecord
		}
		j.readAt += headerBytes + length
		return record, nil
	}
}

// Ack syncs a checkpoint for all records read by Next, then removes fully
// acknowledged segments. Until the checkpoint is persisted, those records replay
// when the journal is reopened.
func (j *Journal) Ack() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	// Normalize a fully consumed segment so its disk space can be reclaimed.
	for {
		index := j.readID - j.segments[0].id
		if j.readAt != j.segments[index].size {
			break
		}
		if index == uint64(len(j.segments)-1) {
			if j.readAt == 0 {
				break
			}
			if j.readID == math.MaxUint64 {
				return ErrSequenceExhausted
			}
			if err := j.createSegment(j.readID + 1); err != nil {
				return err
			}
		}
		j.readID++
		j.readAt = 0
	}
	if err := j.saveState(journalState{Version: journalCheckpointVersion, Key: j.key, Segment: j.readID, Offset: j.readAt}); err != nil {
		return err
	}
	if j.reader != nil && j.readerID < j.readID {
		if err := j.reader.Close(); err != nil {
			return journalError("close acknowledged segment", err)
		}
		j.reader = nil
	}
	return j.removeBefore(j.readID)
}

// Close closes the journal files and releases its lock without acknowledging
// unread or unacknowledged records. Calling Close more than once is safe.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	var err error
	for _, file := range []*os.File{j.reader, j.writer, j.lock} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	if err != nil {
		return journalError("close", err)
	}
	return nil
}

// Notify returns a channel signaled after a successful Append. Signals coalesce;
// the consumer should read until Next returns io.EOF before waiting again.
// Opening a journal with a backlog does not signal the channel. Close does not
// close the channel.
func (j *Journal) Notify() <-chan struct{} {
	return j.notify
}

func segmentName(id uint64) string {
	return fmt.Sprintf("%0*d%s", journalSegmentIDDigits, id, journalSegmentSuffix)
}

// readJournalHeader accepts the original 8-byte header and its extended form:
// a zero 32-bit length indicates an additional 64-bit length after the checksum.
func readJournalHeader(file *os.File, offset int64) (length int64, checksum uint32, headerBytes int64, err error) {
	var header [journalExtendedHeaderBytes]byte
	if _, err := file.ReadAt(header[:journalHeaderBytes], offset); err != nil {
		return 0, 0, 0, err
	}
	length = int64(binary.LittleEndian.Uint32(header[:journalLengthBytes]))
	checksum = binary.LittleEndian.Uint32(header[journalLengthBytes:journalHeaderBytes])
	headerBytes = journalHeaderBytes
	if length == 0 {
		if _, err := file.ReadAt(header[journalHeaderBytes:], offset+journalHeaderBytes); err != nil {
			return 0, 0, 0, err
		}
		extended := binary.LittleEndian.Uint64(header[journalHeaderBytes:])
		if extended == 0 || extended > math.MaxInt64 {
			return 0, 0, 0, ErrCorruptFrame
		}
		length, headerBytes = int64(extended), journalExtendedHeaderBytes
	}
	return length, checksum, headerBytes, nil
}

func (j *Journal) createSegment(id uint64) error {
	file, err := os.OpenFile(filepath.Join(j.dir, segmentName(id)), os.O_CREATE|os.O_EXCL|os.O_RDWR, journalFileMode)
	if err != nil {
		return journalError("create segment", err)
	}
	if err := errors.Join(file.Sync(), syncJournalDirectory(j.dir)); err != nil {
		file.Close()
		return journalError("persist segment", err)
	}
	if j.writer != nil {
		if err := j.writer.Close(); err != nil {
			file.Close()
			return journalError("close previous segment", err)
		}
	}
	j.writer = file
	j.segments = append(j.segments, journalSegment{id: id})
	return nil
}

func (j *Journal) removeBefore(id uint64) error {
	var count int
	for _, segment := range j.segments {
		if segment.id >= id {
			break
		}
		if err := os.Remove(filepath.Join(j.dir, segmentName(segment.id))); err != nil {
			return journalError("remove acknowledged segment", err)
		}
		count++
	}
	if count != 0 {
		j.segments = append([]journalSegment(nil), j.segments[count:]...)
		return syncJournalDirectory(j.dir)
	}
	return nil
}
