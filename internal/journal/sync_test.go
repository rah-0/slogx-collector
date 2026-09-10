//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalSyncPublishesPendingRecords(t *testing.T) {
	j := testJournal(t, t.TempDir())
	syncJournal(t, j) // An idle sync must not announce records.
	for _, record := range []string{`{"id":1}`, `{"id":2}`, `{"id":3}`} {
		appendJournal(t, j, record)
		if _, err := j.Next(0); !errors.Is(err, io.EOF) {
			t.Fatalf("pending record became readable: %v", err)
		}
	}
	select {
	case <-j.Notify():
		t.Fatal("pending records signaled the consumer")
	default:
	}
	syncJournal(t, j)
	select {
	case <-j.Notify():
	default:
		t.Fatal("sync did not signal the consumer")
	}
	for _, record := range []string{`{"id":1}`, `{"id":2}`, `{"id":3}`} {
		nextJournal(t, j, record)
	}
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("after synced records = %v, want EOF", err)
	}
	syncJournal(t, j)
	select {
	case <-j.Notify():
		t.Fatal("idle sync signaled the consumer")
	default:
	}
}

func TestJournalAckPreservesUnsyncedTail(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	syncJournal(t, j)
	nextJournal(t, j, `{"id":1}`)
	appendJournal(t, j, `{"id":2}`)
	appendJournal(t, j, `{"id":3}`)
	segmentID := j.readID
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	checkpointJournal(t, j)
	if len(j.segments) != 1 || j.segments[0].id != segmentID {
		t.Fatalf("ack rotated the segment containing pending records: %+v", j.segments)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentName(segmentID))); err != nil {
		t.Fatalf("ack removed the pending segment: %v", err)
	}
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("ack exposed pending records: %v", err)
	}
	syncJournal(t, j)
	nextJournal(t, j, `{"id":2}`)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":3}`)
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("after unacknowledged tail = %v, want EOF", err)
	}
}

func TestJournalCloseSyncsPendingRecords(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	appendJournal(t, j, `{"id":2}`)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":1}`)
	nextJournal(t, j, `{"id":2}`)
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("after final sync = %v, want EOF", err)
	}
}

func TestJournalSyncFailureDoesNotPublishPendingRecords(t *testing.T) {
	j := testJournal(t, t.TempDir())
	appendJournal(t, j, `{"id":1}`)
	path := j.writer.Name()
	if err := j.writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("sync closed writer = %v, want os.ErrClosed", err)
	}
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("failed sync exposed pending records: %v", err)
	}
	select {
	case <-j.Notify():
		t.Fatal("failed sync signaled the consumer")
	default:
	}
	var err error
	j.writer, err = os.OpenFile(path, os.O_RDWR, journalFileMode)
	if err != nil {
		t.Fatal(err)
	}
	syncJournal(t, j)
	nextJournal(t, j, `{"id":1}`)
}

func TestJournalCloseReleasesLockAfterSyncFailure(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	if err := j.writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close with failed final sync = %v, want os.ErrClosed", err)
	}
	if err := j.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("sync after close = %v, want ErrClosed", err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":1}`)
}

func TestJournalAppendFailurePreservesPendingRecords(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	syncJournal(t, j)
	nextJournal(t, j, `{"id":1}`)
	appendJournal(t, j, `{"id":2}`)
	if err := j.writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.Append(json.RawMessage(`{"id":3}`)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("append with failed writer = %v, want os.ErrClosed", err)
	}
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("failed append exposed pending records: %v", err)
	}
	if err := j.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("sync after failed rollback = %v, want os.ErrClosed", err)
	}
	if err := j.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close after failed rollback = %v, want os.ErrClosed", err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":2}`)
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("after recovered pending record = %v, want EOF", err)
	}
}

func TestJournalRollsBackPartialTailBeforeShorterAppend(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	path := j.writer.Name()
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Leave a complete header and only part of a payload, as a short write can.
	// Its tail is longer than the next successful record, so overwriting alone
	// would leave bytes behind the new complete frame.
	frame := extendedJournalFrame(`{"incomplete":"this longer payload must never be replayed"}`)
	if _, err := j.writer.WriteAt(frame[:len(frame)-5], before.Size()); err != nil {
		t.Fatal(err)
	}
	if err := j.rollbackAppend(io.ErrShortWrite); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("rollback = %v, want original short-write error", err)
	}
	after, err := os.Stat(path)
	if err != nil || after.Size() != before.Size() {
		t.Fatalf("rollback did not restore the complete pending prefix: stat = %v, %v", after, err)
	}
	appendJournal(t, j, `{"id":2}`)
	syncJournal(t, j)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":1}`)
	nextJournal(t, j, `{"id":2}`)
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("after rollback and shorter append = %v, want EOF", err)
	}
}
