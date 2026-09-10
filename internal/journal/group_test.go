//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestJournalAppendCommitsRecordGroups(t *testing.T) {
	j := testJournal(t, t.TempDir())
	const record = `{"id":1}`
	for range 2 {
		for range journalSyncRecords - 1 {
			appendJournal(t, j, record)
		}
		assertJournalEOF(t, j)
		appendJournal(t, j, record)
		for range journalSyncRecords {
			nextJournal(t, j, record)
		}
		assertJournalEOF(t, j)
	}
	// An explicit sync starts a new count, just like a full group does.
	appendJournal(t, j, record)
	syncJournal(t, j)
	nextJournal(t, j, record)
	for range journalSyncRecords - 1 {
		appendJournal(t, j, record)
	}
	assertJournalEOF(t, j)
	appendJournal(t, j, record)
	for range journalSyncRecords {
		nextJournal(t, j, record)
	}
}

func TestJournalAppendCommitsByteGroups(t *testing.T) {
	j := testJournal(t, t.TempDir())
	const overhead = len(`{"data":""}`) + journalHeaderBytes
	record := `{"data":"` + strings.Repeat("x", journalSyncBytes/2-overhead) + `"}`
	for range 2 {
		appendJournal(t, j, record)
		assertJournalEOF(t, j)
		appendJournal(t, j, record)
		nextJournal(t, j, record)
		nextJournal(t, j, record)
		assertJournalEOF(t, j)
	}
}

func TestJournalRotationStartsNewRecordGroup(t *testing.T) {
	j := testJournal(t, t.TempDir())
	const record = `{"id":1}`
	for range journalSyncRecords - 1 {
		appendJournal(t, j, record)
	}
	if err := j.createSegment(2); err != nil {
		t.Fatal(err)
	}
	for range journalSyncRecords - 1 {
		nextJournal(t, j, record)
	}
	appendJournal(t, j, record)
	assertJournalEOF(t, j)
	for range journalSyncRecords - 1 {
		appendJournal(t, j, record)
	}
	for range journalSyncRecords {
		nextJournal(t, j, record)
	}
}

func TestJournalFailedGroupSyncDoesNotPublishRecords(t *testing.T) {
	j := testJournal(t, t.TempDir())
	const record = `{"id":1}`
	for range journalSyncRecords - 1 {
		appendJournal(t, j, record)
	}
	// /dev/null accepts the final frame writes but rejects fsync on supported
	// Unix platforms, isolating sync failure from an append failure.
	writer := j.writer
	file, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	j.writer = file
	err = j.Append(json.RawMessage(record))
	j.writer = writer
	if closeErr := file.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err == nil || !strings.HasPrefix(err.Error(), "journal: sync records:") {
		t.Fatalf("append = %v, want group sync failure", err)
	}
	assertJournalEOF(t, j)
	select {
	case <-j.Notify():
		t.Fatal("failed group sync signaled readable records")
	default:
	}
	// Restore the failed frame to the real file before final cleanup sync.
	j.writeAt -= int64(journalHeaderBytes + len(record))
	j.pendingRecords--
	appendJournal(t, j, record)
	for range journalSyncRecords {
		nextJournal(t, j, record)
	}
}

func TestJournalGroupsAcknowledgementCheckpoints(t *testing.T) {
	j := testJournal(t, t.TempDir())
	const record = `{"id":1}`
	for range journalCheckpointBatches + 1 {
		appendJournal(t, j, record)
	}
	syncJournal(t, j)
	for index := range journalCheckpointBatches {
		nextJournal(t, j, record)
		if err := j.Ack(); err != nil {
			t.Fatal(err)
		}
		// Repeating an acknowledgement without another read is not a new batch.
		if err := j.Ack(); err != nil {
			t.Fatal(err)
		}
		state, err := j.loadState()
		if err != nil {
			t.Fatal(err)
		}
		want := int64(0)
		if index+1 == journalCheckpointBatches {
			want = int64(journalCheckpointBatches * (journalHeaderBytes + len(record)))
		}
		if state.Offset != want {
			t.Fatalf("checkpoint after %d batches = %d, want %d", index+1, state.Offset, want)
		}
	}
}

func TestJournalCheckpointExcludesLaterReads(t *testing.T) {
	for _, operation := range []string{"checkpoint", "sync", "close"} {
		t.Run(operation, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			for _, record := range []string{`{"id":1}`, `{"id":2}`, `{"id":3}`} {
				appendJournal(t, j, record)
			}
			syncJournal(t, j)
			nextJournal(t, j, `{"id":1}`)
			if err := j.Ack(); err != nil {
				t.Fatal(err)
			}
			nextJournal(t, j, `{"id":2}`) // This batch has not succeeded.
			switch operation {
			case "checkpoint":
				checkpointJournal(t, j)
			case "sync":
				syncJournal(t, j)
			}
			if operation != "close" {
				state, err := j.loadState()
				if err != nil {
					t.Fatal(err)
				}
				if state.Segment != 1 || state.Offset != int64(journalHeaderBytes+len(`{"id":1}`)) {
					t.Fatalf("%s persisted the wrong read boundary: %+v", operation, state)
				}
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			j = testJournal(t, dir)
			nextJournal(t, j, `{"id":2}`)
			nextJournal(t, j, `{"id":3}`)
			assertJournalEOF(t, j)
		})
	}
}

func TestJournalSyncFailureStillCheckpointsDeliveredPrefix(t *testing.T) {
	for _, operation := range []string{"sync", "close"} {
		t.Run(operation, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			appendJournal(t, j, `{"id":1}`)
			syncJournal(t, j)
			nextJournal(t, j, `{"id":1}`)
			if err := j.Ack(); err != nil {
				t.Fatal(err)
			}
			appendJournal(t, j, `{"id":2}`)
			if err := j.writer.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			if operation == "sync" {
				err = j.Sync()
			} else {
				err = j.Close()
			}
			if !errors.Is(err, os.ErrClosed) {
				t.Fatalf("%s = %v, want data sync failure", operation, err)
			}
			state, err := j.loadState()
			if err != nil {
				t.Fatal(err)
			}
			if state.Segment != 1 || state.Offset != int64(journalHeaderBytes+len(`{"id":1}`)) {
				t.Fatalf("delivered prefix was not checkpointed: %+v", state)
			}
			if operation == "sync" {
				if err := j.Close(); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("close = %v, want original writer failure", err)
				}
			}
			j = testJournal(t, dir)
			nextJournal(t, j, `{"id":2}`)
			assertJournalEOF(t, j)
		})
	}
}

func TestJournalCheckpointFailurePreservesSegmentsAndCanRetry(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	if err := j.createSegment(2); err != nil {
		t.Fatal(err)
	}
	appendJournal(t, j, `{"id":2}`)
	syncJournal(t, j)
	nextJournal(t, j, `{"id":1}`)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	nextJournal(t, j, `{"id":2}`)
	before, err := os.ReadFile(filepath.Join(dir, journalCheckpointName))
	if err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(dir, journalCheckpointTempName)
	if err := os.Mkdir(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := j.Checkpoint(); err == nil {
		t.Fatal("checkpoint succeeded with an unusable temporary path")
	}
	if _, err := os.Stat(filepath.Join(dir, segmentName(1))); err != nil {
		t.Fatalf("failed checkpoint removed delivered segment: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, journalCheckpointName))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed checkpoint changed durable state: %v", err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	checkpointJournal(t, j)
	if _, err := os.Stat(filepath.Join(dir, segmentName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successful checkpoint did not reclaim segment: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":2}`)
	assertJournalEOF(t, j)
}

func TestJournalReplaysUncheckpointedAcknowledgementsAfterExit(t *testing.T) {
	for _, checkpoint := range []string{"pending", "persisted"} {
		t.Run(checkpoint, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestJournalAcknowledgementCrashHelper$")
			cmd.Env = append(os.Environ(), "SLOGX_TEST_ACK_CRASH_DIR="+dir, "SLOGX_TEST_ACK_CRASH_STATE="+checkpoint)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child failed: %v\n%s", err, output)
			}
			j := testJournal(t, dir)
			if checkpoint == "pending" {
				nextJournal(t, j, `{"id":1}`)
			}
			nextJournal(t, j, `{"id":2}`)
			assertJournalEOF(t, j)
		})
	}
}

func TestJournalAcknowledgementCrashHelper(t *testing.T) {
	dir := os.Getenv("SLOGX_TEST_ACK_CRASH_DIR")
	if dir == "" {
		return
	}
	j, err := Open(dir, "test-destination")
	if err != nil {
		t.Fatal(err)
	}
	appendJournal(t, j, `{"id":1}`)
	appendJournal(t, j, `{"id":2}`)
	syncJournal(t, j)
	nextJournal(t, j, `{"id":1}`)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	nextJournal(t, j, `{"id":2}`)
	if os.Getenv("SLOGX_TEST_ACK_CRASH_STATE") == "persisted" {
		checkpointJournal(t, j)
	}
	os.Exit(0) // Deliberately bypass Close and testing cleanup.
}

func assertJournalEOF(t *testing.T, j *Journal) {
	t.Helper()
	if record, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("Next = %s, %v; want EOF", record, err)
	}
}
