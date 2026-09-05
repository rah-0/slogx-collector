//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func testJournal(t *testing.T, dir string) *journal {
	t.Helper()
	j, err := openJournal(dir, "test-destination")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := j.close(); err != nil {
			t.Error(err)
		}
	})
	return j
}

func appendJournal(t *testing.T, j *journal, record string) {
	t.Helper()
	if err := j.append(json.RawMessage(record)); err != nil {
		t.Fatal(err)
	}
}

func nextJournal(t *testing.T, j *journal, want string) {
	t.Helper()
	record, err := j.next(0)
	if err != nil || string(record) != want {
		t.Fatalf("next = %s, %v; want %s", record, err, want)
	}
}

func TestJournalReplaysOnlyUnacknowledgedRecords(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	for _, record := range []string{`{"id":1}`, `{"id":2}`, `{"id":3}`} {
		appendJournal(t, j, record)
	}
	nextJournal(t, j, `{"id":1}`)
	if err := j.ack(); err != nil {
		t.Fatal(err)
	}
	nextJournal(t, j, `{"id":2}`) // Reading is not acknowledgement.
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":2}`)
	nextJournal(t, j, `{"id":3}`)
	if err := j.ack(); err != nil {
		t.Fatal(err)
	}
	if len(j.segments) != 1 || j.segments[0].size != 0 {
		t.Fatalf("acknowledged segments were not reclaimed: %+v", j.segments)
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	if _, err := j.next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("acknowledged record replayed: %v", err)
	}
	appendJournal(t, j, `{"id":4}`)
	nextJournal(t, j, `{"id":4}`)
}

func TestJournalNextDoesNotAdvanceOnBatchLimit(t *testing.T) {
	j := testJournal(t, t.TempDir())
	appendJournal(t, j, `{"id":1}`)
	if _, err := j.next(1); !errors.Is(err, errBatchFull) {
		t.Fatalf("batch limit = %v", err)
	}
	// An empty batch accepts the same record regardless of the batch budget.
	nextJournal(t, j, `{"id":1}`)
}

func TestJournalReplaysRecordLargerThanSegment(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	record := `{"data":"` + strings.Repeat("x", journalSegmentBytes) + `"}`
	appendJournal(t, j, record)
	appendJournal(t, j, `{"id":2}`)
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, record)
	if err := j.ack(); err != nil {
		t.Fatal(err)
	}
	nextJournal(t, j, `{"id":2}`)
}

func extendedJournalFrame(record string) []byte {
	frame := make([]byte, 16+len(record))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE([]byte(record)))
	binary.LittleEndian.PutUint64(frame[8:16], uint64(len(record)))
	copy(frame[16:], record)
	return frame
}

func TestJournalReplaysExtendedFramesAcrossCheckpoints(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	path := filepath.Join(dir, segmentName(j.segments[0].id))
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	// Small payloads exercise the extended format without allocating 4 GiB.
	frames := append(extendedJournalFrame(`{"id":1}`), extendedJournalFrame(`{"id":2}`)...)
	if err := os.WriteFile(path, frames, 0o600); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":1}`)
	if err := j.ack(); err != nil {
		t.Fatal(err)
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	appendJournal(t, j, `{"id":3}`)
	nextJournal(t, j, `{"id":2}`)
	nextJournal(t, j, `{"id":3}`)
}

func TestJournalReadsExtendedLength(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "header")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	var header [16]byte
	const wantLength = int64(math.MaxUint32) + 1
	binary.LittleEndian.PutUint64(header[8:], uint64(wantLength))
	if _, err := file.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	length, _, headerBytes, err := readJournalHeader(file, 0)
	if err != nil || length != wantLength || headerBytes != 16 {
		t.Fatalf("extended header = %d, %d, %v", length, headerBytes, err)
	}
}

func TestJournalRotatesAndReclaimsSegments(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	// Two records cannot share a segment; this exercises real rotation without
	// reducing the production segment size or coupling to allocation details.
	record := `{"data":"` + strings.Repeat("x", journalSegmentBytes/2) + `"}`
	appendJournal(t, j, record)
	appendJournal(t, j, record)
	if len(j.segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(j.segments))
	}
	firstID := j.segments[0].id
	nextJournal(t, j, record)
	if err := j.ack(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentName(firstID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged segment still exists: %v", err)
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, record)
	if err := j.ack(); err != nil {
		t.Fatal(err)
	}
}

func TestJournalRecoversIncompleteTail(t *testing.T) {
	extended := extendedJournalFrame(`{"id":2}`)
	for name, tail := range map[string][]byte{
		"short header":          {3, 0, 0},
		"short payload":         {5, 0, 0, 0, 0, 0, 0, 0, '{'},
		"short extended header": extended[:12],
		"short extended frame":  extended[:len(extended)-1],
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			appendJournal(t, j, `{"id":1}`)
			path := filepath.Join(dir, segmentName(j.segments[0].id))
			if err := j.close(); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write(tail); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			j = testJournal(t, dir)
			appendJournal(t, j, `{"id":2}`)
			nextJournal(t, j, `{"id":1}`)
			nextJournal(t, j, `{"id":2}`)
		})
	}
}

func TestJournalRejectsCorruptExtendedFrames(t *testing.T) {
	invalidLength := extendedJournalFrame(`{"id":1}`)
	binary.LittleEndian.PutUint64(invalidLength[8:16], math.MaxUint64)
	invalidChecksum := extendedJournalFrame(`{"id":1}`)
	invalidChecksum[4] ^= 1
	for name, data := range map[string][]byte{
		"zero length":      make([]byte, 16),
		"invalid length":   invalidLength,
		"invalid checksum": invalidChecksum,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			path := filepath.Join(dir, segmentName(j.segments[0].id))
			if err := j.close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := openJournal(dir, "test-destination"); err == nil {
				t.Fatal("corrupt extended frame was accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, data) {
				t.Fatalf("corrupt data was changed: %v", err)
			}
		})
	}
}

func TestJournalRejectsCorruptionWithoutDeletingData(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	path := filepath.Join(dir, segmentName(j.segments[0].id))
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-2] = '2' // Still JSON, but the checksum no longer matches.
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openJournal(dir, "test-destination"); err == nil {
		t.Fatal("corrupt journal was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, data) {
		t.Fatalf("corrupt data was changed: %v", err)
	}
}

func TestJournalLocksAndBindsDestination(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	if _, err := openJournal(dir, "test-destination"); err == nil {
		t.Fatal("concurrent journal use succeeded")
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openJournal(dir, "different-destination"); err == nil {
		t.Fatal("journal accepted another destination")
	}
	_ = testJournal(t, dir)
}

func TestJournalRejectsInvalidCheckpoints(t *testing.T) {
	for _, contents := range []string{"{}", `{"version":1,"key":"test-destination","segment":1,"offset":1}`, `{"version":1,"key":"test-destination","segment":2,"offset":0}`, `{"version":2,"segment":1}`} {
		t.Run(contents, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			appendJournal(t, j, `{"id":1}`)
			if err := j.close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := openJournal(dir, "test-destination"); err == nil {
				t.Fatal("invalid checkpoint was accepted")
			}
			if _, err := os.Stat(filepath.Join(dir, segmentName(1))); err != nil {
				t.Fatalf("invalid checkpoint caused record deletion: %v", err)
			}
		})
	}
}

func TestJournalCreatesNestedDirectories(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "parent", "queue")
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":1}`)
}

func TestJournalPreservesUnrelatedDirectoryContents(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "user-file")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openJournal(dir, "test-destination"); err == nil {
		t.Fatal("non-journal directory was accepted")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatalf("unrelated file changed: %q, %v", data, err)
	}
}

func TestJournalReplayAfterProcessExit(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestJournalCrashHelper$")
	cmd.Env = append(os.Environ(), "SLOGX_TEST_CRASH_JOURNAL="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child failed: %v\n%s", err, output)
	}
	j := testJournal(t, dir)
	nextJournal(t, j, `{"id":1}`)
	// An interrupted write after a durable record is discarded on recovery.
	if _, err := j.next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("incomplete child write replayed: %v", err)
	}
}

func TestJournalCrashHelper(t *testing.T) {
	dir := os.Getenv("SLOGX_TEST_CRASH_JOURNAL")
	if dir == "" {
		return
	}
	j, err := openJournal(dir, "test-destination")
	if err != nil {
		t.Fatal(err)
	}
	appendJournal(t, j, `{"id":1}`)
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], 123)
	if _, err := j.writer.WriteAt(header[:], j.segments[0].size); err != nil {
		t.Fatal(err)
	}
	os.Exit(0) // Deliberately bypass Close and testing cleanup.
}
