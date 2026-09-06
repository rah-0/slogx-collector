//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestJournalOpenRemovesStaleCheckpointAndReplaysBacklog(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	appendJournal(t, j, `{"id":2}`)
	nextJournal(t, j, `{"id":1}`)
	if err := j.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	// An interrupted checkpoint write must not supersede the durable state.
	if err := os.WriteFile(filepath.Join(dir, "state.tmp"), []byte(`{"version":`), 0o600); err != nil {
		t.Fatal(err)
	}

	j = testJournal(t, dir)
	nextJournal(t, j, `{"id":2}`)
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("after backlog = %v, want EOF", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale checkpoint remains: %v", err)
	}
	assertJournalStartupFiles(t, dir, map[string][]byte{"state.json": checkpoint})
}

func TestJournalOpenPreservesSegmentsWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	files := map[string][]byte{
		segmentName(1): extendedJournalFrame(`{"id":1}`),
	}
	writeJournalStartupFiles(t, dir, files)

	assertJournalOpenError(t, dir, ErrCheckpointMissing)
	assertJournalStartupFiles(t, dir, files)
	if _, err := os.Stat(filepath.Join(dir, "state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing checkpoint was replaced: %v", err)
	}
}

func TestJournalOpenReclaimsSegmentsWithGapBeforeCheckpoint(t *testing.T) {
	dir := t.TempDir()
	files := map[string][]byte{
		"state.json":   []byte(`{"version":1,"key":"test-destination","segment":3,"offset":0}`),
		segmentName(1): extendedJournalFrame(`{"id":1}`),
		segmentName(3): extendedJournalFrame(`{"id":3}`),
		segmentName(4): extendedJournalFrame(`{"id":4}`),
	}
	writeJournalStartupFiles(t, dir, files)

	j := testJournal(t, dir)
	nextJournal(t, j, `{"id":3}`)
	nextJournal(t, j, `{"id":4}`)
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("after backlog = %v, want EOF", err)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged segment was not reclaimed: %v", err)
	}
	delete(files, segmentName(1))
	assertJournalStartupFiles(t, dir, files)
}

func TestJournalOpenPreservesSegmentsWithGapAfterCheckpoint(t *testing.T) {
	for _, checkpoint := range []string{
		`{"version":1,"key":"test-destination","segment":1,"offset":0}`,
		`{"version":1,"key":"test-destination","segment":3,"offset":0}`,
	} {
		t.Run(checkpoint, func(t *testing.T) {
			dir := t.TempDir()
			files := map[string][]byte{
				"state.json":   []byte(checkpoint),
				segmentName(1): extendedJournalFrame(`{"id":1}`),
				segmentName(3): extendedJournalFrame(`{"id":3}`),
				segmentName(5): extendedJournalFrame(`{"id":5}`),
			}
			writeJournalStartupFiles(t, dir, files)

			assertJournalOpenError(t, dir, ErrSegmentGap)
			assertJournalStartupFiles(t, dir, files)
		})
	}
}

func TestJournalOpenPreservesOlderSegmentsWithInvalidCheckpointBoundary(t *testing.T) {
	dir := t.TempDir()
	files := map[string][]byte{
		"state.json":   []byte(`{"version":1,"key":"test-destination","segment":2,"offset":1}`),
		segmentName(1): extendedJournalFrame(`{"id":1}`),
		segmentName(2): extendedJournalFrame(`{"id":2}`),
		segmentName(3): extendedJournalFrame(`{"id":3}`),
	}
	writeJournalStartupFiles(t, dir, files)

	assertJournalOpenError(t, dir, ErrCheckpointBoundary)
	assertJournalStartupFiles(t, dir, files)
}

func writeJournalStartupFiles(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func assertJournalStartupFiles(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s changed: got %q, want %q", name, got, want)
		}
	}
}

func assertJournalOpenError(t *testing.T, dir string, want error) {
	t.Helper()
	j, err := Open(dir, "test-destination")
	if j != nil {
		t.Cleanup(func() {
			if err := j.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if !errors.Is(err, want) {
		t.Fatalf("open journal = %v, want %v", err, want)
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
			if err := j.Close(); err != nil {
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
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir, "test-destination"); err == nil {
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
	if err := j.Close(); err != nil {
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
	if _, err := Open(dir, "test-destination"); err == nil {
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
	if _, err := Open(dir, "test-destination"); err == nil {
		t.Fatal("concurrent journal use succeeded")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, "different-destination"); err == nil {
		t.Fatal("journal accepted another destination")
	}
	_ = testJournal(t, dir)
}

func TestJournalCreatesNestedDirectories(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "parent", "queue")
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	if err := j.Close(); err != nil {
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
	if _, err := Open(dir, "test-destination"); err == nil {
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
	if _, err := j.Next(0); !errors.Is(err, io.EOF) {
		t.Fatalf("incomplete child write replayed: %v", err)
	}
}

func TestJournalCrashHelper(t *testing.T) {
	dir := os.Getenv("SLOGX_TEST_CRASH_JOURNAL")
	if dir == "" {
		return
	}
	j, err := Open(dir, "test-destination")
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
