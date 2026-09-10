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
	syncJournal(t, j)
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
			syncJournal(t, j)
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

func TestJournalLocksDestinationChanges(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	for _, key := range []string{"test-destination", "different-destination"} {
		if other, err := Open(dir, key); err == nil {
			_ = other.Close()
			t.Fatal("concurrent journal use succeeded")
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	_ = testJournal(t, dir)
}

func TestJournalRebindsDrainedDestination(t *testing.T) {
	for _, setup := range []string{"fresh", "acknowledged", "initial checkpoint without segment"} {
		t.Run(setup, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			if setup == "acknowledged" {
				appendJournal(t, j, `{"id":1}`)
				syncJournal(t, j)
				nextJournal(t, j, `{"id":1}`)
				if err := j.Ack(); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if setup == "initial checkpoint without segment" {
				if err := os.Remove(filepath.Join(dir, segmentName(1))); err != nil {
					t.Fatal(err)
				}
			}

			j, err := Open(dir, "different-destination")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := j.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := j.Next(0); !errors.Is(err, io.EOF) {
				t.Fatalf("rebound journal = %v, want EOF", err)
			}
			appendJournal(t, j, `{"id":2}`)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			// The new key must already be durable before a subsequent Ack.
			assertJournalOpenError(t, dir, ErrDestinationMismatch)
			reopened, err := Open(dir, "different-destination")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			nextJournal(t, reopened, `{"id":2}`)
			if _, err := reopened.Next(0); !errors.Is(err, io.EOF) {
				t.Fatalf("after rebound backlog = %v, want EOF", err)
			}
		})
	}
}

func TestJournalRejectsDestinationChangeWithUnacknowledgedRecords(t *testing.T) {
	for _, setup := range []string{"pending", "read without Ack", "empty newer segment", "incomplete tail"} {
		t.Run(setup, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			appendJournal(t, j, `{"id":1}`)
			if setup == "read without Ack" {
				syncJournal(t, j)
				nextJournal(t, j, `{"id":1}`)
				if _, err := j.Next(0); !errors.Is(err, io.EOF) {
					t.Fatalf("after reading = %v, want EOF", err)
				}
			}
			if setup == "empty newer segment" {
				if err := j.createSegment(2); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			files := map[string][]byte{"state.tmp": []byte(`{"version":`)}
			for _, name := range []string{"state.json", segmentName(1)} {
				data, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				files[name] = data
			}
			if setup == "empty newer segment" {
				files[segmentName(2)] = nil
			}
			if setup == "incomplete tail" {
				files[segmentName(1)] = append(files[segmentName(1)], 3, 0, 0)
			}
			writeJournalStartupFiles(t, dir, files)

			other, err := Open(dir, "different-destination")
			if other != nil {
				_ = other.Close()
			}
			if !errors.Is(err, ErrDestinationMismatch) {
				t.Fatalf("change destination = %v, want ErrDestinationMismatch", err)
			}
			assertJournalStartupFiles(t, dir, files)
			j = testJournal(t, dir)
			nextJournal(t, j, `{"id":1}`)
		})
	}
}

func TestJournalDestinationChangePreservesInvalidState(t *testing.T) {
	frame := extendedJournalFrame(`{"id":1}`)
	corrupt := bytes.Clone(frame)
	corrupt[4] ^= 1
	for name, files := range map[string]map[string][]byte{
		"invalid checkpoint":   {"state.json": []byte(`{`), segmentName(1): nil},
		"missing checkpoint":   {segmentName(1): frame},
		"missing segment":      {"state.json": []byte(`{"version":1,"key":"test-destination","segment":2,"offset":0}`)},
		"past segment end":     {"state.json": []byte(`{"version":1,"key":"test-destination","segment":1,"offset":1}`), segmentName(1): nil},
		"gap after checkpoint": {"state.json": []byte(`{"version":1,"key":"test-destination","segment":1,"offset":0}`), segmentName(1): nil, segmentName(3): nil},
		"corrupt frame":        {"state.json": []byte(`{"version":1,"key":"test-destination","segment":1,"offset":0}`), segmentName(1): corrupt},
		"incomplete frame":     {"state.json": []byte(`{"version":1,"key":"test-destination","segment":1,"offset":0}`), segmentName(1): frame[:len(frame)-1]},
		"unrecognized entry":   {"state.json": []byte(`{"version":1,"key":"test-destination","segment":1,"offset":0}`), segmentName(1): nil, "user-file": []byte("keep")},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeJournalStartupFiles(t, dir, files)
			if j, err := Open(dir, "different-destination"); err == nil {
				_ = j.Close()
				t.Fatal("invalid journal accepted another destination")
			}
			assertJournalStartupFiles(t, dir, files)
		})
	}
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
	for _, tail := range []string{"complete", "incomplete"} {
		t.Run(tail, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestJournalCrashHelper$")
			cmd.Env = append(os.Environ(), "SLOGX_TEST_CRASH_JOURNAL="+dir, "SLOGX_TEST_CRASH_TAIL="+tail)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child failed: %v\n%s", err, output)
			}
			j := testJournal(t, dir)
			nextJournal(t, j, `{"id":1}`)
			// Complete frames left without a sync are persisted during recovery.
			nextJournal(t, j, `{"id":2}`)
			if _, err := j.Next(0); !errors.Is(err, io.EOF) {
				t.Fatalf("incomplete child write replayed: %v", err)
			}
		})
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
	syncJournal(t, j)
	appendJournal(t, j, `{"id":2}`)
	if os.Getenv("SLOGX_TEST_CRASH_TAIL") == "incomplete" {
		var header [8]byte
		binary.LittleEndian.PutUint32(header[:4], 123)
		if _, err := j.writer.WriteAt(header[:], j.writeAt); err != nil {
			t.Fatal(err)
		}
	}
	os.Exit(0) // Deliberately bypass Close and testing cleanup.
}
