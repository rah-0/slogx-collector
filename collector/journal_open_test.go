//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalOpenRemovesStaleCheckpointAndReplaysBacklog(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(t, dir)
	appendJournal(t, j, `{"id":1}`)
	appendJournal(t, j, `{"id":2}`)
	nextJournal(t, j, `{"id":1}`)
	if err := j.ack(); err != nil {
		t.Fatal(err)
	}
	if err := j.close(); err != nil {
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
	if _, err := j.next(0); !errors.Is(err, io.EOF) {
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

	assertJournalOpenError(t, dir, ErrJournalCheckpointMissing)
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
	if _, err := j.next(0); !errors.Is(err, io.EOF) {
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

			assertJournalOpenError(t, dir, ErrJournalSegmentGap)
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

	assertJournalOpenError(t, dir, ErrJournalCheckpointBoundary)
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
	j, err := openJournal(dir, "test-destination")
	if j != nil {
		t.Cleanup(func() {
			if err := j.close(); err != nil {
				t.Error(err)
			}
		})
	}
	if !errors.Is(err, want) {
		t.Fatalf("open journal = %v, want %v", err, want)
	}
}
