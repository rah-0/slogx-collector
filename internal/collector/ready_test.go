//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/rah-0/slogx-collector/internal/journal"
)

func TestRunReadinessFailureClosesInputAndPreservesBacklog(t *testing.T) {
	dir := t.TempDir()
	store, err := journal.Open(dir, "target")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Append(json.RawMessage(`{"old":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reader := strings.NewReader("{\"new\":true}\n")
	input := &closeObserver{Reader: reader}
	wantUnread := reader.Len()
	failure := errors.New("readiness receiver closed")
	var batches [][]string
	err = Run(t.Context(), input, captureBatches(&batches), Options{
		JournalDir: dir,
		JournalKey: "target",
		OnReady:    func() error { return failure },
	})
	if !errors.Is(err, failure) {
		t.Fatalf("Run = %v, want readiness failure", err)
	}
	if !input.closed || input.closeCalls != 1 || reader.Len() != wantUnread || len(batches) != 0 {
		t.Fatal("readiness failure consumed input, delivered records, or failed to close input once")
	}

	// Reopening proves the failed startup released its lock and retained the
	// original backlog without accepting the new input.
	if err := Run(t.Context(), io.NopCloser(strings.NewReader("")), captureBatches(&batches), Options{
		JournalDir: dir,
		JournalKey: "target",
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(batches, [][]string{{`{"old":true}`}}) {
		t.Fatalf("replayed batches = %#v", batches)
	}
}
