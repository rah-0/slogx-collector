//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package journal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJournalRejectsInvalidCheckpoints(t *testing.T) {
	for _, contents := range []string{"{}", `{"version":1,"key":"test-destination","segment":1,"offset":1}`, `{"version":1,"key":"test-destination","segment":2,"offset":0}`, `{"version":2,"segment":1}`} {
		t.Run(contents, func(t *testing.T) {
			dir := t.TempDir()
			j := testJournal(t, dir)
			appendJournal(t, j, `{"id":1}`)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir, "test-destination"); err == nil {
				t.Fatal("invalid checkpoint was accepted")
			}
			if _, err := os.Stat(filepath.Join(dir, segmentName(1))); err != nil {
				t.Fatalf("invalid checkpoint caused record deletion: %v", err)
			}
		})
	}
}
