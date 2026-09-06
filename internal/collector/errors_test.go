//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package collector_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/journal"
	"github.com/rah-0/slogx-collector/internal/jsonx"
)

type errorTestDestination func(context.Context, []json.RawMessage) error

func (f errorTestDestination) Send(ctx context.Context, records []json.RawMessage) error {
	return f(ctx, records)
}

func TestInputErrorsAreMatchableWithoutExposingInput(t *testing.T) {
	for _, test := range []struct {
		name  string
		input io.Reader
		want  error
	}{
		{"invalid JSON", strings.NewReader("{}\n{\"secret-token\":"), jsonx.ErrInvalidRecord},
		{"read failure", io.MultiReader(strings.NewReader("{}\n"), errorTestReader{}), jsonx.ErrRead},
	} {
		t.Run(test.name, func(t *testing.T) {
			delivered := 0
			err := collector.Run(t.Context(), io.NopCloser(test.input), errorTestDestination(func(_ context.Context, records []json.RawMessage) error {
				delivered += len(records)
				return nil
			}), collector.Options{JournalDir: t.TempDir()})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.want)
			}
			if !strings.Contains(err.Error(), "input line 2") || strings.Contains(err.Error(), "secret-token") {
				t.Fatalf("error lost line context or exposed input: %v", err)
			}
			if delivered != 1 {
				t.Fatalf("delivered records = %d, want preceding valid record", delivered)
			}
		})
	}
}

func TestTerminalDeliveryErrorRetainsSentinel(t *testing.T) {
	failure := errors.New("destination rejected record")
	err := collector.Run(t.Context(), io.NopCloser(strings.NewReader("{}\n")), errorTestDestination(func(context.Context, []json.RawMessage) error {
		return failure
	}), collector.Options{JournalDir: t.TempDir()})
	if !errors.Is(err, failure) {
		t.Fatalf("wrapped delivery error = %v", err)
	}
	if !errors.Is(&collector.RetryError{}, collector.ErrTemporaryDelivery) {
		t.Fatal("default retry error does not expose its static cause")
	}
}

func TestRunPreservesJournalErrors(t *testing.T) {
	run := func(t *testing.T, dir, key string) error {
		t.Helper()
		return collector.Run(t.Context(), io.NopCloser(strings.NewReader("")), errorTestDestination(func(context.Context, []json.RawMessage) error {
			return nil
		}), collector.Options{JournalDir: dir, JournalKey: key})
	}
	t.Run("destination mismatch", func(t *testing.T) {
		dir := t.TempDir()
		if err := run(t, dir, "first-target"); err != nil {
			t.Fatal(err)
		}
		if err := run(t, dir, "second-target"); !errors.Is(err, journal.ErrDestinationMismatch) {
			t.Fatalf("journal mismatch = %v", err)
		}
	})
	t.Run("invalid checkpoint", func(t *testing.T) {
		dir := t.TempDir()
		if err := run(t, dir, "target"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := run(t, dir, "target"); !errors.Is(err, journal.ErrInvalidCheckpoint) {
			t.Fatalf("invalid checkpoint = %v", err)
		}
	})
	t.Run("checksum mismatch", func(t *testing.T) {
		dir := t.TempDir()
		failure := errors.New("destination rejected record")
		err := collector.Run(t.Context(), io.NopCloser(strings.NewReader("{}\n")), errorTestDestination(func(context.Context, []json.RawMessage) error {
			return failure
		}), collector.Options{JournalDir: dir, JournalKey: "target"})
		if !errors.Is(err, failure) {
			t.Fatal(err)
		}
		segments, err := filepath.Glob(filepath.Join(dir, "*.segment"))
		if err != nil || len(segments) != 1 {
			t.Fatalf("journal segments = %v, error = %v", segments, err)
		}
		data, err := os.ReadFile(segments[0])
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)-1] ^= 1
		if err := os.WriteFile(segments[0], data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := run(t, dir, "target"); !errors.Is(err, journal.ErrChecksum) {
			t.Fatalf("journal corruption = %v", err)
		}
	})
}

type errorTestReader struct{}

func (errorTestReader) Read([]byte) (int, error) {
	return 0, errors.New("reader failed while handling secret-token")
}
