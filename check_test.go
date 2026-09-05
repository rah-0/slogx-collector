package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestCheckLeavesInputJournalAndDestinationUntouched(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	t.Setenv("COLLECTOR_CHECK_PASSWORD", "fixture-password")
	t.Setenv("COLLECTOR_CHECK_HEADER", "fixture-token")
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte("fixture-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "no credentials"},
		{name: "literal", args: []string{"-username", "fixture", "-password", "fixture-password"}},
		{name: "environment", args: []string{"-username", "fixture", "-password-env", "COLLECTOR_CHECK_PASSWORD"}},
		{name: "file", args: []string{"-username", "fixture", "-password-file", passwordFile}},
		{name: "header environment", args: []string{"-header-env", "Authorization=COLLECTOR_CHECK_HEADER"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			journalPath := filepath.Join(t.TempDir(), "journal")
			args := []string{"-check", "-destination", "openobserve", "-endpoint", server.URL, "-journal-dir", journalPath, "-field", "project=fixture"}
			args = append(args, test.args...)
			input := new(observedInput)
			var stdout, stderr bytes.Buffer
			if code := run(t.Context(), args, input, &stdout, &stderr); code != 0 {
				t.Fatalf("check returned %d: %s", code, stderr.String())
			}
			if _, err := os.Stat(journalPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("check created journal: %v", err)
			}

			// An unusable journal path must also be ignored: this mode only
			// validates configuration, even when collection could not start.
			const existing = "leave this file unchanged"
			if err := os.WriteFile(journalPath, []byte(existing), 0o600); err != nil {
				t.Fatal(err)
			}
			if code := run(t.Context(), args, input, &stdout, &stderr); code != 0 {
				t.Fatalf("check with existing journal path returned %d: %s", code, stderr.String())
			}
			got, err := os.ReadFile(journalPath)
			if err != nil || string(got) != existing {
				t.Fatalf("check changed journal path: %v", err)
			}
			if input.reads.Load() != 0 || requests.Load() != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("check had side effects: reads=%d, requests=%d, stdout=%q, stderr=%q", input.reads.Load(), requests.Load(), stdout.String(), stderr.String())
			}
		})
	}
}

func TestCheckRejectsInvalidConfigurationAndCredentialSources(t *testing.T) {
	t.Setenv("COLLECTOR_CHECK_EMPTY", "")
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "missing destination", args: []string{"-destination", ""}},
		{name: "missing journal", args: []string{"-journal-dir", ""}},
		{name: "invalid endpoint", args: []string{"-endpoint", "fixture-secret"}},
		{name: "invalid option", args: []string{"-batch-size", "-1"}},
		{name: "invalid field", args: []string{"-field", "fixture-secret"}},
		{name: "password environment", args: []string{"-password-env", "COLLECTOR_CHECK_EMPTY"}},
		{name: "header environment", args: []string{"-header-env", "Authorization=COLLECTOR_CHECK_EMPTY"}},
		{name: "password file", args: []string{"-password-file", filepath.Join(t.TempDir(), "fixture-secret")}},
		{name: "auth conflict", args: []string{"-username", "fixture", "-password", "fixture-secret", "-header", "Authorization=fixture-secret"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			journalPath := filepath.Join(t.TempDir(), "journal")
			args := []string{"-check", "-destination", "openobserve", "-endpoint", "http://127.0.0.1:1", "-journal-dir", journalPath}
			args = append(args, test.args...)
			input := new(observedInput)
			var stdout, stderr bytes.Buffer
			if code := run(t.Context(), args, input, &stdout, &stderr); code != 2 {
				t.Fatalf("invalid check returned %d, want 2", code)
			}
			if input.reads.Load() != 0 || stdout.Len() != 0 || stderr.Len() == 0 || strings.Contains(stderr.String(), "fixture-secret") {
				t.Fatal("invalid check consumed input or failed to return a private error")
			}
			if _, err := os.Stat(journalPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid check touched journal: %v", err)
			}
		})
	}
}
