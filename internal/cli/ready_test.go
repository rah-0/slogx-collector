package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rah-0/slogx-collector/internal/collector/otlp"
	"github.com/rah-0/slogx-collector/internal/journal"
)

type readyWriterFunc func([]byte) (int, error)

func (write readyWriterFunc) Write(p []byte) (int, error) { return write(p) }

func TestReadyPrecedesInputAndBacklogDelivery(t *testing.T) {
	requireJournalPlatform(t)
	var requests, signals atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if signals.Load() != 1 {
			t.Error("delivery started before readiness")
		}
		fmt.Fprint(w, `{"code":200,"status":[{"successful":1,"failed":0}]}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	args := []string{"-ready", "-destination", "openobserve", "-endpoint", server.URL, "-journal-dir", dir}
	cfg, err := parseConfig(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.newDestination(); err != nil {
		t.Fatal(err)
	}
	store, err := journal.Open(dir, cfg.options.JournalKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(json.RawMessage(`{"msg":"pending"}`)); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	input := new(observedInput)
	var stdout, stderr bytes.Buffer
	output := readyWriterFunc(func(p []byte) (int, error) {
		if input.reads.Load() != 0 || requests.Load() != 0 {
			t.Error("input or delivery started before readiness")
		}
		if unlocked, err := journal.Open(dir, cfg.options.JournalKey); err == nil {
			unlocked.Close()
			t.Error("journal was not exclusively locked at readiness")
		}
		signals.Add(1)
		return stdout.Write(p)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if code := Run(ctx, args, input, output, &stderr); code != 0 {
		t.Fatalf("run returned %d: %s", code, &stderr)
	}
	if stdout.String() != "ready\n" || stderr.Len() != 0 || signals.Load() != 1 || requests.Load() != 1 || input.reads.Load() == 0 {
		t.Fatalf("signals=%d requests=%d reads=%d stdout=%q stderr=%q", signals.Load(), requests.Load(), input.reads.Load(), &stdout, &stderr)
	}
}

func TestReadyIsSuppressedByCheckAndStartupFailures(t *testing.T) {
	requireJournalPlatform(t)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	for _, test := range []struct {
		name string
		code int
	}{
		{name: "check", code: 0},
		{name: "invalid configuration", code: 2},
		{name: "invalid destination", code: 2},
		{name: "corrupt journal", code: 1},
		{name: "locked journal", code: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			args := []string{"-ready", "-destination", "openobserve", "-endpoint", server.URL, "-journal-dir", dir}
			switch test.name {
			case "check", "locked journal":
				store, err := journal.Open(dir, "fixture")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				})
				if test.name == "check" {
					args = append(args, "-check")
				}
			case "invalid configuration":
				args = append(args, "-batch-size", "0")
			case "invalid destination":
				args = append(args, "-endpoint", "invalid-url")
			case "corrupt journal":
				if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("invalid checkpoint"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			input := new(observedInput)
			var stdout, stderr bytes.Buffer
			if code := Run(t.Context(), args, input, &stdout, &stderr); code != test.code {
				t.Fatalf("run returned %d, want %d: %s", code, test.code, &stderr)
			}
			if input.reads.Load() != 0 || requests.Load() != 0 || stdout.Len() != 0 || (stderr.Len() != 0) != (test.code != 0) {
				t.Fatalf("reads=%d requests=%d stdout=%q stderr=%q", input.reads.Load(), requests.Load(), &stdout, &stderr)
			}
		})
	}
}

func TestReadyWriteFailureStopsBeforeInputAndReleasesJournal(t *testing.T) {
	requireJournalPlatform(t)
	dir := t.TempDir()
	args := []string{"-ready", "-destination", "openobserve", "-endpoint", "http://127.0.0.1:1", "-journal-dir", dir}
	input := new(observedInput)
	var writes int
	output := readyWriterFunc(func(p []byte) (int, error) {
		writes++
		if string(p) != "ready\n" {
			t.Errorf("readiness bytes = %q", p)
		}
		return 0, io.ErrClosedPipe
	})
	var stderr bytes.Buffer
	if code := Run(t.Context(), args, input, output, &stderr); code != 1 || !strings.Contains(stderr.String(), io.ErrClosedPipe.Error()) {
		t.Fatalf("run returned %d: %s", code, &stderr)
	}
	if writes != 1 || input.reads.Load() != 0 {
		t.Fatalf("writes=%d reads=%d", writes, input.reads.Load())
	}
	stderr.Reset()
	var stdout bytes.Buffer
	if code := Run(t.Context(), args, new(observedInput), &stdout, &stderr); code != 0 || stdout.String() != "ready\n" || stderr.Len() != 0 {
		t.Fatalf("restart returned %d; stdout=%q stderr=%q", code, &stdout, &stderr)
	}
}

func TestReadyAllowsEnablingTracesAfterLogJournalDrains(t *testing.T) {
	requireJournalPlatform(t)
	var logs, traces atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/logs":
			var records []struct{ Msg string }
			if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
				t.Error(err)
			}
			want := "before"
			if logs.Add(1) == 2 {
				want = "after"
			}
			if len(records) != 1 || records[0].Msg != want {
				t.Errorf("logs=%+v, want one %q record", records, want)
			}
			fmt.Fprintf(w, `{"code":200,"status":[{"successful":%d,"failed":0}]}`, len(records))
		case "/traces":
			traces.Add(1)
			var envelope otlp.ExportRequest
			if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
				t.Error(err)
			} else if len(envelope.ResourceSpans) != 1 || len(envelope.ResourceSpans[0].ScopeSpans) != 1 || len(envelope.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
				t.Errorf("unexpected trace envelope: %+v", envelope)
			} else if span := envelope.ResourceSpans[0].ScopeSpans[0].Spans[0]; span.Name != "operation" {
				t.Errorf("trace name = %q", span.Name)
			}
			fmt.Fprint(w, `{}`)
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	args := []string{"-ready", "-destination", "openobserve", "-endpoint", server.URL + "/logs", "-journal-dir", t.TempDir()}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	if code := Run(ctx, args, io.NopCloser(strings.NewReader(`{"msg":"before"}`)), &stdout, &stderr); code != 0 || stdout.String() != "ready\n" || stderr.Len() != 0 {
		t.Fatalf("logs-only run returned %d; stdout=%q stderr=%q", code, &stdout, &stderr)
	}
	args = append(args, "-traces-endpoint", server.URL+"/traces", "-header", "stream-name=traces", "-resource", "service.name=application")
	const span = `{"msg":"operation","span":{"trace_id":"11111111111111111111111111111111","span_id":"2222222222222222","name":"operation","kind":1,"start_time":"2026-09-06T12:00:01Z","end_time":"2026-09-06T12:00:02Z","status":0}}`
	stdout.Reset()
	stderr.Reset()
	input := io.NopCloser(strings.NewReader("{\"msg\":\"after\"}\n" + span + "\n"))
	if code := Run(ctx, args, input, &stdout, &stderr); code != 0 || stdout.String() != "ready\n" || stderr.Len() != 0 {
		t.Fatalf("mixed run returned %d; stdout=%q stderr=%q", code, &stdout, &stderr)
	}
	if logs.Load() != 2 || traces.Load() != 1 {
		t.Fatalf("log requests=%d trace requests=%d", logs.Load(), traces.Load())
	}
}

func TestReadyRejectsEnablingTracesWithPendingLogs(t *testing.T) {
	requireJournalPlatform(t)
	var requests atomic.Int64
	var healthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/logs" {
			t.Errorf("pending logs reached %q", r.URL.Path)
		}
		if !healthy.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != `[{"msg":"pending"}]` {
			t.Errorf("replayed body=%s, error=%v", body, err)
		}
		fmt.Fprint(w, `{"code":200,"status":[{"successful":1,"failed":0}]}`)
	}))
	defer server.Close()
	args := []string{"-destination", "openobserve", "-endpoint", server.URL + "/logs", "-journal-dir", t.TempDir()}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	if code := Run(ctx, args, io.NopCloser(strings.NewReader(`{"msg":"pending"}`)), &stdout, &stderr); code != 1 || requests.Load() != 1 {
		t.Fatalf("failed delivery returned %d; requests=%d stderr=%q", code, requests.Load(), &stderr)
	}
	healthy.Store(true)
	changed := append(append([]string(nil), args...), "-ready", "-traces-endpoint", server.URL+"/traces")
	input := new(observedInput)
	stderr.Reset()
	if code := Run(ctx, changed, input, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "different destination") {
		t.Fatalf("changed destination returned %d: %s", code, &stderr)
	}
	if input.reads.Load() != 0 || requests.Load() != 1 || stdout.Len() != 0 {
		t.Fatalf("reads=%d requests=%d stdout=%q", input.reads.Load(), requests.Load(), &stdout)
	}
	stderr.Reset()
	if code := Run(ctx, args, new(observedInput), &stdout, &stderr); code != 0 || requests.Load() != 2 || stderr.Len() != 0 {
		t.Fatalf("original destination replay returned %d; requests=%d stderr=%q", code, requests.Load(), &stderr)
	}
}
