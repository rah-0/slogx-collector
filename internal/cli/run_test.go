package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type observedInput struct{ reads atomic.Int64 }

func requireJournalPlatform(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd":
	default:
		t.Skip("durable journal is not supported on this platform")
	}
}

func (r *observedInput) Read([]byte) (int, error) {
	r.reads.Add(1)
	return 0, io.EOF
}

func (*observedInput) Close() error { return nil }

func TestHelpDoesNotReadInputOrExposeArguments(t *testing.T) {
	input := new(observedInput)
	var stdout, stderr bytes.Buffer
	code := Run(t.Context(), []string{"-password", "secret-value", "-header", "X-Example=secret-value", "-field", "name=secret-value", "-help"}, input, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "Usage: slogx-collector") || !strings.Contains(stdout.String(), "-journal-dir") {
		t.Fatalf("help returned %d; stdout=%q, stderr=%q", code, stdout.String(), stderr.String())
	}
	if input.reads.Load() != 0 || strings.Contains(stdout.String(), "secret-value") {
		t.Fatal("help read input or exposed arguments")
	}
}

func TestInvalidConfigurationDoesNotReadInput(t *testing.T) {
	t.Setenv("SLOGX_COLLECT_TEST_EMPTY", "")
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "unknown flag", args: []string{"-secret-value"}},
		{name: "invalid number", args: []string{"-batch-size", "secret-value"}},
		{name: "positional argument", args: []string{"secret-value"}},
		{name: "unsupported source", args: []string{"-input", "secret-value"}},
		{name: "unsupported destination", args: []string{"-destination", "secret-value"}},
		{name: "missing destination", args: []string{"-destination", ""}},
		{name: "missing endpoint", args: []string{"-endpoint", ""}},
		{name: "missing journal", args: []string{"-journal-dir", ""}},
		{name: "embedded credentials", args: []string{"-endpoint", "http://user:secret-value@example.invalid"}},
		{name: "zero batch size", args: []string{"-batch-size", "0"}},
		{name: "negative bytes", args: []string{"-batch-bytes", "-1"}},
		{name: "zero flush", args: []string{"-flush-interval", "0"}},
		{name: "zero request timeout", args: []string{"-request-timeout", "0"}},
		{name: "zero shutdown timeout", args: []string{"-shutdown-timeout", "0"}},
		{name: "negative retries", args: []string{"-max-retries", "-1"}},
		{name: "decreasing retry delay", args: []string{"-retry-interval", "2s", "-max-retry-interval", "1s"}},
		{name: "malformed header", args: []string{"-header", "secret-value"}},
		{name: "malformed field", args: []string{"-field", "secret-value"}},
		{name: "empty field name", args: []string{"-field", "=secret-value"}},
		{name: "malformed environment header", args: []string{"-header-env", "secret-value"}},
		{name: "empty environment header", args: []string{"-header-env", "Authorization=SLOGX_COLLECT_TEST_EMPTY"}},
		{name: "empty environment password", args: []string{"-password-env", "SLOGX_COLLECT_TEST_EMPTY"}},
		{name: "empty environment name", args: []string{"-password-env", ""}},
		{name: "empty password path", args: []string{"-password-file", ""}},
		{name: "unreadable password file", args: []string{"-password-file", filepath.Join(t.TempDir(), "secret-value")}},
		{name: "password source conflict", args: []string{"-password", "secret-value", "-password-env", "SLOGX_COLLECT_TEST_EMPTY"}},
		{name: "empty literal password conflict", args: []string{"-password", "", "-password-file", "secret-value"}},
		{name: "authentication conflict", args: []string{"-username", "user", "-password", "secret-value", "-header", "Authorization=secret-value"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			journalDir := filepath.Join(t.TempDir(), "journal")
			args := []string{"-destination", "openobserve", "-endpoint", "http://127.0.0.1:1", "-journal-dir", journalDir}
			args = append(args, test.args...)
			input := new(observedInput)
			var stdout, stderr bytes.Buffer
			code := Run(t.Context(), args, input, &stdout, &stderr)
			if code != 2 || stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("invalid config returned %d; stdout=%q, stderr=%q", code, stdout.String(), stderr.String())
			}
			if input.reads.Load() != 0 {
				t.Fatal("invalid configuration read stdin")
			}
			if strings.Contains(stderr.String(), "secret-value") {
				t.Fatal("configuration error exposed an argument value")
			}
			if _, err := os.Stat(journalDir); !os.IsNotExist(err) {
				t.Fatalf("invalid configuration touched journal: %v", err)
			}
		})
	}
}

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
			if code := Run(t.Context(), args, input, &stdout, &stderr); code != 0 {
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
			if code := Run(t.Context(), args, input, &stdout, &stderr); code != 0 {
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
		{name: "invalid header name", args: []string{"-header", "Invalid Header=fixture-secret"}},
		{name: "invalid header value", args: []string{"-header", "Authorization=fixture-secret\nvalue"}},
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
			if code := Run(t.Context(), args, input, &stdout, &stderr); code != 2 {
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

func TestRunAddsConfiguredFields(t *testing.T) {
	requireJournalPlatform(t)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var records []struct {
			Project string      `json:"project"`
			Version string      `json:"version"`
			Empty   string      `json:"empty"`
			Number  json.Number `json:"n"`
		}
		if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(records) != 1 {
			t.Errorf("received %d records, want 1", len(records))
		} else if got := records[0]; got.Project != "example=service" || got.Version != "v1'\"雪" || got.Empty != "" || got.Number != "9007199254740993" {
			t.Errorf("unexpected enriched record: %+v", got)
		}
		if _, err := fmt.Fprintf(w, `{"code":200,"status":[{"successful":%d,"failed":0}]}`, len(records)); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	args := []string{
		"-destination", "openobserve", "-endpoint", server.URL, "-journal-dir", t.TempDir(),
		"-field", "project=old", "-field", "project=example=service",
		"-field", "version=v1'\"雪", "-field", "empty=",
	}
	input := io.NopCloser(strings.NewReader(`{"project":"from application","version":"stale","empty":"replaced","n":9007199254740993}`))
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if code := Run(ctx, args, input, &stdout, &stderr); code != 0 {
		t.Fatalf("run returned %d: %s", code, stderr.String())
	}
	if calls.Load() != 1 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("calls=%d, stdout=%q, stderr=%q", calls.Load(), stdout.String(), stderr.String())
	}
}

func TestRunDeliversJSON(t *testing.T) {
	requireJournalPlatform(t)
	t.Setenv("SLOGX_COLLECT_TEST_PASSWORD", "secret-value")
	t.Setenv("SLOGX_COLLECT_TEST_HEADER", "header-value")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		user, password, ok := r.BasicAuth()
		if !ok || user != "writer" || password != "secret-value" || r.Header.Get("X-Token") != "header-value" || r.Header.Get("X-Example") != "one=two" {
			t.Error("request authentication or headers differ")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		want := `[{"time":"2026-09-05T12:00:00Z","n":9007199254740993,"n":2,"_timestamp":1788609600000000}]`
		if string(body) != want {
			t.Errorf("body=%s; want %s", body, want)
		}
		fmt.Fprint(w, `{"code":200,"status":[{"name":"logs","successful":1,"failed":0}]}`)
	}))
	defer server.Close()
	args := []string{
		"-destination", "openobserve", "-endpoint", server.URL + "/api/default/logs/_json", "-journal-dir", t.TempDir(),
		"-username", "writer", "-password-env", "SLOGX_COLLECT_TEST_PASSWORD",
		"-header-env", "X-Token=SLOGX_COLLECT_TEST_HEADER", "-header", "X-Example=one=two",
	}
	input := io.NopCloser(strings.NewReader("{\"time\":\"2026-09-05T12:00:00Z\",\"n\":9007199254740993,\"n\":2}\n"))
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if code := Run(ctx, args, input, &stdout, &stderr); code != 0 {
		t.Fatalf("run returned %d: %s", code, stderr.String())
	}
	if calls.Load() != 1 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("calls=%d, stdout=%q, stderr=%q", calls.Load(), stdout.String(), stderr.String())
	}
}

func TestRunKeepsUndeliveredPrefixAfterMalformedInput(t *testing.T) {
	requireJournalPlatform(t)
	var healthy atomic.Bool
	var accepted atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var records []json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
			t.Error(err)
		}
		for _, record := range records {
			if string(record) != `{"msg":"pending"}` {
				t.Errorf("replayed unexpected record %s", record)
			}
		}
		accepted.Add(int64(len(records)))
		fmt.Fprintf(w, `{"code":200,"status":[{"successful":%d,"failed":0}]}`, len(records))
	}))
	defer server.Close()
	args := []string{
		"-destination", "openobserve", "-endpoint", server.URL, "-journal-dir", t.TempDir(),
		"-retry-interval", "1ms", "-max-retry-interval", "1ms", "-max-retries", "1", "-shutdown-timeout", "100ms",
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	input := io.NopCloser(strings.NewReader("{\"msg\":\"pending\"}\nsecret-value\n"))
	if code := Run(ctx, args, input, &stdout, &stderr); code != 1 {
		t.Fatalf("malformed input returned %d: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "secret-value") || stdout.Len() != 0 {
		t.Fatal("diagnostics exposed input or wrote to stdout")
	}
	healthy.Store(true)
	stderr.Reset()
	if code := Run(ctx, args, io.NopCloser(strings.NewReader("")), &stdout, &stderr); code != 0 {
		t.Fatalf("journal replay returned %d: %s", code, stderr.String())
	}
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d records after restart; want 1", accepted.Load())
	}
}

func TestCanceledRunIsNonzero(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	args := []string{"-destination", "openobserve", "-endpoint", "http://127.0.0.1:1", "-journal-dir", t.TempDir()}
	var stdout, stderr bytes.Buffer
	if code := Run(ctx, args, io.NopCloser(strings.NewReader("")), &stdout, &stderr); code != 1 {
		t.Fatalf("canceled run returned %d; stderr=%s", code, stderr.String())
	}
}

const traceEnvelope = `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"example-service"}}]},"scopeSpans":[{"spans":[{"traceId":"11111111111111111111111111111111","spanId":"2222222222222222","name":"operation","startTimeUnixNano":"1788609600000000000","endTimeUnixNano":"1788609600001000000","attributes":[{"key":"item_id","value":{"intValue":"9007199254740993"}}]}]}]}]}`

func TestTraceCheckAndLogOptionRejection(t *testing.T) {
	t.Setenv("TRACE_CHECK_PASSWORD", "fixture-password")
	for _, test := range []struct {
		name string
		args []string
		code int
	}{
		{name: "valid", code: 0},
		{name: "credentials", args: []string{"-username", "fixture", "-password-env", "TRACE_CHECK_PASSWORD"}, code: 0},
		{name: "field", args: []string{"-field", "service.name=fixture-secret"}, code: 0},
		{name: "resource", args: []string{"-resource", "service.name=fixture-secret"}, code: 0},
		{name: "invalid resource", args: []string{"-resource", "fixture-secret"}, code: 2},
		{name: "duplicate trace endpoint", args: []string{"-traces-endpoint", "http://localhost:5080/v1/traces"}, code: 2},
		{name: "timestamp field", args: []string{"-timestamp-field", "fixture-secret"}, code: 2},
		{name: "empty timestamp field", args: []string{"-timestamp-field", ""}, code: 2},
		{name: "timestamp layout", args: []string{"-timestamp-layout", "fixture-secret"}, code: 2},
		{name: "auth conflict", args: []string{"-username", "fixture", "-password", "fixture-secret", "-header", "Authorization=fixture-secret"}, code: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal := filepath.Join(t.TempDir(), "journal")
			args := []string{"-check", "-destination", "otlp", "-endpoint", "http://127.0.0.1:1/v1/traces", "-journal-dir", journal}
			args = append(args, test.args...)
			input := new(observedInput)
			var stdout, stderr bytes.Buffer
			if code := Run(t.Context(), args, input, &stdout, &stderr); code != test.code {
				t.Fatalf("code = %d, want %d; stderr=%s", code, test.code, &stderr)
			}
			if input.reads.Load() != 0 || stdout.Len() != 0 || strings.Contains(stderr.String(), "fixture-secret") {
				t.Fatal("trace check read input or exposed arguments")
			}
			if _, err := os.Stat(journal); !os.IsNotExist(err) {
				t.Fatalf("trace check touched journal: %v", err)
			}
		})
	}
}

func TestRunDeliversTraceEnvelopes(t *testing.T) {
	requireJournalPlatform(t)
	t.Setenv("TRACE_TEST_PASSWORD", "fixture-password")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		user, password, ok := r.BasicAuth()
		if !ok || user != "fixture" || password != "fixture-password" || r.Header.Get("Stream-Name") != "traces" || r.URL.Path != "/api/default/v1/traces" {
			t.Error("trace destination, credentials, or stream differ")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var expected, got map[string]json.RawMessage
		if json.Unmarshal([]byte(traceEnvelope), &expected) != nil || json.Unmarshal(body, &got) != nil ||
			len(got) != 1 || !bytes.Equal(expected["resourceSpans"], got["resourceSpans"]) {
			t.Errorf("trace body changed: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	args := []string{
		"-destination", "otlp", "-endpoint", server.URL + "/api/default/v1/traces", "-journal-dir", t.TempDir(),
		"-username", "fixture", "-password-env", "TRACE_TEST_PASSWORD", "-header", "stream-name=traces",
	}
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if code := Run(ctx, args, io.NopCloser(strings.NewReader(traceEnvelope+"\n")), &stdout, &stderr); code != 0 {
		t.Fatalf("trace run returned %d: %s", code, &stderr)
	}
	if calls.Load() != 1 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("calls=%d stdout=%q stderr=%q", calls.Load(), &stdout, &stderr)
	}
}

func TestTraceReplayRejectsChangedStream(t *testing.T) {
	requireJournalPlatform(t)
	var healthy atomic.Bool
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Stream-Name") != "original" {
			t.Error("backlog sent to another stream")
		}
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	t.Setenv("TRACE_REPLAY_STREAM", "original")
	args := []string{
		"-destination", "otlp", "-endpoint", server.URL, "-journal-dir", t.TempDir(),
		"-header-env", "stream-name=TRACE_REPLAY_STREAM", "-max-retries", "1",
		"-retry-interval", "1ms", "-max-retry-interval", "1ms",
	}
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if code := Run(ctx, args, io.NopCloser(strings.NewReader(traceEnvelope)), &stdout, &stderr); code != 1 {
		t.Fatalf("unavailable run returned %d: %s", code, &stderr)
	}
	before := calls.Load()
	healthy.Store(true)
	t.Setenv("TRACE_REPLAY_STREAM", "changed")
	stderr.Reset()
	input := new(observedInput)
	if code := Run(ctx, args, input, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "different destination") {
		t.Fatalf("changed-stream replay returned %d: %s", code, &stderr)
	}
	if calls.Load() != before || input.reads.Load() != 0 {
		t.Fatal("changed destination sent backlog or read new input")
	}
	t.Setenv("TRACE_REPLAY_STREAM", "original")
	stderr.Reset()
	changedResource := append(append([]string(nil), args...), "-resource", "service.name=changed")
	if code := Run(ctx, changedResource, new(observedInput), &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "different destination") {
		t.Fatalf("changed-resource replay returned %d: %s", code, &stderr)
	}
	if calls.Load() != before {
		t.Fatal("changed resource sent backlog")
	}
	stderr.Reset()
	if code := Run(ctx, args, io.NopCloser(strings.NewReader("")), &stdout, &stderr); code != 0 {
		t.Fatalf("original-stream replay returned %d: %s", code, &stderr)
	}
	if calls.Load() != before+1 {
		t.Fatal("retained backlog was not delivered once on restart")
	}
}
