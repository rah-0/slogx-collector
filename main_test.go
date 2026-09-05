package main

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
	code := run(t.Context(), []string{"-password", "secret-value", "-header", "X-Example=secret-value", "-help"}, input, &stdout, &stderr)
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
			code := run(t.Context(), args, input, &stdout, &stderr)
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

func TestConfigurationErrorsCanBeReferenced(t *testing.T) {
	for _, test := range []struct {
		args []string
		want error
	}{
		{[]string{"-unknown"}, ErrInvalidOptions},
		{[]string{"-input", "file"}, ErrUnsupportedInput},
		{[]string{"-destination", ""}, ErrUnsupportedDestination},
		{[]string{"-request-timeout", "-1s"}, ErrNonpositiveLimits},
		{[]string{"-password", "value", "-password-env", "NAME"}, ErrPasswordSourceConflict},
	} {
		args := []string{"-destination", "openobserve", "-endpoint", "http://localhost:5080", "-journal-dir", t.TempDir()}
		_, err := parseConfig(append(args, test.args...), io.Discard)
		if !errors.Is(err, test.want) {
			t.Fatalf("configuration error = %v, want %v", err, test.want)
		}
	}
	_, err := (commandConfig{headers: repeatedFlag{"missing-separator"}}).openObserve()
	if !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("header error = %v, want %v", err, ErrInvalidHeader)
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
	if code := run(ctx, args, input, &stdout, &stderr); code != 0 {
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
	if code := run(ctx, args, input, &stdout, &stderr); code != 1 {
		t.Fatalf("malformed input returned %d: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "secret-value") || stdout.Len() != 0 {
		t.Fatal("diagnostics exposed input or wrote to stdout")
	}
	healthy.Store(true)
	stderr.Reset()
	if code := run(ctx, args, io.NopCloser(strings.NewReader("")), &stdout, &stderr); code != 0 {
		t.Fatalf("journal replay returned %d: %s", code, stderr.String())
	}
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d records after restart; want 1", accepted.Load())
	}
}

func TestPasswordFile(t *testing.T) {
	wantPassword := " " + strings.Repeat("secret-value", 6000) + " "
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte(wantPassword+"\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, password, ok := r.BasicAuth()
		if !ok || password != wantPassword {
			t.Error("password file did not preserve spaces and remove line endings")
		}
		fmt.Fprint(w, `{"code":200,"status":[{"successful":1,"failed":0}]}`)
	}))
	defer server.Close()
	cfg := commandConfig{endpoint: server.URL, username: "user", passwordFile: passwordFile}
	destination, err := cfg.openObserve()
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
}

func TestCanceledRunIsNonzero(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	args := []string{"-destination", "openobserve", "-endpoint", "http://127.0.0.1:1", "-journal-dir", t.TempDir()}
	var stdout, stderr bytes.Buffer
	if code := run(ctx, args, io.NopCloser(strings.NewReader("")), &stdout, &stderr); code != 1 {
		t.Fatalf("canceled run returned %d; stderr=%s", code, stderr.String())
	}
}
