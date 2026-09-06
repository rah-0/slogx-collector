package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func testCollectorCommand(t *testing.T, server openObserveServer) {
	t.Helper()
	switch runtime.GOOS {
	case "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd":
	default:
		t.Skip("collector journal requires Linux, macOS, or BSD")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "slogx-collector")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "github.com/rah-0/slogx-collector")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build collector: %v\n%s", err, output)
	}

	runCommand := func(t *testing.T, stream, journalDir, password, input string) (string, error) {
		t.Helper()
		command := exec.CommandContext(ctx, binary,
			"-destination", "openobserve",
			"-endpoint", server.url+"/api/default/"+stream+"/_json",
			"-journal-dir", journalDir,
			"-username", server.username,
			"-password-env", "SLOGX_COLLECT_INTEGRATION_PASSWORD",
		)
		command.Env = append(os.Environ(), "SLOGX_COLLECT_INTEGRATION_PASSWORD="+password)
		command.Stdin = strings.NewReader(input)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		for _, secret := range []string{password, server.password} {
			if secret != "" && (strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret)) {
				t.Fatal("collector output exposed credentials")
			}
		}
		if stdout.Len() != 0 {
			t.Fatal("collector wrote output to stdout")
		}
		return stderr.String(), err
	}

	t.Run("piped JSON reaches search", func(t *testing.T) {
		firstTime := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
		secondTime := firstTime.Add(time.Microsecond)
		firstText := firstTime.Format(time.RFC3339Nano)
		secondText := secondTime.Format("2006-01-02 15:04:05.000000")
		input := fmt.Sprintf("{\"time\":%q,\"msg\":\"first\",\"n\":9007199254740993}\n{\"time\":%q,\"msg\":\"second\",\"n\":42}\n", firstText, secondText)
		if stderr, err := runCommand(t, "collector_command", t.TempDir(), server.password, input); err != nil {
			t.Fatalf("collect piped JSON: %v\n%s", err, stderr)
		}
		want := map[string]commandRecord{
			"first":  {Message: "first", Number: "9007199254740993", Time: firstText, Timestamp: firstTime.UnixMicro()},
			"second": {Message: "second", Number: "42", Time: secondText, Timestamp: secondTime.UnixMicro()},
		}
		for _, record := range server.hits(t, "collector_command", len(want)) {
			var got commandRecord
			if err := json.Unmarshal(record, &got); err != nil {
				t.Fatal(err)
			}
			expected, exists := want[got.Message]
			if !exists || got != expected {
				t.Fatalf("queried record = %+v; expected %+v", got, expected)
			}
			delete(want, got.Message)
		}
		if len(want) != 0 {
			t.Fatalf("query is missing %d records", len(want))
		}
	})

	t.Run("restart replays journal after authentication failure", func(t *testing.T) {
		journalDir := t.TempDir()
		timestamp := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
		timeText := timestamp.Format(time.RFC3339Nano)
		input := fmt.Sprintf("{\"time\":%q,\"msg\":\"replayed\",\"n\":9007199254740993}\n", timeText)
		stderr, err := runCommand(t, "collector_replay", journalDir, "incorrect-integration-password", input)
		if exitError, ok := errors.AsType[*exec.ExitError](err); !ok || exitError.ExitCode() != 1 || !strings.Contains(stderr, "HTTP status 401") {
			t.Fatalf("wrong-password collector = %v; expected exit 1 and HTTP 401\n%s", err, stderr)
		}
		if stderr, err := runCommand(t, "collector_replay", journalDir, server.password, ""); err != nil {
			t.Fatalf("replay journal with corrected credentials: %v\n%s", err, stderr)
		}
		records := server.hits(t, "collector_replay", 1)
		var got commandRecord
		if err := json.Unmarshal(records[0], &got); err != nil {
			t.Fatal(err)
		}
		want := commandRecord{Message: "replayed", Number: "9007199254740993", Time: timeText, Timestamp: timestamp.UnixMicro()}
		if got != want {
			t.Fatalf("replayed record = %+v; expected %+v", got, want)
		}
	})
	t.Run("traces", func(t *testing.T) { testCollectorTraces(t, server, binary) })
}

type commandRecord struct {
	Message   string      `json:"msg"`
	Number    json.Number `json:"n"`
	Time      string      `json:"time"`
	Timestamp int64       `json:"_timestamp"`
}
