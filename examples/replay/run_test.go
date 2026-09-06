package replay

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/rah-0/slogx-collector/examples/internal/exampletest"
)

func TestReplayRetainsOriginalRecords(t *testing.T) {
	binary := exampletest.BuildCollector(t)
	fixture, err := os.ReadFile("events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte("["), bytes.Join(bytes.Split(bytes.TrimSpace(fixture), []byte("\n")), []byte(","))...)
	want = append(want, ']')

	var mu sync.Mutex
	var available bool
	var requests [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			http.Error(writer, "read failed", http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests = append(requests, body)
		ready := available
		mu.Unlock()
		if !ready {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var records []json.RawMessage
		if err := json.Unmarshal(body, &records); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(writer, "invalid JSON", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(writer, `{"code":200,"status":[{"successful":%d,"failed":0}]}`, len(records))
	}))
	t.Cleanup(server.Close)
	journal := t.TempDir()
	run := func(mode string, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), "bash", append([]string{"run.sh", mode}, args...)...)
		cmd.Env = append(os.Environ(),
			"COLLECTOR_BIN="+binary, "LOG_ENDPOINT="+server.URL, "JOURNAL_DIR="+journal)
		return cmd.CombinedOutput()
	}

	output, err := run("ingest")
	exit, ok := errors.AsType[*exec.ExitError](err)
	if !ok || exit.ExitCode() != 1 || !strings.Contains(string(output), "503") {
		t.Fatalf("outage result = %v\n%s", err, output)
	}
	mu.Lock()
	attempts := len(requests)
	available = true
	mu.Unlock()
	if attempts != 2 {
		t.Fatalf("outage attempts = %d, want initial attempt and one retry", attempts)
	}

	// Static fields only apply to new input; they must not relabel the backlog.
	if output, err := run("replay", "-field", "deployment=after-restart"); err != nil {
		t.Fatalf("recover backlog: %v\n%s", err, output)
	}
	mu.Lock()
	attempts = len(requests)
	mu.Unlock()
	if attempts != 3 {
		t.Fatalf("requests after recovery = %d, want 3", attempts)
	}
	if output, err := run("replay"); err != nil {
		t.Fatalf("replay acknowledged journal: %v\n%s", err, output)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != attempts {
		t.Fatalf("acknowledged records were delivered again: %d requests", len(requests))
	}
	for i, request := range requests {
		if !bytes.Equal(request, want) {
			t.Errorf("request %d changed original records:\ngot  %s\nwant %s", i, request, want)
		}
	}
}
