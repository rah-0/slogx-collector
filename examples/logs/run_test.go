package logs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/rah-0/slogx-collector/examples/internal/exampletest"
)

func TestRun(t *testing.T) {
	binary := exampletest.BuildCollector(t)
	var mu sync.Mutex
	var received []json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/default/application/_json" {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
		var batch []json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&batch); err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, batch...)
		mu.Unlock()
		fmt.Fprintf(writer, `{"code":200,"status":[{"successful":%d,"failed":0}]}`, len(batch))
	}))
	defer server.Close()
	command := exec.CommandContext(t.Context(), "bash", "run.sh",
		"-max-retries", "1", "-retry-interval", "1ms", "-max-retry-interval", "1ms",
		"-request-timeout", "1s")
	command.Env = append(os.Environ(),
		"COLLECTOR_BIN="+binary,
		"LOG_ENDPOINT="+server.URL+"/api/default/application/_json",
		"JOURNAL_DIR="+t.TempDir(),
	)
	if output, err := command.CombinedOutput(); err != nil || len(output) != 0 {
		t.Fatalf("run example: %v\n%s", err, output)
	}
	input, err := os.ReadFile("events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Split(strings.TrimSpace(string(input)), "\n")
	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(want) {
		t.Fatalf("received %d records, want %d", len(received), len(want))
	}
	for i, record := range received {
		if !bytes.Equal(record, []byte(want[i])) {
			t.Fatalf("record %d changed: %s", i, record)
		}
	}
}
