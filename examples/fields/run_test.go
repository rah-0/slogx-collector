package fields

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/rah-0/slogx-collector/examples/internal/exampletest"
)

func TestFieldOverridesPreserveValues(t *testing.T) {
	binary := exampletest.BuildCollector(t)
	var mu sync.Mutex
	var received []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("X-Example") != "fields" {
			t.Error("script did not send a POST with the trailing header option")
			http.Error(writer, "unexpected request", http.StatusBadRequest)
			return
		}
		var records []map[string]json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&records); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(writer, "invalid JSON", http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, records...)
		mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(writer, `{"code":200,"status":[{"successful":%d,"failed":0}]}`, len(records))
	}))
	t.Cleanup(server.Close)

	cmd := exec.CommandContext(t.Context(), "bash", "run.sh",
		"-field", "deployment=blue", "-header", "X-Example=fields",
		"-max-retries", "1", "-retry-interval", "1ms", "-max-retry-interval", "1ms",
		"-request-timeout", "1s")
	cmd.Env = append(os.Environ(),
		"COLLECTOR_BIN="+binary, "LOG_ENDPOINT="+server.URL, "JOURNAL_DIR="+t.TempDir())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run script: %v\n%s", err, output)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 {
		t.Fatalf("records = %d, want 2", len(received))
	}
	for i, record := range received {
		for key, value := range map[string]string{
			"environment": `"staging"`, "service": `"catalog"`, "deployment": `"blue"`,
		} {
			if string(record[key]) != value {
				t.Errorf("record %d %s = %s, want %s", i, key, record[key], value)
			}
		}
		want := []string{"9007199254740993", "9007199254740995"}[i]
		if string(record["item_id"]) != want {
			t.Errorf("record %d item_id = %s, want %s", i, record["item_id"], want)
		}
	}
	if string(received[0]["http"]) != `{"method":"GET","status":200}` ||
		string(received[1]["tags"]) != `["catalog","search"]` {
		t.Fatal("nested input values changed")
	}
}
