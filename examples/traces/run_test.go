package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rah-0/slogx-collector/examples/internal/exampletest"
)

type OTLPAttribute struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type OTLPSpan struct {
	TraceID      string          `json:"traceId"`
	SpanID       string          `json:"spanId"`
	ParentSpanID string          `json:"parentSpanId"`
	Name         string          `json:"name"`
	Attributes   []OTLPAttribute `json:"attributes"`
	Events       []struct {
		Name       string          `json:"name"`
		Attributes []OTLPAttribute `json:"attributes"`
	} `json:"events"`
}

type OTLPRequest struct {
	ResourceSpans []struct {
		Resource struct {
			Attributes []OTLPAttribute `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []struct {
			Spans []OTLPSpan `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"resourceSpans"`
}

func TestScriptRoutesLogsAndConvertedSpans(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("the example script requires bash")
	}
	binary := exampletest.BuildCollector(t)
	var mu sync.Mutex
	var logs []LogRecord
	var spans []OTLPSpan
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		username, password, ok := request.BasicAuth()
		if !ok || username != "example" || password != "fixture-password" ||
			request.Header.Get("Stream-Name") != "application" || request.Method != http.MethodPost ||
			request.Header.Get("Content-Type") != "application/json" {
			t.Error("request lost authentication, routing header, method, or content type")
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/default/application/_json":
			var batch []LogRecord
			if err := json.NewDecoder(request.Body).Decode(&batch); err != nil {
				t.Error(err)
				http.Error(writer, "invalid log batch", http.StatusBadRequest)
				return
			}
			mu.Lock()
			logs = append(logs, batch...)
			mu.Unlock()
			fmt.Fprintf(writer, `{"code":200,"status":[{"successful":%d,"failed":0}]}`, len(batch))
		case "/api/default/v1/traces":
			var payload OTLPRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Error(err)
				http.Error(writer, "invalid trace batch", http.StatusBadRequest)
				return
			}
			for _, resource := range payload.ResourceSpans {
				if got := otlpAttribute(resource.Resource.Attributes, "service.name"); string(got) != `{"stringValue":"catalog"}` {
					t.Errorf("service.name = %s, want catalog", got)
				}
				for _, scope := range resource.ScopeSpans {
					mu.Lock()
					spans = append(spans, scope.Spans...)
					mu.Unlock()
				}
			}
			fmt.Fprint(writer, `{}`)
		default:
			t.Errorf("unexpected destination %s", request.URL.Path)
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	script, err := filepath.Abs("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", script,
		"-username", "example", "-password-env", "EXAMPLE_PASSWORD",
		"-max-retries", "1", "-retry-interval", "1ms", "-max-retry-interval", "1ms",
		"-request-timeout", "2s",
	)
	command.Dir = t.TempDir() // The script must locate its producer from another directory.
	command.Env = append(os.Environ(),
		"COLLECTOR_BIN="+binary,
		"JOURNAL_DIR="+t.TempDir(),
		"LOG_ENDPOINT="+server.URL+"/api/default/application/_json",
		"TRACE_ENDPOINT="+server.URL+"/api/default/v1/traces",
		"EXAMPLE_PASSWORD=fixture-password",
	)
	command.WaitDelay = 2 * time.Second
	if output, err := command.CombinedOutput(); err != nil || len(output) != 0 {
		t.Fatalf("script: %v\n%s", err, output)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(logs) != 4 || len(spans) != 3 {
		t.Fatalf("delivered %d logs and %d spans, want 4 and 3", len(logs), len(spans))
	}
	byName := make(map[string]OTLPSpan)
	for _, span := range spans {
		if _, duplicate := byName[span.Name]; duplicate {
			t.Fatalf("duplicate span %q", span.Name)
		}
		byName[span.Name] = span
		if len(span.Events) != 1 || span.Events[0].Name != "error" ||
			!bytes.Contains(otlpAttribute(span.Events[0].Attributes, "err"), []byte("connection refused")) {
			t.Fatalf("span %q lost its structured error event", span.Name)
		}
		if source := otlpAttribute(span.Attributes, "source"); !bytes.Contains(source, []byte(`"kvlistValue"`)) || !bytes.Contains(source, []byte("main.go")) {
			t.Fatalf("span %q lost its native source group: %s", span.Name, source)
		}
	}
	root := byName["handle request"]
	var parentID string
	for _, name := range []string{"handle request", "load item", "query item"} {
		span := byName[name]
		if len(span.TraceID) != 32 || span.TraceID != root.TraceID || len(span.SpanID) != 16 || span.ParentSpanID != parentID {
			t.Fatalf("span %q lost its identity or parent: %+v", name, span)
		}
		parentID = span.SpanID
	}
	for _, record := range logs {
		if record.Span != nil || record.TraceID != root.TraceID || len(record.SpanID) != 16 {
			t.Fatalf("ordinary log lost correlation or contains a completed span: %+v", record)
		}
		if record.Message == "request failed" && !strings.Contains(string(record.Error), "connection refused") {
			t.Fatal("ordinary error log lost its structured cause")
		}
	}
}

func otlpAttribute(attrs []OTLPAttribute, key string) json.RawMessage {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value
		}
	}
	return nil
}
