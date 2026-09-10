package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func requireJournalPlatform(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd":
	default:
		t.Skip("durable journal is not supported on this platform")
	}
}

// This fixture imports only slogx. Its stdout crosses an OS pipe into a separate
// collector executable before any OTLP encoding or HTTP request takes place.
func TestApplicationPipeRoutesLogsAndThreeLayerTrace(t *testing.T) {
	requireJournalPlatform(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	dir := t.TempDir()
	application, binary := filepath.Join(dir, "application"), filepath.Join(dir, "collector")
	for _, build := range []struct{ output, source string }{{application, "./testdata/traceapp"}, {binary, "."}} {
		if output, err := exec.CommandContext(ctx, "go", "build", "-o", build.output, build.source).CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", build.source, err, output)
		}
	}
	var mu sync.Mutex
	var logs []map[string]any
	var spans []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/default/logs/_json":
			var records []map[string]any
			if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
				t.Error(err)
			}
			logs = append(logs, records...)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "status": []any{map[string]any{"successful": len(records), "failed": 0}}})
		case "/api/default/v1/traces":
			var request struct {
				Resources []struct {
					Resource struct {
						Attributes []any `json:"attributes"`
					} `json:"resource"`
					Scopes []struct {
						Spans []map[string]any `json:"spans"`
					} `json:"scopeSpans"`
				} `json:"resourceSpans"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			for _, resource := range request.Resources {
				if attribute(resource.Resource.Attributes, "service.name")["stringValue"] != "example-service" {
					t.Error("collector did not supply service resource")
				}
				for _, scope := range resource.Scopes {
					spans = append(spans, scope.Spans...)
				}
			}
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	collector := exec.CommandContext(ctx, binary,
		"-ready",
		"-destination", "openobserve", "-endpoint", server.URL+"/api/default/logs/_json",
		"-traces-endpoint", server.URL+"/api/default/v1/traces", "-resource", "service.name=example-service",
		"-journal-dir", filepath.Join(dir, "journal"), "-flush-interval", "1h")
	input, err := collector.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := collector.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var collectorErr, applicationErr bytes.Buffer
	collector.Stderr = &collectorErr
	if err := collector.Start(); err != nil {
		t.Fatal(err)
	}
	ready := bufio.NewReader(output)
	if line, err := ready.ReadString('\n'); err != nil || line != "ready\n" {
		_ = collector.Process.Kill()
		_ = collector.Wait()
		t.Fatalf("collector readiness = %q, error = %v; stderr: %s", line, err, &collectorErr)
	}
	producer := exec.CommandContext(ctx, application)
	producer.Stdout, producer.Stderr = input, &applicationErr
	producerErr := producer.Run()
	closeErr := input.Close()
	collectorOut, readErr := io.ReadAll(ready)
	collectorResult := collector.Wait()
	if producerErr != nil || closeErr != nil || readErr != nil || collectorResult != nil {
		t.Fatalf("application=%v pipe=%v output=%v collector=%v\napplication stderr: %s\ncollector stderr: %s", producerErr, closeErr, readErr, collectorResult, &applicationErr, &collectorErr)
	}
	if len(collectorOut) != 0 || collectorErr.Len() != 0 || applicationErr.Len() != 0 {
		t.Fatal("unexpected process output")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logs) != 4 || len(spans) != 3 {
		t.Fatalf("logs=%d spans=%d; want 4 logs and 3 spans", len(logs), len(spans))
	}
	byName := make(map[string]map[string]any)
	for _, span := range spans {
		byName[span["name"].(string)] = span
		attrs := span["attributes"].([]any)
		if attribute(attrs, "request_id")["stringValue"] != "example-request" {
			t.Error("span lost context attributes")
		}
		source := attribute(attrs, "source")["kvlistValue"].(map[string]any)["values"].([]any)
		if !strings.HasSuffix(attribute(source, "file")["stringValue"].(string), "traceapp/main.go") {
			t.Error("span lost caller source")
		}
		if len(span["events"].([]any)) != 1 {
			t.Error("span lost error event")
		}
	}
	root, middle, leaf := byName["handle request"], byName["load item"], byName["query item"]
	if root["traceId"] != middle["traceId"] || root["traceId"] != leaf["traceId"] || middle["parentSpanId"] != root["spanId"] || leaf["parentSpanId"] != middle["spanId"] {
		t.Fatal("collector lost three-layer trace relationships")
	}
	if attribute(leaf["attributes"].([]any), "item_id")["intValue"] != "9007199254740993" {
		t.Fatal("collector rounded a large integer")
	}
	event := root["events"].([]any)[0].(map[string]any)
	cause := attribute(event["attributes"].([]any), "err")
	for _, name := range []string{"handle request", "load item", "query item"} {
		fields := cause["kvlistValue"].(map[string]any)["values"].([]any)
		if attribute(fields, "msg")["stringValue"] != name {
			t.Fatalf("wrapped error lost layer %q", name)
		}
		cause = attribute(fields, "cause")
	}
	if cause["stringValue"] != "connection refused" {
		t.Fatal("wrapped error lost original cause")
	}
	for _, record := range logs {
		if _, found := record["span"]; found {
			t.Error("span was delivered as an ordinary log")
		}
	}
}

func attribute(attrs []any, name string) map[string]any {
	for _, attr := range attrs {
		entry := attr.(map[string]any)
		if entry["key"] == name {
			return entry["value"].(map[string]any)
		}
	}
	return nil
}
