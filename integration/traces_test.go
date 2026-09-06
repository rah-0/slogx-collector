package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rah-0/slogx"
)

const traceService = "collector-trace-integration"

func testCollectorTraces(t *testing.T, server openObserveServer, binary string) {
	t.Helper()
	t.Run("three layers reach trace search", func(t *testing.T) {
		input, spans := threeLayerTrace(t)
		stream := "collector_traces"
		if stderr, err := runTraceCollector(t, binary, server, server.url+"/api/default/v1/traces", stream, t.TempDir(), input); err != nil {
			t.Fatalf("collect trace input: %v\n%s", err, stderr)
		}
		assertStoredTrace(t, server, stream, spans)
		assertStoredTraceLogs(t, server, stream+"_logs", spans)
	})

	t.Run("restart replays traces after an outage", func(t *testing.T) {
		target, err := url.Parse(server.url)
		if err != nil {
			t.Fatal(err)
		}
		forward := httputil.NewSingleHostReverseProxy(target)
		forward.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
			w.WriteHeader(http.StatusBadGateway)
		}
		var healthy atomic.Bool
		var failed, forwarded atomic.Int32
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !healthy.Load() {
				failed.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"message":"temporarily unavailable"}`)
				return
			}
			forwarded.Add(1)
			forward.ServeHTTP(w, r)
		}))
		t.Cleanup(proxy.Close)

		input, spans := threeLayerTrace(t)
		endpoint := proxy.URL + "/api/default/v1/traces"
		stream := "collector_trace_replay"
		journalDir := t.TempDir()
		stderr, err := runTraceCollector(t, binary, server, endpoint, stream, journalDir, input)
		if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.Contains(stderr, "503") {
			t.Fatalf("unavailable destination = %v; want exit 1 and HTTP 503\n%s", err, stderr)
		}
		if failed.Load() == 0 || forwarded.Load() != 0 {
			t.Fatal("outage did not prevent delivery to the backend")
		}

		// Recovery keeps the destination URL, stream, and journal unchanged.
		// Empty stdin ensures all delivered spans came from the saved backlog.
		healthy.Store(true)
		if stderr, err := runTraceCollector(t, binary, server, endpoint, stream, journalDir, nil); err != nil {
			t.Fatalf("replay trace journal: %v\n%s", err, stderr)
		}
		assertStoredTrace(t, server, stream, spans)
		assertStoredTraceLogs(t, server, stream+"_logs", spans)
		acceptedRequests := forwarded.Load()
		if acceptedRequests == 0 {
			t.Fatal("recovery did not forward the saved backlog")
		}
		if stderr, err := runTraceCollector(t, binary, server, endpoint, stream, journalDir, nil); err != nil {
			t.Fatalf("restart acknowledged journal: %v\n%s", err, stderr)
		}
		if forwarded.Load() != acceptedRequests {
			t.Fatal("another restart resent acknowledged spans")
		}
	})
}

func runTraceCollector(t *testing.T, binary string, server openObserveServer, endpoint, stream, journalDir string, input []byte) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary,
		"-destination", "openobserve",
		"-endpoint", server.url+"/api/default/"+stream+"_logs/_json",
		"-traces-endpoint", endpoint,
		"-resource", "service.name="+traceService,
		"-header", "stream-name="+stream,
		"-journal-dir", journalDir,
		"-username", server.username,
		"-password-env", "SLOGX_COLLECT_INTEGRATION_PASSWORD",
		"-batch-size", "3",
		"-flush-interval", "1h",
		"-request-timeout", "5s",
		"-retry-interval", "1ms",
		"-max-retry-interval", "1ms",
		"-max-retries", "1",
	)
	command.Env = append(os.Environ(), "SLOGX_COLLECT_INTEGRATION_PASSWORD="+server.password)
	command.Stdin = bytes.NewReader(input)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if strings.Contains(stdout.String(), server.password) || strings.Contains(stderr.String(), server.password) {
		t.Fatal("collector output exposed credentials")
	}
	if stdout.Len() != 0 {
		t.Fatal("collector wrote output to stdout")
	}
	return stderr.String(), err
}

func threeLayerTrace(t *testing.T) ([]byte, [3]slogx.SpanContext) {
	t.Helper()
	var output bytes.Buffer
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	slogx.SetDefault(slogx.Options{Format: slogx.JSON, Writer: &output, AddSource: true})
	ctx := slogx.WithAttrs(t.Context(), slog.String("request_id", "integration-request"))
	var spans [3]slogx.SpanContext
	queryItem := func(ctx context.Context) error {
		ctx, span := slogx.StartSpan(ctx, "query item", slog.Int("item_id", 42), slog.Int("attempt", 2))
		defer span.End()
		span.SetKind(slogx.SpanKindClient)
		spans[2] = span.SpanContext()
		slog.InfoContext(ctx, "query started")
		err := slogx.Wrap(errors.New("connection refused"), "query item", slog.Group("db", "item_id", 42, "attempt", 2))
		span.RecordError(err, slog.Group("retry", "scheduled", false))
		span.SetStatus(slogx.StatusError, "query failed")
		return err
	}
	loadItem := func(ctx context.Context) error {
		ctx, span := slogx.StartSpan(ctx, "load item", slog.Bool("cache_hit", false))
		defer span.End()
		spans[1] = span.SpanContext()
		slog.InfoContext(ctx, "load started")
		err := slogx.Wrap(queryItem(ctx), "load item", "cache_hit", false)
		span.RecordError(err)
		span.SetStatus(slogx.StatusError, "load failed")
		return err
	}
	handleRequest := func(ctx context.Context) error {
		ctx, span := slogx.StartSpan(ctx, "handle request", slog.Group("http", "method", "GET", "route", "/items/{id}"))
		defer span.End()
		span.SetKind(slogx.SpanKindServer)
		spans[0] = span.SpanContext()
		slog.InfoContext(ctx, "request started")
		err := slogx.Wrap(loadItem(ctx), "handle request", slog.Group("http", "method", "GET"))
		span.RecordError(err)
		span.SetStatus(slogx.StatusError, "request failed")
		slogx.ErrorContext(ctx, "request failed", err)
		return err
	}
	if err := handleRequest(ctx); err == nil {
		t.Fatal("sample operation should fail")
	}
	if t.Failed() {
		t.FailNow()
	}
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	var logged []slogx.SpanContext
	for {
		var record struct {
			TraceID string          `json:"trace_id"`
			SpanID  string          `json:"span_id"`
			Span    json.RawMessage `json:"span"`
		}
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if record.Span == nil {
			logged = append(logged, slogx.SpanContext{TraceID: record.TraceID, SpanID: record.SpanID})
		}
	}
	want := []slogx.SpanContext{spans[0], spans[1], spans[2], spans[0]}
	if len(logged) != len(want) {
		t.Fatalf("logged %d records, want %d", len(logged), len(want))
	}
	for i, record := range logged {
		if record.TraceID != want[i].TraceID || record.SpanID != want[i].SpanID {
			t.Fatalf("log identity = %+v, want %+v", record, want[i])
		}
	}
	return output.Bytes(), spans
}

func assertStoredTraceLogs(t *testing.T, server openObserveServer, stream string, spans [3]slogx.SpanContext) {
	t.Helper()
	hits := server.search(t, "logs", stream, spans[0].TraceID, 4)
	want := map[string]string{
		"request started": spans[0].SpanID,
		"load started":    spans[1].SpanID,
		"query started":   spans[2].SpanID,
		"request failed":  spans[0].SpanID,
	}
	for _, hit := range hits {
		var record struct {
			Message string `json:"msg"`
			SpanID  string `json:"span_id"`
		}
		if err := json.Unmarshal(hit, &record); err != nil {
			t.Fatal(err)
		}
		if expected, ok := want[record.Message]; !ok || expected != record.SpanID {
			t.Fatalf("stored log lost correlation: %+v", record)
		}
		delete(want, record.Message)
	}
}

func assertStoredTrace(t *testing.T, server openObserveServer, stream string, spans [3]slogx.SpanContext) {
	t.Helper()
	hits := server.search(t, "traces", stream, spans[0].TraceID, len(spans))
	byID := make(map[string]map[string]any, len(hits))
	for _, raw := range hits {
		var hit map[string]any
		if err := json.Unmarshal(raw, &hit); err != nil {
			t.Fatal(err)
		}
		id, _ := hit["span_id"].(string)
		if _, exists := byID[id]; exists {
			t.Fatalf("duplicate stored span %q", id)
		}
		byID[id] = hit
	}
	for i, sc := range spans {
		hit := byID[sc.SpanID]
		if !sc.IsValid() || hit == nil || sc.TraceID != spans[0].TraceID || hit["trace_id"] != sc.TraceID || hit["service_name"] != traceService {
			t.Fatalf("span %d identity or service mismatch: %+v", i, hit)
		}
		var parentID string
		if i > 0 {
			parentID = spans[i-1].SpanID
		}
		if actual, _ := hit["reference_parent_span_id"].(string); actual != parentID {
			t.Fatalf("span %d parent = %q, want %q", i, actual, parentID)
		}
		if hit["span_status"] != "ERROR" {
			t.Fatalf("span %d status = %v, want ERROR", i, hit["span_status"])
		}
	}
	if root := byID[spans[0].SpanID]; root["http_method"] != "GET" || root["http_route"] != "/items/{id}" {
		t.Fatalf("root span lost grouped attributes: %+v", root)
	}
	if middle := byID[spans[1].SpanID]; fmt.Sprint(middle["cache_hit"]) != "false" {
		t.Fatalf("middle span lost its attributes: %+v", middle)
	}
	if leaf := byID[spans[2].SpanID]; fmt.Sprint(leaf["item_id"]) != "42" || fmt.Sprint(leaf["attempt"]) != "2" {
		t.Fatalf("leaf span lost its attributes: %+v", leaf)
	}

	var events []struct {
		Name string          `json:"name"`
		Err  json.RawMessage `json:"err"`
	}
	storedEvents, _ := byID[spans[0].SpanID]["events"].(string)
	if err := json.Unmarshal([]byte(storedEvents), &events); err != nil || len(events) != 1 || events[0].Name != "error" {
		t.Fatalf("stored error events = %q, %v", storedEvents, err)
	}
	layer := events[0].Err
	// The pinned OpenObserve v0.92.2 image stores these boolean and integer
	// event attributes as strings.
	for _, expected := range []struct{ message, attrs string }{
		{"handle request", `{"http":{"method":"GET"}}`},
		{"load item", `{"cache_hit":"false"}`},
		{"query item", `{"db":{"attempt":"2","item_id":"42"}}`},
	} {
		var wrapped struct {
			Message string          `json:"msg"`
			Attrs   map[string]any  `json:"attrs"`
			Cause   json.RawMessage `json:"cause"`
		}
		if err := json.Unmarshal(layer, &wrapped); err != nil {
			t.Fatalf("decode stored error layer: %v", err)
		}
		attrs, err := json.Marshal(wrapped.Attrs)
		if err != nil || wrapped.Message != expected.message || string(attrs) != expected.attrs {
			t.Fatalf("stored error layer = %s; want message %q and attributes %s", layer, expected.message, expected.attrs)
		}
		layer = wrapped.Cause
	}
	if string(layer) != `"connection refused"` {
		t.Fatalf("stored root cause = %s", layer)
	}
}
