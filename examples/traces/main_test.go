package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type LogRecord struct {
	Level    string          `json:"level"`
	Message  string          `json:"msg"`
	TraceID  string          `json:"trace_id"`
	SpanID   string          `json:"span_id"`
	Source   *slog.Source    `json:"source"`
	Span     *SpanRecord     `json:"span"`
	HTTP     map[string]any  `json:"http"`
	CacheHit *bool           `json:"cache_hit"`
	ItemID   int             `json:"item_id"`
	Attempt  int             `json:"attempt"`
	Error    json.RawMessage `json:"err"`
}

type SpanRecord struct {
	TraceID      string               `json:"trace_id"`
	SpanID       string               `json:"span_id"`
	ParentSpanID string               `json:"parent_span_id"`
	Name         string               `json:"name"`
	Status       int                  `json:"status"`
	StartTime    time.Time            `json:"start_time"`
	EndTime      time.Time            `json:"end_time"`
	Events       map[string]SpanEvent `json:"events"`
}

type SpanEvent struct {
	Time       time.Time                  `json:"time"`
	Message    string                     `json:"msg"`
	Attributes map[string]json.RawMessage `json:"attrs"`
}

type ErrorLayer struct {
	Message    string          `json:"msg"`
	Attributes map[string]any  `json:"attrs"`
	Cause      json.RawMessage `json:"cause"`
}

func TestRun(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	var output bytes.Buffer
	run(&output)

	spans := make(map[string]LogRecord)
	logs := make(map[string]LogRecord)
	decoder := json.NewDecoder(&output)
	for {
		var record LogRecord
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if record.Span != nil {
			if _, exists := spans[record.Span.Name]; exists {
				t.Fatalf("duplicate span %q", record.Span.Name)
			}
			spans[record.Span.Name] = record
		} else {
			if _, exists := logs[record.Message]; exists {
				t.Fatalf("duplicate log %q", record.Message)
			}
			logs[record.Message] = record
		}
	}
	if len(spans) != 3 || len(logs) != 4 {
		t.Fatalf("got %d spans and %d logs, want 3 and 4", len(spans), len(logs))
	}
	layers := []ErrorLayer{
		{Message: "handle request", Attributes: map[string]any{"method": "GET"}},
		{Message: "load item", Attributes: map[string]any{"cache_hit": false}},
		{Message: "query item", Attributes: map[string]any{"item_id": float64(42), "attempt": float64(2)}},
	}
	var traceID, parentID string
	seen := make(map[string]bool)
	for index, layer := range layers {
		record, exists := spans[layer.Message]
		if !exists {
			t.Fatalf("missing span %q", layer.Message)
		}
		span := record.Span
		if index == 0 {
			traceID = span.TraceID
		}
		if len(traceID) != 32 || span.TraceID != traceID || len(span.SpanID) != 16 || seen[span.SpanID] {
			t.Fatalf("invalid or duplicated span identity: %+v", span)
		}
		seen[span.SpanID] = true
		if span.ParentSpanID != parentID || record.TraceID != traceID || record.SpanID != span.SpanID {
			t.Fatalf("span %q lost its parent or log correlation", span.Name)
		}
		parentID = span.SpanID
		if record.Level != "INFO" || span.Status != 2 || span.StartTime.IsZero() || span.EndTime.Before(span.StartTime) {
			t.Fatalf("unexpected completed operation: %+v", record)
		}
		function := []string{"handleRequest", "loadItem", "queryItem"}[index]
		if record.Source == nil || !strings.HasSuffix(record.Source.Function, "."+function) ||
			filepath.Base(record.Source.File) != "main.go" || record.Source.Line <= 0 {
			t.Fatalf("source for %q = %+v", span.Name, record.Source)
		}
		event := span.Events["0"]
		if len(span.Events) != 1 || event.Message != "error" || event.Time.IsZero() {
			t.Fatalf("error events = %+v", span.Events)
		}
		checkErrorLayers(t, event.Attributes["err"], layers[index:])
	}
	if !reflect.DeepEqual(spans["handle request"].HTTP, map[string]any{"method": "GET", "route": "/items/{id}"}) {
		t.Fatal("root span lost its grouped HTTP attributes")
	}
	if hit := spans["load item"].CacheHit; hit == nil || *hit {
		t.Fatal("middle span lost cache_hit=false")
	}
	if record := spans["query item"]; record.ItemID != 42 || record.Attempt != 2 {
		t.Fatal("inner span lost its query attributes")
	}
	for message, operation := range map[string]string{
		"request started": "handle request", "request failed": "handle request",
		"cache miss": "load item", "query started": "query item",
	} {
		record := logs[message]
		if record.TraceID != traceID || record.SpanID != spans[operation].Span.SpanID {
			t.Fatalf("log %q lost correlation with %q", message, operation)
		}
	}
	if logs["request failed"].Level != "ERROR" {
		t.Fatal("failure was not logged at ERROR")
	}
	checkErrorLayers(t, logs["request failed"].Error, layers)
}

func checkErrorLayers(t *testing.T, raw json.RawMessage, expected []ErrorLayer) {
	t.Helper()
	for _, want := range expected {
		var layer ErrorLayer
		if err := json.Unmarshal(raw, &layer); err != nil {
			t.Fatal(err)
		}
		if layer.Message != want.Message || !reflect.DeepEqual(layer.Attributes, want.Attributes) {
			t.Fatalf("error layer = %+v, want %+v", layer, want)
		}
		raw = layer.Cause
	}
	var cause string
	if err := json.Unmarshal(raw, &cause); err != nil || cause != "connection refused" {
		t.Fatalf("underlying cause = %s, error %v", raw, err)
	}
}
