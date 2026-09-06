package otlp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rah-0/slogx"
)

func TestDestinationConvertsNativeThreeLayerTrace(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	slog.SetDefault(slogx.New(slogx.Options{
		Format: slogx.JSON, AddSource: true, Writer: &output,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == "secret" {
				return slog.Attr{}
			}
			return attr
		},
	}))
	ctx := slogx.WithAttrs(t.Context(), slog.String("request_id", "request-1"))
	profile := struct {
		Name   string `json:"name"`
		Secret string `json:"-"`
	}{Name: "public", Secret: "synthetic-private"}
	pc, file, line, ok := runtime.Caller(0)
	ctx, handler := slogx.StartSpan(ctx, "handler", slog.Any("profile", profile), slog.String("secret", "synthetic-private"))
	if !ok {
		t.Fatal("caller source unavailable")
	}
	ctx, service := slogx.StartSpan(ctx, "service", slog.String("layer", "business"))
	_, database := slogx.StartSpan(ctx, "database", slog.String("layer", "storage"))
	failure := slogx.Wrap(errors.New("connection refused"), "query failed", slog.Int("attempt", 2))
	failure = slogx.Wrap(failure, "checkout failed", slog.Group("order", slog.Int("id", 42)))
	failure = slogx.Wrap(failure, "request failed", slog.String("method", "POST"))
	handler.RecordError(failure, slog.String("secret", "synthetic-private"))
	handler.SetStatus(slogx.StatusError, "request failed")
	slog.InfoContext(ctx, "ordinary log", "secret", "synthetic-private")
	for _, span := range []*slogx.Span{database, service, handler} {
		span.End()
	}
	records := jsonLines(t, output.Bytes())
	if len(records) != 4 || bytes.Contains(output.Bytes(), []byte("resourceSpans")) {
		t.Fatalf("application did not produce four native JSON records: %s", output.Bytes())
	}
	resource := map[string]any{"service.name": "example-service", "deployment": map[string]any{"environment": "test"}}
	var payload []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ = io.ReadAll(r.Body)
		respond(w, `{}`)
	}))
	defer server.Close()
	destination := newDestination(t, server, Config{Resource: resource})
	resource["service.name"] = "changed"
	resource["deployment"].(map[string]any)["environment"] = "changed"
	if err := destination.Send(t.Context(), records); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("synthetic-private")) || bytes.Contains(payload, []byte("changed")) {
		t.Fatal("redacted data or subsequent resource mutations reached delivery")
	}
	var envelope ExportRequest
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.ResourceSpans) != 1 || *envelope.ResourceSpans[0].Resource.Attributes[1].Value.StringValue != "example-service" {
		t.Fatalf("resource attributes = %s", payload)
	}
	spans := writtenSpans(t, payload)
	if len(spans) != 3 {
		t.Fatalf("spans = %d; want 3 and ordinary log skipped", len(spans))
	}
	parents := []string{service.SpanContext().SpanID, handler.SpanContext().SpanID, ""}
	for i, name := range []string{"database", "service", "handler"} {
		if spans[i]["name"] != name || spans[i]["traceId"] != handler.SpanContext().TraceID || optionalString(spans[i]["parentSpanId"]) != parents[i] {
			t.Fatalf("span %d lost trace hierarchy: %v", i, spans[i])
		}
		attrs := spans[i]["attributes"].([]any)
		if attributeValue(attrs, "request_id")["stringValue"] != "request-1" || attributeValue(attrs, slog.LevelKey)["stringValue"] != "INFO" {
			t.Fatalf("span %d lost native/context attributes: %v", i, attrs)
		}
	}
	attrs := spans[2]["attributes"].([]any)
	source := attributeValue(attrs, slog.SourceKey)["kvlistValue"].(map[string]any)["values"].([]any)
	if attributeValue(source, "function")["stringValue"] != runtime.FuncForPC(pc).Name() ||
		attributeValue(source, "file")["stringValue"] != file || attributeValue(source, "line")["intValue"] != strconv.Itoa(line+1) {
		t.Fatalf("source changed: %v", source)
	}
	fields := attributeValue(attrs, "profile")["kvlistValue"].(map[string]any)["values"].([]any)
	if len(fields) != 1 || attributeValue(fields, "name")["stringValue"] != profile.Name {
		t.Fatalf("profile fields = %v", fields)
	}
	event := spans[2]["events"].([]any)[0].(map[string]any)
	if event["name"] != "error" {
		t.Fatalf("error event = %v", event)
	}
	errValue := attributeValue(event["attributes"].([]any), "err")
	for _, message := range []string{"request failed", "checkout failed", "query failed"} {
		fields := errValue["kvlistValue"].(map[string]any)["values"].([]any)
		if attributeValue(fields, "msg")["stringValue"] != message || attributeValue(fields, "attrs")["kvlistValue"] == nil {
			t.Fatalf("error lost message or attributes at %q: %v", message, fields)
		}
		attrs := attributeValue(fields, "attrs")["kvlistValue"].(map[string]any)["values"].([]any)
		switch message {
		case "request failed":
			if attributeValue(attrs, "method")["stringValue"] != "POST" {
				t.Fatal("request error attribute changed")
			}
		case "checkout failed":
			order := attributeValue(attrs, "order")["kvlistValue"].(map[string]any)["values"].([]any)
			if attributeValue(order, "id")["intValue"] != "42" {
				t.Fatal("nested order group changed")
			}
		case "query failed":
			if attributeValue(attrs, "attempt")["intValue"] != "2" {
				t.Fatal("query error attribute changed")
			}
		}
		errValue = attributeValue(fields, "cause")
	}
	if errValue["stringValue"] != "connection refused" {
		t.Fatalf("root cause = %v", errValue)
	}
}

func TestDestinationRejectsInvalidSpanRecords(t *testing.T) {
	for _, test := range []struct {
		Name  string
		Key   string
		Value any
	}{
		{"trace length", "trace_id", "1234"},
		{"trace hex", "trace_id", strings.Repeat("x", 32)},
		{"zero trace", "trace_id", strings.Repeat("0", 32)},
		{"span id", "span_id", strings.Repeat("0", 16)},
		{"parent id", "parent_span_id", "invalid"},
		{"name", "name", ""}, {"kind", "kind", 6}, {"negative kind", "kind", -1},
		{"status", "status", 3}, {"negative status", "status", -1},
		{"flags", "trace_flags", 256}, {"negative flags", "trace_flags", -1},
		{"start", "start_time", "invalid"}, {"end", "end_time", "1969-12-31T23:59:59Z"},
		{"end before start", "end_time", "2026-09-06T00:00:00Z"},
		{"timestamp overflow", "end_time", "9999-01-01T00:00:00Z"},
		{"event index", "events", map[string]any{"invalid": map[string]any{"time": "2026-09-06T12:00:00Z", "msg": "event"}}},
		{"negative event index", "events", map[string]any{"-1": map[string]any{}}},
		{"duplicate numeric index", "events", map[string]any{"00": map[string]any{}}},
		{"event time", "events", map[string]any{"0": map[string]any{"msg": "event"}}},
		{"event name", "events", map[string]any{"0": map[string]any{"time": "2026-09-06T12:00:00Z"}}},
		{"events type", "events", []any{}},
	} {
		t.Run(test.Name, func(t *testing.T) {
			metadata := sampleMetadata()
			metadata[test.Key] = test.Value
			record, err := json.Marshal(map[string]any{"span": metadata})
			if err != nil {
				t.Fatal(err)
			}
			destination, err := New(Config{Endpoint: "http://127.0.0.1:1/v1/traces"})
			if err != nil {
				t.Fatal(err)
			}
			if err := destination.Send(t.Context(), []json.RawMessage{record}); !errors.Is(err, ErrInvalidSpan) {
				t.Fatalf("invalid span = %v", err)
			}
		})
	}
}

func TestDestinationPreservesJSONValuesAndEventOrder(t *testing.T) {
	var nested any = "leaf"
	for range 128 {
		nested = map[string]any{"group": []any{nested}}
	}
	values := map[string]any{
		"null": nil, "bool": false, "string": "text", "empty_object": map[string]any{}, "empty_array": []any{},
		"deep": nested, "integer": json.Number("9007199254740993"), "fraction": json.Number("1.2300e-99"),
		"large_integer": json.Number("9223372036854775808.0"), "underflow": json.Number("1e-400"),
	}
	metadata := sampleMetadata()
	events := make(map[string]any)
	for _, i := range []int{10, 2, 0} {
		events[strconv.Itoa(i)] = map[string]any{"time": "2026-09-06T12:00:00Z", "msg": fmt.Sprint(i), "attrs": values}
	}
	metadata["events"] = events
	input := map[string]any{"span": metadata, "values": values}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	payload, present, err := mergeRequests([]json.RawMessage{raw}, nil)
	if err != nil || !present {
		t.Fatalf("convert = %v, %t", err, present)
	}
	var envelope ExportRequest
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	span := envelope.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if got := []string{span.Events[0].Name, span.Events[1].Name, span.Events[2].Name}; !reflect.DeepEqual(got, []string{"0", "2", "10"}) {
		t.Fatalf("event order = %v", got)
	}
	attrs := span.Attributes[0].Value.KVListValue.Values
	if len(attrs) != len(values) {
		t.Fatalf("encoded %d values, want %d", len(attrs), len(values))
	}
	for _, event := range span.Events {
		if !reflect.DeepEqual(attrs, event.Attributes) {
			t.Fatal("span and event values differ")
		}
	}
	for _, attr := range attrs {
		switch attr.Key {
		case "null":
			if !reflect.DeepEqual(attr.Value, AnyValue{}) {
				t.Fatal("null value changed")
			}
		case "bool":
			if attr.Value.BoolValue == nil || *attr.Value.BoolValue {
				t.Fatal("false value changed")
			}
		case "string":
			if *attr.Value.StringValue != "text" {
				t.Fatal("string value changed")
			}
		case "empty_object":
			if attr.Value.KVListValue == nil || len(attr.Value.KVListValue.Values) != 0 {
				t.Fatal("empty object changed")
			}
		case "empty_array":
			if attr.Value.ArrayValue == nil || len(attr.Value.ArrayValue.Values) != 0 {
				t.Fatal("empty array changed")
			}
		case "integer":
			if *attr.Value.IntValue != "9007199254740993" {
				t.Fatal("integer precision lost")
			}
		case "fraction":
			if string(attr.Value.DoubleValue) != "1.2300e-99" {
				t.Fatal("fraction JSON changed")
			}
		case "large_integer":
			if *attr.Value.StringValue != "9223372036854775808.0" {
				t.Fatal("large integer lost")
			}
		case "underflow":
			if *attr.Value.StringValue != "1e-400" {
				t.Fatal("underflow rounded to zero")
			}
		case "deep":
			value := attr.Value
			for range 128 {
				value = value.KVListValue.Values[0].Value.ArrayValue.Values[0]
			}
			if *value.StringValue != "leaf" {
				t.Fatal("deep value lost")
			}
		}
	}
}

func TestDestinationUnsignedTimestampAndInvalidResource(t *testing.T) {
	metadata := sampleMetadata()
	instant := time.Date(2500, 1, 1, 0, 0, 0, 123, time.UTC)
	metadata["start_time"], metadata["end_time"] = instant, instant
	raw, err := json.Marshal(map[string]any{"span": metadata})
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := mergeRequests([]json.RawMessage{raw}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.FormatUint(uint64(instant.Unix())*1_000_000_000+123, 10)
	if writtenSpans(t, payload)[0]["startTimeUnixNano"] != want {
		t.Fatalf("unsigned timestamp changed: %s", payload)
	}
	for _, resource := range []map[string]any{{"value": make(chan int)}, {"value": json.Number("invalid")}} {
		if _, err := New(Config{Endpoint: "http://example.com/v1/traces", Resource: resource}); !errors.Is(err, ErrInvalidAttributes) {
			t.Fatalf("invalid resource = %v", err)
		}
	}
}

func sampleMetadata() map[string]any {
	return map[string]any{
		"trace_id": "1234567890abcdef1234567890abcdef", "span_id": "1234567890abcdef",
		"trace_flags": 1, "name": "operation", "kind": 1, "status": 0,
		"start_time": "2026-09-06T12:00:00.000000123Z", "end_time": "2026-09-06T12:00:00.000000456Z",
	}
}

func jsonLines(t *testing.T, output []byte) []json.RawMessage {
	t.Helper()
	var records []json.RawMessage
	for line := range bytes.SplitSeq(bytes.TrimSpace(output), []byte{'\n'}) {
		if !json.Valid(line) {
			t.Fatalf("invalid JSON record: %s", line)
		}
		records = append(records, bytes.Clone(line))
	}
	return records
}

func writtenSpans(t *testing.T, output []byte) []map[string]any {
	t.Helper()
	var spans []map[string]any
	reader := bufio.NewScanner(bytes.NewReader(output))
	for reader.Scan() {
		var request struct {
			ResourceSpans []struct {
				ScopeSpans []struct {
					Spans []map[string]any `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		if err := json.Unmarshal(reader.Bytes(), &request); err != nil {
			t.Fatal(err)
		}
		for _, resource := range request.ResourceSpans {
			for _, scope := range resource.ScopeSpans {
				spans = append(spans, scope.Spans...)
			}
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	return spans
}

func attributeValue(attrs []any, key string) map[string]any {
	for _, attr := range attrs {
		entry := attr.(map[string]any)
		if entry["key"] == key {
			return entry["value"].(map[string]any)
		}
	}
	return nil
}

func optionalString(value any) string {
	text, _ := value.(string)
	return text
}
