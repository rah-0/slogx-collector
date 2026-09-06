package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/collector/otlp"
	"github.com/rah-0/slogx-collector/internal/jsonx"
)

type destinationFunc func(context.Context, []json.RawMessage) error

func (send destinationFunc) Send(ctx context.Context, records []json.RawMessage) error {
	return send(ctx, records)
}

func TestRoutingSharesCredentialsAndKeepsAdapterOptionsSeparate(t *testing.T) {
	t.Setenv("ROUTING_TEST_PASSWORD", "fixture-password")
	t.Setenv("ROUTING_TEST_HEADER", "fixture-header")
	requests := make(chan struct {
		path string
		body []byte
	}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "fixture-user" || password != "fixture-password" || r.Header.Get("X-Token") != "fixture-header" {
			t.Error("destination lost shared authentication or header")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			http.Error(w, "cannot read request", http.StatusBadRequest)
			return
		}
		requests <- struct {
			path string
			body []byte
		}{r.URL.Path, body}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/logs" {
			_, _ = io.WriteString(w, `{"code":200,"status":[{"successful":1,"failed":0}]}`)
		} else {
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer server.Close()
	cfg := commandConfig{
		destination: "openobserve", endpoint: server.URL + "/logs", tracesEndpoint: server.URL + "/traces",
		username: "fixture-user", passwordEnv: "ROUTING_TEST_PASSWORD", headerEnv: repeatedFlag{"X-Token=ROUTING_TEST_HEADER"},
		timestampField: "event_time", timestampLayout: "2006/01/02 15:04:05",
		resource: map[string]any{"service.name": "catalog"},
	}
	destination, err := cfg.newDestination()
	if err != nil {
		t.Fatal(err)
	}
	log := json.RawMessage(`{"msg":"request completed","event_time":"2026/09/06 12:00:00"}`)
	span := json.RawMessage(`{"msg":"operation","event_time":"not a log timestamp","span":{"trace_id":"11111111111111111111111111111111","span_id":"2222222222222222","name":"operation","kind":1,"start_time":"2026-09-06T12:00:01Z","end_time":"2026-09-06T12:00:02Z","status":0}}`)
	if err := destination.Send(t.Context(), []json.RawMessage{log, span}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want one per destination", len(requests))
	}
	traces, logs := <-requests, <-requests
	if traces.path != "/traces" || logs.path != "/logs" {
		t.Fatalf("destination order = %q, %q", traces.path, logs.path)
	}
	var records []map[string]json.RawMessage
	if err := json.Unmarshal(logs.body, &records); err != nil {
		t.Fatal(err)
	}
	wantTime := time.Date(2026, time.September, 6, 12, 0, 0, 0, time.UTC)
	if len(records) != 1 || string(records[0]["event_time"]) != `"2026/09/06 12:00:00"` ||
		string(records[0]["_timestamp"]) != strconv.FormatInt(wantTime.UnixMicro(), 10) {
		t.Fatalf("log timestamp mapping = %s", logs.body)
	}
	var envelope otlp.ExportRequest
	if err := json.Unmarshal(traces.body, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.ResourceSpans) != 1 {
		t.Fatalf("trace resources = %s", traces.body)
	}
	resource := envelope.ResourceSpans[0]
	if len(resource.Resource.Attributes) != 1 || resource.Resource.Attributes[0].Key != "service.name" ||
		resource.Resource.Attributes[0].Value.StringValue == nil || *resource.Resource.Attributes[0].Value.StringValue != "catalog" {
		t.Fatalf("trace resource attributes = %s", traces.body)
	}
	if len(resource.ScopeSpans) != 1 || len(resource.ScopeSpans[0].Spans) != 1 {
		t.Fatalf("trace spans = %s", traces.body)
	}
	converted := resource.ScopeSpans[0].Spans[0]
	if converted.StartTimeUnixNano != strconv.FormatInt(wantTime.Add(time.Second).UnixNano(), 10) ||
		converted.EndTimeUnixNano != strconv.FormatInt(wantTime.Add(2*time.Second).UnixNano(), 10) {
		t.Fatalf("trace timestamps = %s", traces.body)
	}
	for _, attr := range converted.Attributes {
		if attr.Key == "_timestamp" {
			t.Fatal("log timestamp mapping was applied to the trace")
		}
	}
}

func TestRoutingWaitsForBothDestinations(t *testing.T) {
	failure := errors.New("destination unavailable")
	log := json.RawMessage(`{"msg":"started","n":9007199254740993}`)
	span := json.RawMessage(`{"span":{"name":"operation"}}`)
	var calls []string
	tracesFail, logsFail := true, true
	router := routingDestination{
		traces: destinationFunc(func(_ context.Context, records []json.RawMessage) error {
			calls = append(calls, "traces")
			if len(records) != 1 || string(records[0]) != string(span) {
				t.Fatal("trace partition changed bytes")
			}
			if tracesFail {
				return failure
			}
			return nil
		}),
		logs: destinationFunc(func(_ context.Context, records []json.RawMessage) error {
			calls = append(calls, "logs")
			if len(records) != 1 || string(records[0]) != string(log) {
				t.Fatal("log partition changed bytes")
			}
			if logsFail {
				return failure
			}
			return nil
		}),
	}
	batch := []json.RawMessage{log, span}
	if err := router.Send(t.Context(), batch); !errors.Is(err, failure) || len(calls) != 1 {
		t.Fatalf("trace failure = %v; calls=%v", err, calls)
	}
	tracesFail = false
	if err := router.Send(t.Context(), batch); !errors.Is(err, failure) || len(calls) != 3 {
		t.Fatalf("log failure = %v; calls=%v", err, calls)
	}
	logsFail = false
	if err := router.Send(t.Context(), batch); err != nil || len(calls) != 5 {
		t.Fatalf("success = %v; calls=%v", err, calls)
	}
	if err := router.Send(t.Context(), nil); err != nil || len(calls) != 5 {
		t.Fatalf("empty batch = %v", err)
	}
	if err := router.Send(t.Context(), []json.RawMessage{log, json.RawMessage(`null`)}); !errors.Is(err, jsonx.ErrInvalidRecord) || len(calls) != 5 {
		t.Fatalf("invalid batch = %v; calls=%v", err, calls)
	}
}

func TestMixedBatchReplaysAfterOneDestinationAccepted(t *testing.T) {
	requireJournalPlatform(t)
	var traceAttempts, logAttempts int
	acceptLogs := false
	failure := errors.New("logs unavailable")
	log := json.RawMessage(`{"msg":"pending"}`)
	envelope := json.RawMessage(traceEnvelope)
	router := routingDestination{
		traces: destinationFunc(func(_ context.Context, records []json.RawMessage) error {
			traceAttempts++
			if len(records) != 1 || !bytes.Equal(records[0], envelope) {
				t.Error("trace partition changed during replay")
			}
			return nil
		}),
		logs: destinationFunc(func(_ context.Context, records []json.RawMessage) error {
			logAttempts++
			if len(records) != 1 || !bytes.Equal(records[0], log) {
				t.Error("log partition changed during replay")
			}
			if !acceptLogs {
				return failure
			}
			return nil
		}),
	}
	options := collector.Options{JournalDir: t.TempDir(), JournalKey: "mixed-test", BatchSize: 100, FlushInterval: time.Hour}
	input := string(log) + "\n" + string(envelope) + "\n"
	if err := collector.Run(t.Context(), io.NopCloser(strings.NewReader(input)), router, options); !errors.Is(err, failure) {
		t.Fatalf("failed batch = %v", err)
	}
	if traceAttempts != 1 || logAttempts != 1 {
		t.Fatalf("initial trace/log attempts = %d/%d", traceAttempts, logAttempts)
	}
	acceptLogs = true
	for range 2 {
		if err := collector.Run(t.Context(), io.NopCloser(strings.NewReader("")), router, options); err != nil {
			t.Fatal(err)
		}
	}
	if traceAttempts != 2 || logAttempts != 2 {
		t.Fatalf("replayed trace/log attempts = %d/%d; want 2/2", traceAttempts, logAttempts)
	}
}
