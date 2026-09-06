package openobserve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/httpx"
)

func TestSendPreservesRecordsAndCopiesHeaders(t *testing.T) {
	records := []json.RawMessage{
		json.RawMessage(` {"time":"unchanged","n":9007199254740993,"n":2,"nested":{"z":1,"a":2}} `),
		json.RawMessage(`{"message":"hello"}`),
	}
	want := "[" + string(records[0]) + "," + string(records[1]) + "]"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/org/stream/_json" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request: %s %s, type %q", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != "user" || password != "secret" || r.Header.Get("X-Example") != "original" {
			t.Error("authentication or copied headers differ")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != want {
			t.Errorf("body = %s, err = %v; want %s", body, err, want)
		}
		fmt.Fprint(w, `{"code":200,"status":[{"name":"stream","successful":2,"failed":0}]}`)
	}))
	defer server.Close()
	headers := http.Header{"X-Example": {"original"}}
	destination, err := New(Config{Endpoint: server.URL + "/api/org/stream/_json", Username: "user", Password: "secret", Headers: headers})
	if err != nil {
		t.Fatal(err)
	}
	headers.Set("X-Example", "changed")
	if err := destination.Send(t.Context(), records); err != nil {
		t.Fatal(err)
	}
	if "["+string(records[0])+","+string(records[1])+"]" != want {
		t.Fatal("Send mutated input records")
	}
}

func TestTimestampMapping(t *testing.T) {
	wantTime := time.Date(2026, 9, 5, 12, 34, 56, 123456000, time.UTC).UnixMicro()
	for _, test := range []struct {
		name      string
		record    string
		field     string
		layout    string
		want      string
		wantError bool
	}{
		{name: "disabled", record: `{"time":"invalid"}`, want: `{"time":"invalid"}`},
		{name: "rfc3339", record: ` {"time":"2026-09-05T14:34:56.123456+02:00","n":9007199254740993} `, field: "time", want: fmt.Sprintf(` {"time":"2026-09-05T14:34:56.123456+02:00","n":9007199254740993,"_timestamp":%d} `, wantTime)},
		{name: "slogx", record: `{"time":"2026-09-05 12:34:56.123456"}`, field: "time", want: fmt.Sprintf(`{"time":"2026-09-05 12:34:56.123456","_timestamp":%d}`, wantTime)},
		{name: "explicit layout", record: `{"when":"2026/09/05 12:34:56.123456"}`, field: "when", layout: "2006/01/02 15:04:05.000000", want: fmt.Sprintf(`{"when":"2026/09/05 12:34:56.123456","_timestamp":%d}`, wantTime)},
		{name: "integer micros", record: `{"time":-123}`, field: "time", want: `{"time":-123,"_timestamp":-123}`},
		{name: "first duplicate wins", record: `{"time":1,"time":2}`, field: "time", want: `{"time":1,"time":2,"_timestamp":1}`},
		{name: "exact name", record: `{"Time":"invalid"}`, field: "time", want: `{"Time":"invalid"}`},
		{name: "nested ignored", record: `{"context":{"time":"invalid"}}`, field: "time", want: `{"context":{"time":"invalid"}}`},
		{name: "empty object", record: `{}`, field: "time", want: `{}`},
		{name: "existing timestamp", record: `{"time":"invalid","_timestamp":123}`, field: "time", want: `{"time":"invalid","_timestamp":123}`},
		{name: "existing at timestamp", record: `{"@timestamp":null,"time":"invalid"}`, field: "time", want: `{"@timestamp":null,"time":"invalid"}`},
		{name: "invalid timestamp", record: `{"time":"secret-value"}`, field: "time", wantError: true},
		{name: "null timestamp", record: `{"time":null}`, field: "time", wantError: true},
		{name: "fractional timestamp", record: `{"time":1.2}`, field: "time", wantError: true},
		{name: "overflow timestamp", record: `{"time":9223372036854775808}`, field: "time", wantError: true},
		{name: "array", record: `[]`, wantError: true},
		{name: "invalid outer whitespace", record: "\v{}\v", wantError: true},
		{name: "malformed", record: `{"message":"secret-value"`, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			destination := &Destination{timestampField: test.field, timestampLayout: test.layout}
			record := json.RawMessage(test.record)
			got, err := destination.mapTimestamp(record)
			if (err != nil) != test.wantError || string(got) != test.want {
				t.Fatalf("mapTimestamp = %s, %v; want %s, error %t", got, err, test.want, test.wantError)
			}
			if string(record) != test.record {
				t.Fatal("input was modified")
			}
			if err != nil && strings.Contains(err.Error(), "secret-value") {
				t.Fatal("error contains record contents")
			}
		})
	}
}

func TestInvalidBatchIsNotSent(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	destination, err := New(Config{Endpoint: server.URL, TimestampField: "time"})
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []json.RawMessage{json.RawMessage(`[]`), json.RawMessage(`{"time":"secret-value"}`)} {
		err := destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`), invalid})
		if _, retry := errors.AsType[*collector.RetryError](err); err == nil || retry {
			t.Fatalf("invalid batch = %v, want terminal error", err)
		}
	}
	if err := destination.Send(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid or empty batch made a request")
	}
}

func TestAcknowledgment(t *testing.T) {
	for _, test := range []struct {
		name      string
		body      string
		wantError error
		retry     bool
	}{
		{name: "complete", body: `{"code":200,"status":[{"successful":2,"failed":0}]}`},
		{name: "multiple streams", body: `{"code":200,"status":[{"successful":1,"failed":0},{"successful":1,"failed":0}]}`},
		{name: "partial", body: `{"code":200,"status":[{"successful":1,"failed":1,"error":"secret-value"}]}`, wantError: ErrRejectedRecords},
		{name: "later partial", body: `{"code":200,"status":[{"successful":3,"failed":0},{"successful":0,"failed":1}]}`, wantError: ErrRejectedRecords},
		{name: "too few", body: `{"code":200,"status":[{"successful":1,"failed":0}]}`, wantError: ErrRecordCountMismatch, retry: true},
		{name: "too many", body: `{"code":200,"status":[{"successful":3,"failed":0}]}`, wantError: ErrRecordCountMismatch, retry: true},
		{name: "malformed", body: `secret-value`, wantError: ErrInvalidAcknowledgment, retry: true},
		{name: "empty", body: ``, wantError: ErrInvalidAcknowledgment, retry: true},
		{name: "missing code", body: `{"status":[{"successful":2,"failed":0}]}`, wantError: ErrInvalidAcknowledgment, retry: true},
		{name: "missing status", body: `{"code":200}`, wantError: ErrMissingRecordCounts, retry: true},
		{name: "missing failure count", body: `{"code":200,"status":[{"successful":2}]}`, wantError: ErrInvalidRecordCounts, retry: true},
		{name: "negative count", body: `{"code":200,"status":[{"successful":-1,"failed":0}]}`, wantError: ErrInvalidRecordCounts, retry: true},
		{name: "temporary body code", body: `{"code":503}`, wantError: ErrTemporaryIngestionFailure, retry: true},
		{name: "terminal body code", body: `{"code":400}`, wantError: ErrIngestionFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := acknowledge([]byte(test.body), 2)
			if _, retry := errors.AsType[*collector.RetryError](err); !errors.Is(err, test.wantError) || retry != test.retry {
				t.Fatalf("acknowledge = %v; want error %v, retry %t", err, test.wantError, test.retry)
			}
			if err != nil && strings.Contains(err.Error(), "secret-value") {
				t.Fatal("error contains response contents")
			}
		})
	}
}

func TestHTTPFailures(t *testing.T) {
	for _, status := range []int{302, 400, 401, 408, 413, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "3")
				w.WriteHeader(status)
				fmt.Fprint(w, "secret-value")
			}))
			defer server.Close()
			destination, err := New(Config{Endpoint: server.URL + "/?token=secret-value"})
			if err != nil {
				t.Fatal(err)
			}
			err = destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`)})
			transient, retry := errors.AsType[*collector.RetryError](err)
			wantRetry := status == 408 || status == 429 || status >= 500
			if err == nil || retry != wantRetry {
				t.Fatalf("Send = %v; want retry %t", err, wantRetry)
			}
			if wantRetry && transient.After != 3*time.Second {
				t.Fatalf("retry delay = %v", transient.After)
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Fatal("error contains sensitive contents")
			}
		})
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var calls atomic.Int64
	redirected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer redirected.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirected.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	destination, err := New(Config{Endpoint: server.URL, Username: "user", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`)}); err == nil {
		t.Fatal("redirect was acknowledged")
	}
	if calls.Load() != 0 {
		t.Fatal("redirect was followed")
	}
}

func TestCancellationAndTimeout(t *testing.T) {
	for _, parentCancellation := range []bool{false, true} {
		t.Run(fmt.Sprint(parentCancellation), func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			timeout := 50 * time.Millisecond
			if parentCancellation {
				timeout = time.Second * 5
			}
			destination, err := New(Config{Endpoint: server.URL + "/?token=secret-value", Timeout: timeout})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if parentCancellation {
				go func() {
					<-started
					cancel()
				}()
			}
			err = destination.Send(ctx, []json.RawMessage{json.RawMessage(`{}`)})
			_, retry := errors.AsType[*collector.RetryError](err)
			if parentCancellation {
				if !errors.Is(err, context.Canceled) || retry {
					t.Fatalf("cancellation = %v", err)
				}
			} else if !retry {
				t.Fatalf("timeout = %v, want retryable error", err)
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Fatal("error contains URL query")
			}
		})
	}
}

func TestLargeAcknowledgment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"code":200,"status":[{"successful":1,"failed":0}],"detail":"%s"}`, strings.Repeat("x", 2<<20))
	}))
	defer server.Close()
	destination, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("large acknowledgment = %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	for _, cfg := range []Config{
		{Endpoint: ""},
		{Endpoint: "/api/org/stream/_json"},
		{Endpoint: "ftp://example.invalid"},
		{Endpoint: "http://user:secret-value@example.invalid"},
		{Endpoint: "http://example.invalid/#secret-value"},
		{Endpoint: "http://example.invalid/%zz?secret-value"},
		{Endpoint: "http://example.invalid", Timeout: -time.Second},
		{Endpoint: "http://example.invalid", Username: "user", Headers: http.Header{"authorization": {"secret-value"}}},
	} {
		if _, err := New(cfg); err == nil {
			t.Error("invalid configuration accepted")
		} else if strings.Contains(err.Error(), "secret-value") {
			t.Error("configuration error contains sensitive contents")
		}
	}
	destination, err := New(Config{Endpoint: "https://example.invalid/api/org/stream/_json", Headers: http.Header{"authorization": {"Bearer token"}}})
	if err != nil {
		t.Fatal(err)
	}
	if destination.client.Timeout != 10*time.Second || destination.headers.Get("Authorization") != "Bearer token" {
		t.Fatal("default timeout or custom authorization was not applied")
	}
}

func TestNewRejectsInvalidHeaders(t *testing.T) {
	for _, test := range []struct {
		name    string
		headers http.Header
		want    error
	}{
		{name: "name", headers: http.Header{"Invalid Header": {"fixture-secret"}}, want: httpx.ErrInvalidHeaderName},
		{name: "value", headers: http.Header{"Authorization": {"fixture-secret\n"}}, want: httpx.ErrInvalidHeaderValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(Config{Endpoint: "http://example.invalid", Headers: test.headers})
			if !errors.Is(err, test.want) {
				t.Fatalf("New = %v, want %v", err, test.want)
			}
			if strings.Contains(err.Error(), "fixture-secret") {
				t.Fatal("header error contains credentials")
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		value string
		want  time.Duration
	}{
		{value: "15", want: 15 * time.Second},
		{value: now.Add(time.Minute).Format(http.TimeFormat), want: time.Minute},
		{value: now.Add(-time.Minute).Format(http.TimeFormat)},
		{value: "-1"},
		{value: "9223372036854775807"},
		{value: "invalid"},
	} {
		if got := retryAfter(test.value, now); got != test.want {
			t.Errorf("retryAfter(%q) = %v; want %v", test.value, got, test.want)
		}
	}
}

func benchmarkRecord(fields string) json.RawMessage {
	prefix := "{" + fields + `,"level":"INFO","msg":"request completed","http":{"status":200},"n":9007199254740993,"payload":"`
	return json.RawMessage(prefix + strings.Repeat("x", 256-len(prefix)-2) + `"}`)
}

// BenchmarkMapTimestamp measures one record, including JSON validation and the
// scan needed to preserve existing timestamps and the first duplicate key.
func BenchmarkMapTimestamp(b *testing.B) {
	for _, test := range []struct {
		name, fields, field, layout string
	}{
		{name: "disabled", fields: `"time":"2026-09-05T12:34:56.123456Z"`},
		{name: "RFC3339", fields: `"time":"2026-09-05T12:34:56.123456Z"`, field: "time"},
		{name: "slogx_auto", fields: `"time":"2026-09-05 12:34:56.123456"`, field: "time"},
		{name: "slogx_layout", fields: `"time":"2026-09-05 12:34:56.123456"`, field: "time", layout: "2006-01-02 15:04:05.000000"},
		{name: "existing_timestamp_first", fields: `"_timestamp":1788611696123456`, field: "time"},
	} {
		b.Run(test.name, func(b *testing.B) {
			record := benchmarkRecord(test.fields)
			destination := &Destination{timestampField: test.field, timestampLayout: test.layout}
			b.ReportAllocs()
			b.SetBytes(int64(len(record)))
			for b.Loop() {
				if _, err := destination.mapTimestamp(record); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSend measures a 500-record request: mapping, JSON array construction,
// request setup, HTTP client dispatch, and acknowledgment parsing. The transport
// consumes the body in memory; no sockets, TLS, or OpenObserve server are timed.
func BenchmarkSend(b *testing.B) {
	const batchSize = 500
	for _, field := range []string{"", "time"} {
		name := "mapping_disabled"
		if field != "" {
			name = "mapping_time"
		}
		b.Run(name, func(b *testing.B) {
			record := benchmarkRecord(`"time":"2026-09-05 12:34:56.123456"`)
			records := make([]json.RawMessage, batchSize)
			for i := range records {
				records[i] = record
			}
			destination, err := New(Config{
				Endpoint: "http://example.invalid/api/default/application/_json",
				Username: "benchmark", Password: "benchmark", TimestampField: field,
			})
			if err != nil {
				b.Fatal(err)
			}
			ack := fmt.Sprintf(`{"code":200,"status":[{"successful":%d,"failed":0}]}`, batchSize)
			destination.client.Transport = benchmarkTransport(func(request *http.Request) (*http.Response, error) {
				_, readErr := io.Copy(io.Discard, request.Body)
				closeErr := request.Body.Close()
				if readErr != nil {
					return nil, readErr
				}
				if closeErr != nil {
					return nil, closeErr
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(ack)), Header: make(http.Header)}, nil
			})
			ctx := b.Context()
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * len(record)))
			for b.Loop() {
				if err := destination.Send(ctx, records); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(batchSize, "records/op")
			b.ReportMetric(float64(b.N)*batchSize/b.Elapsed().Seconds(), "records/s")
		})
	}
}

type benchmarkTransport func(*http.Request) (*http.Response, error)

func (f benchmarkTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
