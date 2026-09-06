package otlp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rah-0/slogx"
	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/httpx"
)

func TestDestinationPreservesResourceEnvelopesAndHeaders(t *testing.T) {
	resources := []json.RawMessage{
		json.RawMessage(`{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},"schemaUrl":"https://example.com/schema","scopeSpans":[{"scope":{"name":"producer"},"spans":[{"traceId":"1234567890abcdef1234567890abcdef","spanId":"1234567890abcdef","future":{"number":9007199254740993,"precise":1.2300e-99},"events":[{"name":"exception","attributes":[{"key":"err","value":{"kvlistValue":{"values":[{"key":"cause","value":{"stringValue":"unavailable"}}]}}}]}]}]}]}`),
		json.RawMessage(`{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"worker"}}]},"scopeSpans":[{"spans":[{"traceId":"abcdef1234567890abcdef1234567890","spanId":"abcdef1234567890"}]}],"futureResource":[9007199254740993]}`),
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/traces" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if username, password, ok := r.BasicAuth(); !ok || username != "user" || password != "password" {
			t.Error("Basic authentication changed")
		}
		if r.Header.Get("Stream-Name") != "traces" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" {
			t.Error("routing or content type changed")
		}
		if len(r.Header.Values("Content-Type")) != 1 || len(r.Header.Values("Accept")) != 1 {
			t.Error("caller headers duplicated the required JSON headers")
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var request struct {
			ResourceSpans []json.RawMessage `json:"resourceSpans"`
		}
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(request.ResourceSpans, resources) {
			t.Errorf("resource/span JSON was changed: %s", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	headers := http.Header{
		"stream-name":  {"traces"},
		"content-type": {"application/x-protobuf"}, "accept": {"application/x-protobuf"},
	}
	destination, err := New(Config{
		Endpoint: server.URL + "/v1/traces", Username: "user", Password: "password", Headers: headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	headers["stream-name"][0] = "changed"
	records := []json.RawMessage{
		json.RawMessage(`{"futureEnvelope":1,"resourceSpans":[` + string(resources[0]) + `],"futureEnvelope":2}`),
		json.RawMessage(`{"resourceSpans":[` + string(resources[1]) + `]}`),
	}
	original := string(records[0]) + string(records[1])
	if err := destination.Send(t.Context(), records); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || string(records[0])+string(records[1]) != original {
		t.Fatal("Send retried or mutated input")
	}
}

func TestDestinationRejectsMalformedBatchBeforeSending(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	destination, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		``, `{`, `null`, `[]`, `{"resourceSpans":null}`,
		`{"resourceSpans":{}}`, `{"resourceSpans":[null]}`, `{"resourceSpans":[[]]}`,
		`{"resourceSpans":[{}]}`, `{"resourceSpans":[{"scopeSpans":null}]}`,
		`{"resourceSpans":[{"scopeSpans":"bad"}]}`,
		`{"resourceSpans":[{"scopeSpans":[],"scopeSpans":[]}]}`,
		`{"resourceSpans":[{"scopeSpans":[null]}]}`, `{"resourceSpans":[{"scopeSpans":[[]]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":null}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[],"spans":[]}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[null]}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[1]}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[[]]}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{}]},{"spans":null}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{}]}]},{}]}`,
		`{"resourceSpans":[],"resourceSpans":[]}`,
		`{"resourceSpans":[],"resource\u0053pans":[]}`,
		`{"resourceSpans":[]} {"resourceSpans":[]}`,
	} {
		t.Run(invalid, func(t *testing.T) {
			err := destination.Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef"), json.RawMessage(invalid)})
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Send = %v, want invalid request", err)
			}
			if _, retry := errors.AsType[*collector.RetryError](err); retry {
				t.Fatal("malformed request is retryable")
			}
			if calls.Load() != 0 {
				t.Fatal("malformed batch reached the server")
			}
		})
	}
	for _, empty := range [][]json.RawMessage{
		nil,
		{json.RawMessage(`{}`), json.RawMessage(`{"msg":"ordinary log"}`)},
		{json.RawMessage(`{"resourceSpans":[]}`)},
		{json.RawMessage(`{"resourceSpans":[{"scopeSpans":[]}]}`)},
		{json.RawMessage(`{"resourceSpans":[{"scopeSpans":[{"spans":[]}]}]}`)},
	} {
		if err := destination.Send(t.Context(), empty); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("empty batch reached server")
	}
}

func TestDestinationMapsRetryAndPartialAcknowledgment(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		retry  bool
		want   error
	}{
		{"temporary", 503, "private server error", true, ErrHTTPStatus},
		{"permanent", 400, "private server error", false, ErrHTTPStatus},
		{"partial", 200, `{"partialSuccess":{"rejectedSpans":"3","errorMessage":"private server error"}}`, false, ErrRejectedSpans},
		{"rejections exceed sent spans", 200, `{"partialSuccess":{"rejectedSpans":"4","errorMessage":"private server error"}}`, false, ErrRejectedSpans},
		{"invalid acknowledgment", 200, `not json private server error`, false, ErrInvalidResponse},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "2")
				w.WriteHeader(test.status)
				io.WriteString(w, test.body)
			}))
			defer server.Close()
			destination, err := New(Config{Endpoint: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			err = destination.Send(t.Context(), []json.RawMessage{
				traceEnvelope("1234567890abcdef", "2234567890abcdef"), traceEnvelope("3234567890abcdef"),
			})
			if !errors.Is(err, test.want) || strings.Contains(err.Error(), "private") {
				t.Fatalf("Send = %v", err)
			}
			retry, ok := errors.AsType[*collector.RetryError](err)
			if ok != test.retry || ok && retry.After != 2*time.Second || calls.Load() != 1 {
				t.Fatalf("retry = %#v, attempts = %d", retry, calls.Load())
			}
		})
	}
}

func TestDestinationAuthenticationConflict(t *testing.T) {
	_, err := New(Config{Endpoint: "http://example.com", Username: "user", Headers: http.Header{"authorization": {"secret"}}})
	if !errors.Is(err, ErrAuthenticationConflict) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("New = %v", err)
	}
}

func traceEnvelope(ids ...string) json.RawMessage {
	var spans []string
	for _, id := range ids {
		spans = append(spans, `{"traceId":"1234567890abcdef1234567890abcdef","spanId":"`+id+`"}`)
	}
	return json.RawMessage(`{"resourceSpans":[{"scopeSpans":[{"spans":[` + strings.Join(spans, ",") + `]}]}]}`)
}

func respond(writer http.ResponseWriter, body string) {
	writer.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(writer, body)
}

func newDestination(t *testing.T, server *httptest.Server, config Config) *Destination {
	t.Helper()
	config.Endpoint = server.URL + "/v1/traces"
	if config.Timeout == 0 {
		config.Timeout = time.Second
	}
	destination, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return destination
}

func TestRedirectDoesNotForwardCredentials(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		forwarded.Add(1)
		respond(writer, `{}`)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	destination := newDestination(t, server, Config{Headers: http.Header{"Authorization": {"Bearer test-secret"}}})
	err := destination.Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
	if !errors.Is(err, ErrHTTPStatus) || forwarded.Load() != 0 {
		t.Fatalf("export = %v; redirected requests = %d", err, forwarded.Load())
	}
	if strings.Contains(err.Error(), "test-secret") || strings.Contains(err.Error(), server.URL) {
		t.Fatal("error exposed endpoint or credentials")
	}
}

func TestResponseSizeIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		respond(writer, `{"unknown":"`+strings.Repeat("x", 4<<20)+`"}`)
	}))
	defer server.Close()
	err := newDestination(t, server, Config{}).Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("oversized response = %v", err)
	}
}

func TestInvalidOptionsDoNotExposeValues(t *testing.T) {
	for _, options := range []Config{
		{},
		{Endpoint: "ftp://example.com/private-secret"},
		{Endpoint: "https://username:private-secret@example.com/v1/traces"},
		{Endpoint: "https://example.com/v1/traces#private-secret"},
		{Endpoint: "https://example.com:invalid/private-secret"},
		{Endpoint: "https://example.com:0/private-secret"},
		{Endpoint: "https://example.com:65536/private-secret"},
		{Endpoint: "https://example.com:99999/private-secret"},
		{Endpoint: "https://[::1]:99999/private-secret"},
		{Endpoint: "https://example.com/v1/traces", Timeout: -1},
	} {
		_, err := New(options)
		if !errors.Is(err, ErrInvalidOptions) || strings.Contains(err.Error(), "private-secret") {
			t.Fatalf("New = %v; want sanitized invalid-options error", err)
		}
	}
	for _, endpoint := range []string{"http://localhost:4318/v1/traces", "https://example.com:65535/v1/traces", "https://example.com/v1/traces"} {
		if _, err := New(Config{Endpoint: endpoint}); err != nil {
			t.Fatalf("valid endpoint options = %v", err)
		}
	}
}

func TestConcurrentSends(t *testing.T) {
	var spans atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload struct {
			Resources []struct {
				Scopes []struct {
					Spans []json.RawMessage `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		for _, resource := range payload.Resources {
			for _, scope := range resource.Scopes {
				spans.Add(int32(len(scope.Spans)))
			}
		}
		respond(writer, `{}`)
	}))
	defer server.Close()
	destination := newDestination(t, server, Config{})
	var workers sync.WaitGroup
	for range 10 {
		workers.Go(func() {
			if err := destination.Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef", "2234567890abcdef")}); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if spans.Load() != 20 {
		t.Fatalf("exported spans = %d; want 20", spans.Load())
	}
}

func TestSendNilContext(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		respond(writer, `{}`)
	}))
	defer server.Close()
	destination := newDestination(t, server, Config{})
	if err := destination.Send(nil, nil); err != nil || calls.Load() != 0 {
		t.Fatalf("empty batch = %v; requests = %d", err, calls.Load())
	}
	if err := destination.Send(nil, []json.RawMessage{json.RawMessage(`{"resourceSpans":[]}`)}); err != nil || calls.Load() != 0 {
		t.Fatalf("empty request = %v; requests = %d", err, calls.Load())
	}
	if err := destination.Send(nil, []json.RawMessage{traceEnvelope("1234567890abcdef")}); err != nil || calls.Load() != 1 {
		t.Fatalf("nil-context request = %v; requests = %d", err, calls.Load())
	}
}

func TestSendOneAttemptAndRetryClassification(t *testing.T) {
	for _, status := range []int{200, 201, 204, 400, 401, 403, 404, 408, 429, 500, 501, 502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				writer.Header().Set("Content-Type", "application/json")
				writer.Header().Set("Retry-After", "3")
				writer.WriteHeader(status)
				_, _ = io.WriteString(writer, `{}`)
			}))
			defer server.Close()
			err := newDestination(t, server, Config{}).Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
			transient, retry := errors.AsType[*collector.RetryError](err)
			wantRetry := status == 429 || status == 502 || status == 503 || status == 504
			if calls.Load() != 1 || retry != wantRetry || (err == nil) != (status == 200) {
				t.Fatalf("Send = %v, calls=%d, retry=%t", err, calls.Load(), retry)
			}
			if wantRetry && (!errors.Is(err, ErrHTTPStatus) || transient.After != 3*time.Second) {
				t.Fatalf("retry cause/delay = %+v", transient)
			}
		})
	}
}

func TestSendTimeoutAndCallerCancellation(t *testing.T) {
	for _, test := range []struct {
		name                                                       string
		parentCancels, parentDeadline, nilContext, responseStarted bool
	}{
		{name: "attempt timeout"},
		{name: "nil context timeout", nilContext: true},
		{name: "caller cancellation", parentCancels: true},
		{name: "caller deadline", parentDeadline: true},
		{name: "response timeout", responseStarted: true},
		{name: "response caller cancellation", parentCancels: true, responseStarted: true},
		{name: "response caller deadline", parentDeadline: true, responseStarted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			timeout := 30 * time.Millisecond
			if test.parentDeadline {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, timeout)
				defer deadlineCancel()
				timeout = time.Second
			}
			if test.nilContext {
				ctx = nil
			}
			var calls atomic.Int32
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				if test.responseStarted {
					writer.Header().Set("Content-Type", "application/json")
					writer.(http.Flusher).Flush()
				}
				if test.parentCancels {
					cancel()
				}
				<-release
			}))
			defer server.Close()
			defer close(release)
			destination := newDestination(t, server, Config{Timeout: timeout})
			err := destination.Send(ctx, []json.RawMessage{traceEnvelope("1234567890abcdef")})
			_, retry := errors.AsType[*collector.RetryError](err)
			want := context.DeadlineExceeded
			if test.parentCancels {
				want = context.Canceled
			}
			wantRetry := !test.parentCancels && !test.parentDeadline
			if !errors.Is(err, want) || retry != wantRetry || calls.Load() != 1 {
				t.Fatalf("Send = %v, retry=%t, calls=%d", err, retry, calls.Load())
			}
		})
	}
}

func TestSendRejectionAndTransportFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		respond(writer, `{"partialSuccess":{"rejectedSpans":"2"}}`)
	}))
	destination := newDestination(t, server, Config{})
	err := destination.Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
	if !errors.Is(err, ErrRejectedSpans) {
		t.Fatalf("rejection count greater than sent = %v", err)
	}
	if _, retry := errors.AsType[*collector.RetryError](err); retry {
		t.Fatal("rejection count greater than sent is retryable")
	}
	server.Close()
	err = destination.Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
	if _, retry := errors.AsType[*collector.RetryError](err); !retry || !errors.Is(err, ErrRequestFailed) {
		t.Fatalf("transport failure = %v", err)
	}
}

func TestSendRetryAfterHints(t *testing.T) {
	deadline := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	for _, test := range []struct {
		header string
		want   time.Duration
	}{
		{"2", 2 * time.Second},
		{deadline.Format(http.TimeFormat), time.Until(deadline)},
		{"0", 0}, {"-1", 0}, {"invalid", 0}, {"9223372036854775807", 0},
	} {
		t.Run(test.header, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				writer.Header().Set("Retry-After", test.header)
				writer.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			err := newDestination(t, server, Config{}).Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
			retry, ok := errors.AsType[*collector.RetryError](err)
			if !ok || !errors.Is(err, ErrHTTPStatus) || calls.Load() != 1 {
				t.Fatalf("Send = %v; requests = %d", err, calls.Load())
			}
			if difference := retry.After - test.want; difference < -time.Second || difference > time.Second {
				t.Fatalf("Retry-After = %v; want %v", retry.After, test.want)
			}
		})
	}
}

func TestSendResponseReadFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.Header().Set("Content-Length", "100")
		respond(writer, `{}`)
	}))
	defer server.Close()
	err := newDestination(t, server, Config{}).Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
	if _, retry := errors.AsType[*collector.RetryError](err); !retry || !errors.Is(err, ErrRequestFailed) || calls.Load() != 1 {
		t.Fatalf("truncated response = %v; requests = %d", err, calls.Load())
	}
}

func TestJournalReplaysExactTraceAfterFailedDelivery(t *testing.T) {
	requireJournalPlatform(t)
	var input bytes.Buffer
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	slog.SetDefault(slogx.New(slogx.Options{Format: slogx.JSON, Writer: &input}))
	ctx, root := slogx.StartSpan(t.Context(), "handler")
	ctx, child := slogx.StartSpan(ctx, "service")
	_, leaf := slogx.StartSpan(ctx, "database")
	for _, span := range []*slogx.Span{leaf, child, root} {
		span.End()
	}
	if bytes.Count(input.Bytes(), []byte{'\n'}) != 3 {
		t.Fatal("expected one JSON record per completed span")
	}
	var available atomic.Bool
	var mu sync.Mutex
	var attempts [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		mu.Lock()
		attempts = append(attempts, payload)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	destination, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	options := collector.Options{
		JournalDir: t.TempDir(), JournalKey: "otlp:test-traces", MaxRetries: 1,
		RetryInterval: time.Millisecond, MaxRetryInterval: time.Millisecond,
	}
	err = collector.Run(t.Context(), io.NopCloser(&input), destination, options)
	if !errors.Is(err, ErrHTTPStatus) {
		t.Fatalf("failed delivery = %v", err)
	}
	mu.Lock()
	firstAttempts := len(attempts)
	mu.Unlock()
	if firstAttempts != 2 {
		t.Fatalf("attempts before restart = %d; want one retry owned by collector", firstAttempts)
	}
	available.Store(true)
	if err := collector.Run(t.Context(), io.NopCloser(strings.NewReader("")), destination, options); err != nil {
		t.Fatalf("journal replay = %v", err)
	}
	// A second restart with no input must find an acknowledged, empty journal.
	if err := collector.Run(t.Context(), io.NopCloser(strings.NewReader("")), destination, options); err != nil {
		t.Fatalf("acknowledged journal = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 3 || !bytes.Equal(attempts[0], attempts[1]) || !bytes.Equal(attempts[0], attempts[2]) {
		t.Fatal("retry/replay changed the request or acknowledged records were replayed")
	}
	spans := writtenSpans(t, attempts[2])
	var ids, parents []string
	for _, span := range spans {
		if span["traceId"] != root.SpanContext().TraceID {
			t.Fatal("trace ID changed during persistence")
		}
		ids = append(ids, span["spanId"].(string))
		parents = append(parents, optionalString(span["parentSpanId"]))
	}
	if !reflect.DeepEqual(ids, []string{leaf.SpanContext().SpanID, child.SpanContext().SpanID, root.SpanContext().SpanID}) ||
		!reflect.DeepEqual(parents, []string{child.SpanContext().SpanID, root.SpanContext().SpanID, ""}) {
		t.Fatal("span identity or parent chain changed during persistence")
	}
}

func TestJournalRetainsPartialRejectionWithoutRetry(t *testing.T) {
	requireJournalPlatform(t)
	for _, test := range []struct{ name, rejected string }{
		{"partial", "1"},
		{"rejections exceed sent spans", "3"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var accept atomic.Bool
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if accept.Load() {
					io.WriteString(w, `{}`)
				} else {
					io.WriteString(w, `{"partialSuccess":{"rejectedSpans":"`+test.rejected+`"}}`)
				}
			}))
			defer server.Close()
			destination, err := New(Config{Endpoint: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			options := collector.Options{
				JournalDir: t.TempDir(), MaxRetries: 2,
				RetryInterval: time.Millisecond, MaxRetryInterval: time.Millisecond,
			}
			envelope := traceEnvelope("1234567890abcdef", "2234567890abcdef")
			err = collector.Run(t.Context(), io.NopCloser(bytes.NewReader(envelope)), destination, options)
			if !errors.Is(err, ErrRejectedSpans) || calls.Load() != 1 {
				t.Fatalf("partial rejection = %v; attempts = %d", err, calls.Load())
			}
			// An explicit restart proves the original envelope remains journaled.
			accept.Store(true)
			if err := collector.Run(t.Context(), io.NopCloser(strings.NewReader("")), destination, options); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatal("partial rejection was discarded instead of retained")
			}
		})
	}
}

func requireJournalPlatform(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd":
	default:
		t.Skip("journal locking requires a supported Unix platform")
	}
}

func TestInvalidHeadersDoNotExposeValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		headers http.Header
		want    error
	}{
		{"name", http.Header{"Invalid Header": {"private-secret"}}, httpx.ErrInvalidHeaderName},
		{"value", http.Header{"Authorization": {"private-secret\r\n"}}, httpx.ErrInvalidHeaderValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(Config{Endpoint: "https://example.com/v1/traces", Headers: test.headers})
			if !errors.Is(err, test.want) || strings.Contains(err.Error(), "private-secret") {
				t.Fatalf("New = %v; want sanitized header error %v", err, test.want)
			}
		})
	}
}
