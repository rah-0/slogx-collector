package otlp

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rah-0/slogx-collector/internal/collector"
)

func TestAcknowledgments(t *testing.T) {
	for _, test := range []struct {
		name, body, contentType string
		count                   string
		want                    error
	}{
		{name: "empty message", body: `{}`},
		{name: "null partial", body: `{"partialSuccess":null}`},
		{name: "empty partial", body: `{"partialSuccess":{}}`},
		{name: "zero string", body: `{"partialSuccess":{"rejectedSpans":"0"}}`},
		{name: "zero number", body: `{"partialSuccess":{"rejectedSpans":0}}`},
		{name: "zero decimal", body: `{"partialSuccess":{"rejectedSpans":0.0}}`},
		{name: "zero exponent string", body: `{"partialSuccess":{"rejectedSpans":"0e0"}}`},
		{name: "zero huge exponent", body: `{"partialSuccess":{"rejectedSpans":"0e999999999999999999999"}}`},
		{name: "warning", body: `{"partialSuccess":{"rejectedSpans":"0","errorMessage":"backend-secret"}}`},
		{name: "unknown fields", body: `{"future":{},"partialSuccess":{"future":1}}`},
		{name: "partial rejection string", body: `{"partialSuccess":{"rejectedSpans":"1","errorMessage":"backend-secret"}}`, want: ErrRejectedSpans},
		{name: "partial rejection number", body: `{"partialSuccess":{"rejectedSpans":1}}`, want: ErrRejectedSpans},
		{name: "partial rejection decimal", body: `{"partialSuccess":{"rejectedSpans":"1.000"}}`, want: ErrRejectedSpans},
		{name: "partial rejection exponent", body: `{"partialSuccess":{"rejectedSpans":10e-1}}`, want: ErrRejectedSpans},
		{name: "partial rejection fractional mantissa", body: `{"partialSuccess":{"rejectedSpans":"0.1e1"}}`, want: ErrRejectedSpans},
		{name: "exact large decimal", body: `{"partialSuccess":{"rejectedSpans":"9007199254740993.0"}}`, count: "9007199254740993", want: ErrRejectedSpans},
		{name: "maximum int64 exponent", body: `{"partialSuccess":{"rejectedSpans":9223372036854775807e0}}`, count: "9223372036854775807", want: ErrRejectedSpans},
		{name: "nonintegral", body: `{"partialSuccess":{"rejectedSpans":"1.0000000000000000000001"}}`, want: ErrInvalidResponse},
		{name: "tiny nonintegral", body: `{"partialSuccess":{"rejectedSpans":"1e-99999999999999999"}}`, want: ErrInvalidResponse},
		{name: "int64 overflow", body: `{"partialSuccess":{"rejectedSpans":"9223372036854775808.0"}}`, want: ErrInvalidResponse},
		{name: "huge positive exponent", body: `{"partialSuccess":{"rejectedSpans":"1e99999999999999999"}}`, want: ErrInvalidResponse},
		{name: "negative rejected", body: `{"partialSuccess":{"rejectedSpans":"-1"}}`, want: ErrInvalidResponse},
		{name: "negative decimal", body: `{"partialSuccess":{"rejectedSpans":"-1.0"}}`, want: ErrInvalidResponse},
		{name: "negative zero decimal", body: `{"partialSuccess":{"rejectedSpans":"-0.0"}}`},
		{name: "rejection count greater than sent", body: `{"partialSuccess":{"rejectedSpans":"2"}}`, want: ErrRejectedSpans},
		{name: "invalid count", body: `{"partialSuccess":{"rejectedSpans":"backend-secret"}}`, want: ErrInvalidResponse},
		{name: "invalid partial", body: `{"partialSuccess":"backend-secret"}`, want: ErrInvalidResponse},
		{name: "null message", body: `null`, want: ErrInvalidResponse},
		{name: "array", body: `[]`, want: ErrInvalidResponse},
		{name: "empty body", want: ErrInvalidResponse},
		{name: "malformed", body: `{"partialSuccess":`, want: ErrInvalidResponse},
		{name: "trailing", body: `{} {}`, want: ErrInvalidResponse},
		{name: "wrong content type", body: `{}`, contentType: "text/plain", want: ErrInvalidResponse},
		{name: "charset", body: `{}`, contentType: "application/json; charset=utf-8"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				contentType := test.contentType
				if contentType == "" {
					contentType = "application/json"
				}
				writer.Header().Set("Content-Type", contentType)
				_, _ = io.WriteString(writer, test.body)
			}))
			defer server.Close()
			err := newDestination(t, server, Config{}).Send(t.Context(), []json.RawMessage{traceEnvelope("1234567890abcdef")})
			if !errors.Is(err, test.want) {
				t.Fatalf("export = %v; want %v", err, test.want)
			}
			if test.count != "" && err.Error() != ErrRejectedSpans.Error()+": "+test.count {
				t.Fatalf("rejected span count changed precision: %v", err)
			}
			if _, retry := errors.AsType[*collector.RetryError](err); retry {
				t.Fatal("acknowledgment failure is retryable")
			}
			if err != nil && strings.Contains(err.Error(), "backend-secret") {
				t.Fatal("error exposed backend content")
			}
			if calls.Load() != 1 {
				t.Fatalf("requests = %d; want 1", calls.Load())
			}
		})
	}
}
