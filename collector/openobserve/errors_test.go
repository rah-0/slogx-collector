package openobserve_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rah-0/slogx-collector/collector"
	"github.com/rah-0/slogx-collector/collector/openobserve"
)

func TestValidationErrorSentinel(t *testing.T) {
	_, err := openobserve.New(openobserve.Config{Endpoint: "https://example.invalid", Timeout: -time.Second})
	if !errors.Is(err, openobserve.ErrNegativeTimeout) {
		t.Fatalf("New = %v; want ErrNegativeTimeout", err)
	}
}

func TestRecordErrorSentinels(t *testing.T) {
	destination, err := openobserve.New(openobserve.Config{Endpoint: "https://example.invalid", TimestampField: "time"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		record json.RawMessage
		want   error
	}{
		{name: "invalid object", record: json.RawMessage(`[]`), want: openobserve.ErrInvalidJSONObject},
		{name: "timestamp type", record: json.RawMessage(`{"time":null}`), want: openobserve.ErrInvalidTimestampType},
		{name: "timestamp layout", record: json.RawMessage(`{"time":"invalid"}`), want: openobserve.ErrTimestampLayoutMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`), test.record})
			if !errors.Is(err, test.want) {
				t.Fatalf("Send = %v; want %v", err, test.want)
			}
			if !strings.HasPrefix(err.Error(), "openobserve: record 2: ") {
				t.Fatalf("record context missing from error: %v", err)
			}
		})
	}
}

func TestDeliveryErrorSentinels(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		body       string
		disconnect bool
		truncated  bool
		want       error
		retry      bool
	}{
		{name: "transport", disconnect: true, want: openobserve.ErrRequestFailed, retry: true},
		{name: "response read", status: 200, body: `{}`, truncated: true, want: openobserve.ErrReadAcknowledgment, retry: true},
		{name: "invalid acknowledgment", status: 200, body: `invalid`, want: openobserve.ErrInvalidAcknowledgment, retry: true},
		{name: "record count mismatch", status: 200, body: `{"code":200,"status":[{"successful":0,"failed":0}]}`, want: openobserve.ErrRecordCountMismatch, retry: true},
		{name: "rejected records", status: 200, body: `{"code":200,"status":[{"successful":0,"failed":1}]}`, want: openobserve.ErrRejectedRecords},
		{name: "terminal status", status: 401, want: openobserve.ErrHTTPStatus},
		{name: "retryable status", status: 503, want: openobserve.ErrHTTPStatus, retry: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.disconnect {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
					return
				}
				if test.truncated {
					w.Header().Set("Content-Length", "100")
				}
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			destination, err := openobserve.New(openobserve.Config{Endpoint: server.URL, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			err = destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{}`)})
			if _, retry := errors.AsType[*collector.RetryError](err); !errors.Is(err, test.want) || retry != test.retry {
				t.Fatalf("Send = %v; want %v, retry %t", err, test.want, test.retry)
			}
			if test.want == openobserve.ErrHTTPStatus && err.Error() != fmt.Sprintf("openobserve: HTTP status %d", test.status) {
				t.Fatalf("HTTP status context changed: %v", err)
			}
		})
	}
}
