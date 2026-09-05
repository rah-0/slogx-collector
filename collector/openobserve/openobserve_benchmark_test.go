package openobserve

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

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
