// Package openobserve sends JSON batches to an OpenObserve JSON ingestion API.
package openobserve

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/httpx"
)

const (
	defaultTimeout       = 10 * time.Second
	slogxTimeLayout      = "2006-01-02 15:04:05.000000"
	minJSONObjectBytes   = len("{}")
	timestampFieldPrefix = `,"_timestamp":`
	maxInt64TextBytes    = 20 // 19 decimal digits and an optional minus sign.
)

// Config configures an OpenObserve JSON ingestion endpoint.
type Config struct {
	// Endpoint is the complete HTTP(S) URL, including organization and stream.
	Endpoint string
	// Username and Password enable HTTP Basic authentication when either is set.
	Username string
	Password string
	// Headers adds request headers, for example Authorization for a bearer token.
	// Authorization cannot be combined with Username or Password.
	Headers http.Header
	// Timeout bounds each request, including reading its response. Zero uses 10s.
	Timeout time.Duration
	// TimestampField optionally identifies a top-level field to copy to _timestamp.
	// Existing _timestamp or @timestamp fields are preserved. Integer source values
	// are epoch microseconds; strings use TimestampLayout, or RFC3339Nano and the
	// slogx default layout when TimestampLayout is empty. Zone-less times use UTC.
	TimestampField  string
	TimestampLayout string
}

// Destination delivers complete batches. It is safe for concurrent use.
type Destination struct {
	endpoint        string
	username        string
	password        string
	headers         http.Header
	client          *http.Client
	timestampField  string
	timestampLayout string
}

var _ collector.Destination = (*Destination)(nil)

// New validates cfg and copies its headers. Redirects are never followed.
func New(cfg Config) (*Destination, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Opaque != "" || u.User != nil || u.Fragment != "" {
		return nil, ErrInvalidEndpoint
	}
	if cfg.Timeout < 0 {
		return nil, ErrNegativeTimeout
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	if err := httpx.ValidateHeaders(cfg.Headers); err != nil {
		return nil, err
	}
	headers := make(http.Header, len(cfg.Headers))
	for name, values := range cfg.Headers {
		if strings.EqualFold(name, "Authorization") && (cfg.Username != "" || cfg.Password != "") {
			return nil, ErrAuthenticationConflict
		}
		for _, value := range values {
			headers.Add(name, value)
		}
	}
	return &Destination{
		endpoint: cfg.Endpoint,
		username: cfg.Username,
		password: cfg.Password,
		headers:  headers,
		client: &http.Client{Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
		timestampField:  cfg.TimestampField,
		timestampLayout: cfg.TimestampLayout,
	}, nil
}

// Send posts records as a JSON array and verifies that the response acknowledges
// every record. Ambiguous responses and temporary delivery failures are retryable;
// partial acceptance is terminal because failed records cannot be identified.
// Errors never include endpoint URLs, credentials, or request/response payloads.
func (d *Destination) Send(ctx context.Context, records []json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	var body bytes.Buffer
	body.WriteByte('[')
	for i, record := range records {
		record, err := d.mapTimestamp(record)
		if err != nil {
			return fmt.Errorf("openobserve: record %d: %w", i+1, err)
		}
		if i != 0 {
			body.WriteByte(',')
		}
		body.Write(record)
	}
	body.WriteByte(']')
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, &body)
	if err != nil {
		return ErrRequestCreation
	}
	req.Header = d.headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	if d.username != "" || d.password != "" {
		req.SetBasicAuth(d.username, d.password)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return retry(ErrRequestFailed, 0)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		err := fmt.Errorf("%w %d", ErrHTTPStatus, resp.StatusCode)
		if temporaryStatus(resp.StatusCode) {
			return &collector.RetryError{Err: err, After: retryAfter(resp.Header.Get("Retry-After"), time.Now())}
		}
		return err
	}
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return retry(ErrReadAcknowledgment, 0)
	}
	return acknowledge(payload, len(records))
}

func retry(err error, after time.Duration) error {
	return &collector.RetryError{Err: err, After: after}
}

func temporaryStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
		if seconds >= 0 && seconds <= int64(math.MaxInt64/time.Second) {
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}

func acknowledge(payload []byte, count int) error {
	var ack struct {
		Code   *int `json:"code"`
		Status []struct {
			Successful *int64 `json:"successful"`
			Failed     *int64 `json:"failed"`
		} `json:"status"`
	}
	if json.Unmarshal(payload, &ack) != nil || ack.Code == nil {
		return retry(ErrInvalidAcknowledgment, 0)
	}
	for _, status := range ack.Status {
		if status.Failed != nil && *status.Failed > 0 {
			return ErrRejectedRecords
		}
	}
	if *ack.Code != http.StatusOK {
		if temporaryStatus(*ack.Code) {
			return retry(ErrTemporaryIngestionFailure, 0)
		}
		return ErrIngestionFailure
	}
	if len(ack.Status) == 0 {
		return retry(ErrMissingRecordCounts, 0)
	}
	var successful int64
	for _, status := range ack.Status {
		if status.Successful == nil || status.Failed == nil || *status.Successful < 0 || *status.Failed < 0 {
			return retry(ErrInvalidRecordCounts, 0)
		}
		if *status.Successful > int64(count)-successful {
			return retry(ErrRecordCountMismatch, 0)
		}
		successful += *status.Successful
	}
	if successful != int64(count) {
		return retry(ErrRecordCountMismatch, 0)
	}
	return nil
}

func (d *Destination) mapTimestamp(record json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(record)
	if len(trimmed) < minJSONObjectBytes || trimmed[0] != '{' || !json.Valid(record) {
		return nil, ErrInvalidJSONObject
	}
	if d.timestampField == "" {
		return record, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	if _, err := decoder.Token(); err != nil {
		return nil, ErrInvalidJSONObject
	}
	var source json.RawMessage
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalidJSONObject
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, ErrInvalidJSONObject
		}
		if key == "_timestamp" || key == "@timestamp" {
			return record, nil
		}
		if key == d.timestampField && source == nil {
			source = value
		}
	}
	if source == nil {
		return record, nil
	}
	micros, err := d.timestamp(source)
	if err != nil {
		return nil, err
	}
	// Insert before the original closing brace without re-encoding any fields.
	end := bytes.LastIndexByte(record, '}')
	mapped := make(json.RawMessage, 0, len(record)+len(timestampFieldPrefix)+maxInt64TextBytes)
	mapped = append(mapped, record[:end]...)
	mapped = append(mapped, timestampFieldPrefix...)
	mapped = strconv.AppendInt(mapped, micros, 10)
	mapped = append(mapped, record[end:]...)
	return mapped, nil
}

func (d *Destination) timestamp(source json.RawMessage) (int64, error) {
	if source[0] != '"' {
		if micros, err := strconv.ParseInt(string(source), 10, 64); err == nil {
			return micros, nil
		}
		return 0, ErrInvalidTimestampType
	}
	var value string
	if err := json.Unmarshal(source, &value); err != nil {
		return 0, ErrInvalidTimestampType
	}
	layouts := []string{time.RFC3339Nano, slogxTimeLayout}
	if d.timestampLayout != "" {
		layouts = []string{d.timestampLayout}
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UnixMicro(), nil
		}
	}
	return 0, ErrTimestampLayoutMismatch
}
