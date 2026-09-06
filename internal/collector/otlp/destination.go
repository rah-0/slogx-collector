// Package otlp converts journaled JSON span records to OTLP HTTP/JSON requests.
package otlp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/httpx"
)

const (
	defaultTimeout   = 10 * time.Second
	maxResponseBytes = 4 << 20
	portBitSize      = 16
)

// Config configures one OTLP HTTP/JSON traces destination.
type Config struct {
	// Endpoint is the complete HTTP(S) traces URL, including its path.
	Endpoint string
	// Username and Password enable HTTP Basic authentication when either is set.
	Username string
	Password string
	// Headers contains authentication and routing headers, such as stream-name.
	// Authorization cannot be combined with Username or Password. New copies it.
	Headers http.Header
	// Timeout bounds each HTTP attempt. Zero uses ten seconds.
	Timeout time.Duration
	// Resource identifies the producing service, for example service.name.
	// New snapshots its JSON representation for completed span records.
	Resource map[string]any
}

// Destination sends complete trace batches. It is safe for concurrent use.
// Retries and durable storage belong to collector.Run.
type Destination struct {
	endpoint string
	headers  http.Header
	client   *http.Client
	resource []KeyValue
}

var _ collector.Destination = (*Destination)(nil)

// New validates cfg and copies its headers. Redirects are never followed.
func New(cfg Config) (*Destination, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Hostname() == "" || u.Opaque != "" || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf(invalidEndpointFormat, ErrInvalidOptions)
	}
	if port := u.Port(); port != "" {
		if number, err := strconv.ParseUint(port, 10, portBitSize); err != nil || number == 0 {
			return nil, fmt.Errorf(invalidEndpointPortFormat, ErrInvalidOptions)
		}
	}
	if cfg.Timeout < 0 {
		return nil, fmt.Errorf(invalidTimeoutFormat, ErrInvalidOptions)
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
	if cfg.Username != "" || cfg.Password != "" {
		request := &http.Request{Header: headers}
		request.SetBasicAuth(cfg.Username, cfg.Password)
	}
	encoded, err := json.Marshal(cfg.Resource)
	if err != nil {
		return nil, ErrInvalidAttributes
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(encoded, &raw) != nil {
		return nil, ErrInvalidAttributes
	}
	resource, err := rawAttributes(raw)
	if err != nil {
		return nil, err
	}
	return &Destination{
		endpoint: cfg.Endpoint, headers: headers,
		resource: resource,
		client: &http.Client{Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}, nil
}

// Send converts completed JSON span records into one OTLP request. Records
// without span or resourceSpans fields are ordinary logs and are skipped.
// Existing OTLP envelopes retain their resourceSpans objects for journal replay.
//
// Each call invokes http.Client.Do at most once. Transient failures return a
// collector.RetryError; malformed requests, invalid acknowledgments, and partial
// rejections are terminal. A nil error acknowledges the entire batch. Errors
// exclude credentials, endpoint URLs, and request or response contents.
// A nil context uses context.Background(); the configured timeout still applies.
func (d *Destination) Send(ctx context.Context, records []json.RawMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	payload, hasSpans, err := mergeRequests(records, d.resource)
	if err != nil {
		return err
	}
	if !hasSpans {
		return nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, bytes.NewReader(payload))
	if err != nil {
		return ErrRequestFailed
	}
	request.Header = d.headers.Clone()
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := d.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return &collector.RetryError{Err: context.DeadlineExceeded}
		}
		return &collector.RetryError{Err: ErrRequestFailed}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf(httpStatusFormat, ErrHTTPStatus, response.StatusCode)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusBadGateway ||
			response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusGatewayTimeout {
			return &collector.RetryError{Err: err, After: retryAfter(response.Header.Get("Retry-After"))}
		}
		return err
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return ErrInvalidResponse
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return &collector.RetryError{Err: context.DeadlineExceeded}
		}
		return &collector.RetryError{Err: ErrRequestFailed}
	}
	if len(body) > maxResponseBytes {
		return ErrInvalidResponse
	}
	return acknowledgment(body)
}

func retryAfter(value string) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
		if seconds > 0 && seconds <= int64(math.MaxInt64/time.Second) {
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	if deadline, err := http.ParseTime(value); err == nil {
		return max(0, time.Until(deadline))
	}
	return 0
}
