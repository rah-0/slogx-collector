package otlp

import "errors"

var (
	ErrAuthenticationConflict = errors.New("collector/otlp: conflicting authentication options")
	// ErrInvalidOptions reports an invalid endpoint or timeout.
	ErrInvalidOptions = errors.New("otlp: invalid options")
	// ErrInvalidRequest reports malformed JSON or a malformed raw export envelope.
	ErrInvalidRequest = errors.New("otlp: invalid export request")
	// ErrInvalidSpan reports invalid metadata in a completed span record.
	ErrInvalidSpan = errors.New("otlp: invalid span")
	// ErrInvalidAttributes reports a resource or attribute encoding failure.
	ErrInvalidAttributes = errors.New("otlp: invalid attributes")
	// ErrRequestFailed reports a sanitized HTTP request or transport failure.
	ErrRequestFailed = errors.New("otlp: request failed")
	// ErrHTTPStatus reports an unsuccessful HTTP response status.
	ErrHTTPStatus = errors.New("otlp: HTTP status")
	// ErrInvalidResponse reports a malformed or oversized export acknowledgment.
	ErrInvalidResponse = errors.New("otlp: invalid export response")
	// ErrRejectedSpans reports a positive rejected span count.
	ErrRejectedSpans = errors.New("otlp: server rejected spans")
)

const (
	invalidEndpointFormat     = "%w: endpoint"
	invalidEndpointPortFormat = "%w: endpoint port"
	invalidTimeoutFormat      = "%w: timeout"
	httpStatusFormat          = "%w %d"
	rejectedSpansFormat       = "%w: %d"
	envelopeErrorFormat       = "collector/otlp: record %d: %w"
)
