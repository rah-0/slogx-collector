package openobserve

import "errors"

// Sentinel errors may be wrapped with a record index or collector.RetryError.
// Use errors.Is to identify them.
var (
	ErrInvalidEndpoint           = errors.New("openobserve: endpoint must be an absolute HTTP(S) URL without user information or fragment")
	ErrNegativeTimeout           = errors.New("openobserve: timeout must not be negative")
	ErrAuthenticationConflict    = errors.New("openobserve: use either Basic credentials or an Authorization header")
	ErrRequestCreation           = errors.New("openobserve: cannot create HTTP request")
	ErrRequestFailed             = errors.New("openobserve: HTTP request failed")
	ErrHTTPStatus                = errors.New("openobserve: HTTP status")
	ErrReadAcknowledgment        = errors.New("openobserve: cannot read ingestion acknowledgment")
	ErrInvalidAcknowledgment     = errors.New("openobserve: invalid ingestion acknowledgment")
	ErrRejectedRecords           = errors.New("openobserve: ingestion rejected records; partial batches cannot be retried safely")
	ErrTemporaryIngestionFailure = errors.New("openobserve: ingestion acknowledgment reports a temporary failure")
	ErrIngestionFailure          = errors.New("openobserve: ingestion acknowledgment reports failure")
	ErrMissingRecordCounts       = errors.New("openobserve: ingestion acknowledgment has no record counts")
	ErrInvalidRecordCounts       = errors.New("openobserve: ingestion acknowledgment has invalid record counts")
	ErrRecordCountMismatch       = errors.New("openobserve: ingestion acknowledgment does not match the submitted record count")
	ErrInvalidJSONObject         = errors.New("expected a valid JSON object")
	ErrInvalidTimestampType      = errors.New("timestamp source must be a time string or integer epoch microseconds")
	ErrTimestampLayoutMismatch   = errors.New("timestamp source does not match the configured time layout")
)
