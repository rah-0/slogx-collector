// Package collector forwards newline-delimited JSON objects to a destination.
// It does not require slog-specific fields or configure the application's logger.
package collector

import (
	"context"
	"encoding/json"
)

// Destination delivers a batch of JSON objects. Send must honor ctx and must not
// modify or retain records after returning. A nil error acknowledges the entire
// batch. Return a RetryError for transient failures; other errors stop collection.
// A retried batch may contain records already accepted by the destination.
type Destination interface {
	Send(ctx context.Context, records []json.RawMessage) error
}
