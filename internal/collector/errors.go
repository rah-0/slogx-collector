package collector

import (
	"errors"
	"time"
)

// Option validation errors.
var (
	ErrNegativeOption          = errors.New("collector: limits and durations must not be negative")
	ErrRetryIntervalExceedsMax = errors.New("collector: retry interval must not exceed maximum retry interval")
	ErrInputRequired           = errors.New("collector: input is required")
	ErrDestinationRequired     = errors.New("collector: destination is required")
)

// ErrTemporaryDelivery is the default cause of a RetryError without an Err.
var ErrTemporaryDelivery = errors.New("collector: temporary delivery failure")

// RetryError reports a transient delivery failure. After optionally specifies a
// minimum delay before the next attempt. The collector also applies its backoff.
type RetryError struct {
	Err   error
	After time.Duration
}

func (e *RetryError) Error() string {
	return e.Unwrap().Error()
}

func (e *RetryError) Unwrap() error {
	if e.Err == nil {
		return ErrTemporaryDelivery
	}
	return e.Err
}

// terminalError prevents cancellation or an input failure from causing a
// shutdown retry after delivery or journal operations have already failed.
type terminalError struct{ error }

func (e *terminalError) Unwrap() error { return e.error }

// A joined error can contain both cancellation and a permanent failure. Only
// errors whose complete cause is cancellation permit resuming an interrupted send.
func cancellationOnly(err, cancellation error) bool {
	if err == cancellation {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return cancellationOnly(wrapped.Unwrap(), cancellation)
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !cancellationOnly(cause, cancellation) {
				return false
			}
		}
		return true
	}
	return false
}
