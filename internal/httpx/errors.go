package httpx

import "errors"

// Header validation errors exclude names and values to protect credentials.
var (
	ErrInvalidHeaderName  = errors.New("httpx: invalid header name")
	ErrInvalidHeaderValue = errors.New("httpx: invalid header value")
)
