package jsonx

import "errors"

// Record processing errors.
var (
	ErrInvalidRecord = errors.New("record must contain one complete JSON object")
	ErrRead          = errors.New("input read failure")
	ErrFields        = errors.New("jsonx: encode record fields")
)
