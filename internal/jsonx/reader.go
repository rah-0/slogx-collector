// Package jsonx reads JSON objects and adds configured fields.
package jsonx

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Reader reads one JSON object per physical line.
// A final object without a trailing newline is accepted.
type Reader struct {
	reader *bufio.Reader
	line   int
}

// NewReader returns a reader for newline-delimited JSON objects.
func NewReader(input io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(input)}
}

// Next returns the next JSON object, skipping blank lines and trimming JSON whitespace.
// It returns io.EOF after the last record and omits record contents and underlying
// reader error text from error messages.
func (r *Reader) Next() (json.RawMessage, error) {
	for {
		line, err := r.reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			// Do not expose the reader's error text: *os.PathError includes its path.
			return nil, fmt.Errorf("jsonx: input line %d: %w", r.line+1, ErrRead)
		}
		if len(line) == 0 && errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		r.line++
		line = bytes.Trim(line, " \t\r\n")
		if len(line) == 0 {
			continue
		}
		if line[0] != '{' || !json.Valid(line) {
			return nil, fmt.Errorf("jsonx: input line %d: %w", r.line, ErrInvalidRecord)
		}
		return line, nil
	}
}
