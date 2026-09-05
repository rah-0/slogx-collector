package collector

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// objectReader reads one JSON object per physical line.
// A final object without a trailing newline is accepted.
type objectReader struct {
	reader *bufio.Reader
	line   int
}

func newObjectReader(input io.Reader) *objectReader {
	return &objectReader{reader: bufio.NewReader(input)}
}

func (r *objectReader) next() (json.RawMessage, error) {
	for {
		line, err := r.reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			// Reader errors may contain sensitive source names or input fragments.
			return nil, fmt.Errorf("collector: input line %d: %w", r.line+1, ErrInputRead)
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
			return nil, fmt.Errorf("collector: input line %d: %w", r.line, ErrInvalidJSONRecord)
		}
		return line, nil
	}
}
