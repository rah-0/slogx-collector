package otlp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

func acknowledgment(body []byte) error {
	body = bytes.TrimSpace(body)
	if len(body) == 0 || body[0] != '{' {
		return ErrInvalidResponse
	}
	var response struct {
		PartialSuccess *struct {
			RejectedSpans json.RawMessage `json:"rejectedSpans"`
			ErrorMessage  string          `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	if json.Unmarshal(body, &response) != nil {
		return ErrInvalidResponse
	}
	if response.PartialSuccess == nil {
		return nil
	}
	rejected := bytes.TrimSpace(response.PartialSuccess.RejectedSpans)
	if len(rejected) == 0 || bytes.Equal(rejected, []byte("null")) {
		return nil
	}
	value := string(rejected)
	if rejected[0] == '"' {
		if json.Unmarshal(rejected, &value) != nil {
			return ErrInvalidResponse
		}
	}
	number, ok := decimalInt64(value)
	if !ok || number < 0 {
		return ErrInvalidResponse
	}
	if number > 0 {
		return fmt.Errorf(rejectedSpansFormat, ErrRejectedSpans, number)
	}
	return nil
}

// ProtoJSON accepts integer fields as numbers or strings, including exponents
// such as 1e2: https://protobuf.dev/programming-guides/json/#representation-of-each-type
func decimalInt64(value string) (int64, bool) {
	if number, err := strconv.ParseInt(value, 10, 64); err == nil {
		return number, true
	}
	number, ok := parseDecimal(value)
	if !ok {
		return 0, false
	}
	return number.int64()
}
