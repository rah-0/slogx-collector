package httpx

import (
	"errors"
	"net/http"
	"testing"
)

func TestValidateHeaders(t *testing.T) {
	for _, test := range []struct {
		name    string
		headers http.Header
		want    error
	}{
		{name: "nil"},
		{name: "empty", headers: http.Header{}},
		{name: "token characters", headers: http.Header{"AZaz09!#$%&'*+-.^_`|~": {"value"}}},
		{name: "multiple values", headers: http.Header{"X-Example": {"", "text\t with spaces", "café"}}},
		{name: "no values", headers: http.Header{"X-Example": nil}},
		{name: "empty name", headers: http.Header{"": {"value"}}, want: ErrInvalidHeaderName},
		{name: "space in name", headers: http.Header{"X Example": {"value"}}, want: ErrInvalidHeaderName},
		{name: "colon in name", headers: http.Header{"X:Example": {"value"}}, want: ErrInvalidHeaderName},
		{name: "non ASCII name", headers: http.Header{"X-Café": {"value"}}, want: ErrInvalidHeaderName},
		{name: "invalid name without values", headers: http.Header{"X Example": nil}, want: ErrInvalidHeaderName},
		{name: "carriage return", headers: http.Header{"X-Example": {"secret\rvalue"}}, want: ErrInvalidHeaderValue},
		{name: "line feed", headers: http.Header{"X-Example": {"secret\nvalue"}}, want: ErrInvalidHeaderValue},
		{name: "NUL", headers: http.Header{"X-Example": {"secret\x00value"}}, want: ErrInvalidHeaderValue},
		{name: "control byte", headers: http.Header{"X-Example": {"secret\x1fvalue"}}, want: ErrInvalidHeaderValue},
		{name: "DEL", headers: http.Header{"X-Example": {"secret\x7fvalue"}}, want: ErrInvalidHeaderValue},
		{name: "invalid second value", headers: http.Header{"X-Example": {"valid", "secret\nvalue"}}, want: ErrInvalidHeaderValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateHeaders(test.headers); !errors.Is(err, test.want) {
				t.Fatalf("ValidateHeaders = %v, want %v", err, test.want)
			}
		})
	}
}
