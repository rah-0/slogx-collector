// Package httpx validates configured HTTP headers before delivery starts.
package httpx

import (
	"net/http"
	"strings"
)

const (
	headerValueMinByte    = ' '
	headerValueDeleteByte = 0x7f
)

// ValidateHeaders checks header names and values without modifying headers.
// Errors exclude their contents, which may contain credentials.
func ValidateHeaders(headers http.Header) error {
	for name, values := range headers {
		if name == "" {
			return ErrInvalidHeaderName
		}
		for _, char := range name {
			if !('a' <= char && char <= 'z' || 'A' <= char && char <= 'Z' ||
				'0' <= char && char <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", char)) {
				return ErrInvalidHeaderName
			}
		}
		for _, value := range values {
			for i := range len(value) {
				char := value[i]
				if char == headerValueDeleteByte || char < headerValueMinByte && char != '\t' {
					return ErrInvalidHeaderValue
				}
			}
		}
	}
	return nil
}
