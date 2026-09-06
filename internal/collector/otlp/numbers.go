package otlp

import (
	"encoding/json"
	"strconv"
	"strings"
)

const maxInt64DecimalDigits = 19

// DecimalNumber stores normalized digits, sign, and scale for int64 checks.
// An empty digit string is zero. parseDecimal clamps exponents that cannot yield
// an int64, so Scale need not preserve their original magnitude.
type DecimalNumber struct {
	Digits   string
	Scale    int64
	Negative bool
}

func parseDecimal(value string) (DecimalNumber, bool) {
	if value == "" || strings.TrimSpace(value) != value ||
		(value[0] != '-' && (value[0] < '0' || value[0] > '9')) || !json.Valid([]byte(value)) {
		return DecimalNumber{}, false
	}
	limit := int64(len(value)) + maxInt64DecimalDigits + 1
	negative := value[0] == '-'
	if negative {
		value = value[1:]
	}
	exponentText := "0"
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		value, exponentText = value[:index], value[index+1:]
	}
	whole, fraction, _ := strings.Cut(value, ".")
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		return DecimalNumber{}, true
	}
	trimmed := strings.TrimRight(digits, "0")
	exponent, err := strconv.ParseInt(exponentText, 10, 64)
	if err != nil {
		// Syntax is already validated. Such an exponent cannot be canceled by
		// the mantissa sufficiently to produce a signed 64-bit integer.
		exponent = limit
		if exponentText[0] == '-' {
			exponent = -limit
		}
	}
	// The mantissa shifts the scale by at most the input length, and int64 has
	// at most 19 decimal digits. Larger exponents cannot change the int64 result.
	exponent = min(max(exponent, -limit), limit)
	return DecimalNumber{
		Digits:   trimmed,
		Scale:    exponent - int64(len(fraction)) + int64(len(digits)-len(trimmed)),
		Negative: negative,
	}, true
}

func (number DecimalNumber) int64() (int64, bool) {
	if number.Digits == "" {
		return 0, true
	}
	if number.Scale < 0 || int64(len(number.Digits))+number.Scale > maxInt64DecimalDigits {
		return 0, false
	}
	sign := ""
	if number.Negative {
		sign = "-"
	}
	value, err := strconv.ParseInt(sign+number.Digits+strings.Repeat("0", int(number.Scale)), 10, 64)
	return value, err == nil
}
