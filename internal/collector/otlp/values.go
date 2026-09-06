package otlp

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
)

type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

type AnyValue struct {
	StringValue *string         `json:"stringValue,omitempty"`
	BoolValue   *bool           `json:"boolValue,omitempty"`
	IntValue    *string         `json:"intValue,omitempty"`
	DoubleValue json.RawMessage `json:"doubleValue,omitempty"`
	ArrayValue  *ArrayValue     `json:"arrayValue,omitempty"`
	KVListValue *KeyValueList   `json:"kvlistValue,omitempty"`
}

type ArrayValue struct {
	Values []AnyValue `json:"values"`
}

type KeyValueList struct {
	Values []KeyValue `json:"values"`
}

// UseNumber preserves integer tokens instead of decoding them through float64.
func rawAttributes(attrs map[string]json.RawMessage) ([]KeyValue, error) {
	values := make(map[string]any, len(attrs))
	for key, raw := range attrs {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if decoder.Decode(&value) != nil {
			return nil, ErrInvalidAttributes
		}
		values[key] = value
	}
	return jsonAttributes(values)
}

func jsonAttributes(attrs map[string]any) ([]KeyValue, error) {
	keys := make([]string, 0, len(attrs))
	for key := range attrs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]KeyValue, len(keys))
	for i, key := range keys {
		value, err := jsonValue(attrs[key])
		if err != nil {
			return nil, err
		}
		values[i] = KeyValue{Key: key, Value: value}
	}
	return values, nil
}

func jsonValue(value any) (AnyValue, error) {
	switch value := value.(type) {
	case nil:
		return AnyValue{}, nil
	case bool:
		return AnyValue{BoolValue: new(value)}, nil
	case string:
		return AnyValue{StringValue: new(value)}, nil
	case json.Number:
		number, ok := parseDecimal(string(value))
		if !ok {
			return AnyValue{}, ErrInvalidAttributes
		}
		if integer, ok := number.int64(); ok {
			return AnyValue{IntValue: new(strconv.FormatInt(integer, 10))}, nil
		}
		floating, err := value.Float64()
		if number.Scale >= 0 || err != nil || floating == 0 {
			// Keep out-of-int64 integers and fractions that overflow float64 or
			// underflow to zero as text, such as 9223372036854775808 and 1e-400.
			return AnyValue{StringValue: new(string(value))}, nil
		}
		return AnyValue{DoubleValue: json.RawMessage(value)}, nil
	case []any:
		values := make([]AnyValue, len(value))
		for i, item := range value {
			encoded, err := jsonValue(item)
			if err != nil {
				return AnyValue{}, err
			}
			values[i] = encoded
		}
		return AnyValue{ArrayValue: &ArrayValue{Values: values}}, nil
	case map[string]any:
		values, err := jsonAttributes(value)
		return AnyValue{KVListValue: &KeyValueList{Values: values}}, err
	default:
		return AnyValue{}, ErrInvalidAttributes
	}
}
