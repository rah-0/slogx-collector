package jsonx

import "encoding/json"

// EncodeFields encodes string field values as JSON.
// It returns nil for an empty map.
func EncodeFields(fields map[string]string) map[string]json.RawMessage {
	if len(fields) == 0 {
		return nil
	}
	encoded := make(map[string]json.RawMessage, len(fields))
	for name, value := range fields {
		data, _ := json.Marshal(value) // Marshaling a string cannot fail.
		encoded[name] = data
	}
	return encoded
}

// AddFields merges fields into a JSON object, replacing existing values by name.
// The record must be a valid JSON object, as checked by Reader.
// Unconfigured values retain number precision and nested structure.
// Top-level duplicate names use the last value.
func AddFields(record json.RawMessage, fields map[string]json.RawMessage) (json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(record, &object); err != nil {
		return nil, ErrInvalidRecord
	}
	for name, value := range fields {
		object[name] = value
	}
	data, err := json.Marshal(object)
	if err != nil {
		return nil, ErrFields
	}
	return data, nil
}
