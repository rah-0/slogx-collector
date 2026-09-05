package collector

import "encoding/json"

func encodeFields(fields map[string]string) (map[string]json.RawMessage, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	encoded := make(map[string]json.RawMessage, len(fields))
	for name, value := range fields {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, ErrRecordFields
		}
		encoded[name] = data
	}
	return encoded, nil
}

// addFields merges into an input object already validated by objectReader.
func addFields(record json.RawMessage, fields map[string]json.RawMessage) (json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(record, &object); err != nil {
		return nil, ErrInvalidJSONRecord
	}
	for name, value := range fields {
		object[name] = value
	}
	data, err := json.Marshal(object)
	if err != nil {
		return nil, ErrRecordFields
	}
	return data, nil
}
