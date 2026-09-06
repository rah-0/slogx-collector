package otlp

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// mergeRequests converts completed span records after the journal has stored
// their original JSON. Existing OTLP envelopes retain each resourceSpans object's
// JSON representation; other fields on the outer envelope are not copied.
func mergeRequests(records []json.RawMessage, resource []KeyValue) ([]byte, bool, error) {
	var body bytes.Buffer
	body.WriteString(`{"resourceSpans":[`)
	first, hasSpans := true, false
	var spans []WireSpan
	for i, record := range records {
		var fields map[string]json.RawMessage
		if json.Unmarshal(record, &fields) != nil || fields == nil {
			return nil, false, fmt.Errorf(envelopeErrorFormat, i+1, ErrInvalidRequest)
		}
		if raw, ok := fields["span"]; ok {
			delete(fields, "span")
			span, err := recordSpan(raw, fields)
			if err != nil {
				return nil, false, fmt.Errorf(envelopeErrorFormat, i+1, err)
			}
			spans = append(spans, span)
			continue
		}
		if _, envelope := fields["resourceSpans"]; !envelope {
			continue
		}
		resources, nonempty, err := requestResources(record)
		if err != nil {
			return nil, false, fmt.Errorf(envelopeErrorFormat, i+1, err)
		}
		hasSpans = hasSpans || nonempty
		for _, resource := range resources {
			if !first {
				body.WriteByte(',')
			}
			body.Write(resource)
			first = false
		}
	}
	if len(spans) > 0 {
		encoded, err := json.Marshal(ResourceSpans{
			Resource:   Resource{Attributes: resource},
			ScopeSpans: []ScopeSpans{{Scope: Scope{Name: "github.com/rah-0/slogx"}, Spans: spans}},
		})
		if err != nil {
			return nil, false, ErrInvalidAttributes
		}
		if !first {
			body.WriteByte(',')
		}
		body.Write(encoded)
		hasSpans = true
	}
	body.WriteString(`]}`)
	return body.Bytes(), hasSpans, nil
}

func requestResources(record json.RawMessage) ([]json.RawMessage, bool, error) {
	resources, err := requestArray(record, "resourceSpans")
	if err != nil {
		return nil, false, err
	}
	hasSpans := false
	for _, resource := range resources {
		scopes, err := requestArray(resource, "scopeSpans")
		if err != nil {
			return nil, false, err
		}
		for _, scope := range scopes {
			spans, err := requestArray(scope, "spans")
			if err != nil {
				return nil, false, err
			}
			for _, span := range spans {
				if trimmed := bytes.TrimSpace(span); len(trimmed) == 0 || trimmed[0] != '{' {
					return nil, false, ErrInvalidRequest
				}
			}
			hasSpans = hasSpans || len(spans) > 0
		}
	}
	return resources, hasSpans, nil
}

// Hierarchy fields must be explicit arrays without duplicate names. The outer
// envelope's JSON syntax is validated before inspecting its nested objects.
func requestArray(object []byte, field string) ([]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(object))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, ErrInvalidRequest
	}
	var result []json.RawMessage
	found := false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalidRequest
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, ErrInvalidRequest
		}
		if key != field {
			continue
		}
		if found {
			return nil, ErrInvalidRequest
		}
		found = true
		trimmed := bytes.TrimSpace(value)
		if len(trimmed) == 0 || trimmed[0] != '[' || json.Unmarshal(value, &result) != nil {
			return nil, ErrInvalidRequest
		}
	}
	if !found {
		return nil, ErrInvalidRequest
	}
	return result, nil
}
