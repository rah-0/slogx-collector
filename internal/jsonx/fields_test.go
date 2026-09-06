package jsonx

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestAddFieldsOverridesInputValues(t *testing.T) {
	for _, input := range []string{
		`{}`,
		`{"version":"from input"}`,
		`{"version":42}`,
		`{"version":{"nested":true}}`,
		`{"version":null}`,
		`{"version":"first","versi\u006fn":"second"}`,
	} {
		t.Run(input, func(t *testing.T) {
			fields := EncodeFields(map[string]string{"version": "configured"})
			record, err := AddFields(json.RawMessage(input), fields)
			if err != nil {
				t.Fatal(err)
			}
			// Only one key may remain, including when input uses duplicate or
			// escaped spellings of the configured name.
			if want := `{"version":"configured"}`; string(record) != want {
				t.Fatalf("record = %s, want %s", record, want)
			}
		})
	}
}

func TestAddFieldsPreservesJSONValues(t *testing.T) {
	const input = `{"version":42,"n":9007199254740993123456789,"decimal":1.234567890123456789,"nested":{"version":"nested","n":9007199254740993},"Version":"capital"}`
	fields := map[string]string{
		"version":        "v1",
		"build\"tag\\路径": "line\n=\"β\"",
		"empty":          "",
	}
	encoded := EncodeFields(fields)
	record, err := AddFields(json.RawMessage(input), encoded)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(record, &object); err != nil {
		t.Fatal(err)
	}
	if len(object) != 7 {
		t.Fatalf("object = %s, want seven fields", record)
	}
	for key, want := range map[string]string{
		"n":       "9007199254740993123456789",
		"decimal": "1.234567890123456789",
		"nested":  `{"version":"nested","n":9007199254740993}`,
		"Version": `"capital"`,
	} {
		if got := string(object[key]); got != want {
			t.Errorf("%q = %s, want %s", key, got, want)
		}
	}
	for key, want := range fields {
		var got string
		if err := json.Unmarshal(object[key], &got); err != nil {
			t.Fatalf("decode field %q: %v", key, err)
		}
		if got != want {
			t.Errorf("%q = %q, want %q", key, got, want)
		}
	}
}

func TestEncodeFieldsEmpty(t *testing.T) {
	for _, fields := range []map[string]string{nil, {}} {
		if encoded := EncodeFields(fields); encoded != nil {
			t.Fatalf("EncodeFields(%v) = %v; want nil", fields, encoded)
		}
	}
}

func TestAddFieldsErrors(t *testing.T) {
	if _, err := AddFields(json.RawMessage(`{"secret":`), nil); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("invalid record = %v, want ErrInvalidRecord", err)
	}
	if _, err := AddFields(json.RawMessage(`{}`), map[string]json.RawMessage{"value": json.RawMessage(`invalid`)}); !errors.Is(err, ErrFields) {
		t.Fatalf("invalid field = %v, want ErrFields", err)
	}
}
