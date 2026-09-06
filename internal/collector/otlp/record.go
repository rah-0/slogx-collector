package otlp

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"time"
)

const (
	traceIDBytes = 16
	spanIDBytes  = 8

	spanKindInternal = 1
	spanKindConsumer = 5
	spanStatusUnset  = 0
	spanStatusError  = 2
)

// SpanRecord describes the reserved span group in a completed JSON log record.
// Other root fields are attributes, including the logger's native source field.
type SpanRecord struct {
	TraceID       string               `json:"trace_id"`
	SpanID        string               `json:"span_id"`
	ParentSpanID  string               `json:"parent_span_id"`
	TraceFlags    uint8                `json:"trace_flags"`
	TraceState    string               `json:"trace_state"`
	Name          string               `json:"name"`
	Kind          int                  `json:"kind"`
	StartTime     time.Time            `json:"start_time"`
	EndTime       time.Time            `json:"end_time"`
	Status        int                  `json:"status"`
	StatusMessage string               `json:"status_message"`
	Events        map[string]SpanEvent `json:"events"`
}

type SpanEvent struct {
	Time       time.Time      `json:"time"`
	Message    string         `json:"msg"`
	Attributes map[string]any `json:"attrs"`
}

func recordSpan(raw json.RawMessage, attrs map[string]json.RawMessage) (WireSpan, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var span SpanRecord
	if decoder.Decode(&span) != nil {
		return WireSpan{}, ErrInvalidSpan
	}
	start, startOK := timestamp(span.StartTime)
	end, endOK := timestamp(span.EndTime)
	if !validID(span.TraceID, traceIDBytes) || !validID(span.SpanID, spanIDBytes) ||
		(span.ParentSpanID != "" && !validID(span.ParentSpanID, spanIDBytes)) || span.Name == "" ||
		span.Kind < spanKindInternal || span.Kind > spanKindConsumer ||
		span.Status < spanStatusUnset || span.Status > spanStatusError ||
		!startOK || !endOK || span.EndTime.Before(span.StartTime) {
		return WireSpan{}, ErrInvalidSpan
	}
	attributes, err := rawAttributes(attrs)
	if err != nil {
		return WireSpan{}, err
	}
	// Numeric keys retain the event sequence even when redaction removes one.
	indexes := make([]int, 0, len(span.Events))
	for key := range span.Events {
		index, err := strconv.Atoi(key)
		if err != nil || index < 0 || strconv.Itoa(index) != key {
			return WireSpan{}, ErrInvalidSpan
		}
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	events := make([]WireEvent, len(indexes))
	for i, index := range indexes {
		event := span.Events[strconv.Itoa(index)]
		instant, ok := timestamp(event.Time)
		if !ok || event.Message == "" {
			return WireSpan{}, ErrInvalidSpan
		}
		attrs, err := jsonAttributes(event.Attributes)
		if err != nil {
			return WireSpan{}, err
		}
		events[i] = WireEvent{TimeUnixNano: instant, Name: event.Message, Attributes: attrs}
	}
	return WireSpan{
		TraceID: span.TraceID, SpanID: span.SpanID, ParentSpanID: span.ParentSpanID,
		Flags: uint32(span.TraceFlags), TraceState: span.TraceState,
		Name: span.Name, Kind: span.Kind, StartTimeUnixNano: start, EndTimeUnixNano: end,
		Attributes: attributes, Events: events,
		Status: WireStatus{Code: span.Status, Message: span.StatusMessage},
	}, nil
}

func validID(id string, size int) bool {
	if len(id) != hex.EncodedLen(size) {
		return false
	}
	decoded, err := hex.DecodeString(id)
	if err != nil {
		return false
	}
	for _, value := range decoded {
		if value != 0 {
			return true
		}
	}
	return false
}

// OTLP timestamps are uint64 nanoseconds. UnixNano's int64 result is undefined
// after 2262-04-11T23:47:16.854775807Z, which is still within OTLP's range.
func timestamp(instant time.Time) (string, bool) {
	seconds := instant.Unix()
	nanos := uint64(instant.Nanosecond())
	if seconds < 0 || uint64(seconds) > (math.MaxUint64-nanos)/uint64(time.Second) {
		return "", false
	}
	return strconv.FormatUint(uint64(seconds)*uint64(time.Second)+nanos, 10), true
}
