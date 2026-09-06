package otlp

type ExportRequest struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans"`
}

type ResourceSpans struct {
	Resource   Resource     `json:"resource"`
	ScopeSpans []ScopeSpans `json:"scopeSpans"`
}

type Resource struct {
	Attributes []KeyValue `json:"attributes,omitempty"`
}

type ScopeSpans struct {
	Scope Scope      `json:"scope"`
	Spans []WireSpan `json:"spans"`
}

type Scope struct {
	Name string `json:"name"`
}

type WireSpan struct {
	TraceID           string      `json:"traceId"`
	SpanID            string      `json:"spanId"`
	TraceState        string      `json:"traceState,omitempty"`
	ParentSpanID      string      `json:"parentSpanId,omitempty"`
	Flags             uint32      `json:"flags,omitempty"`
	Name              string      `json:"name"`
	Kind              int         `json:"kind"`
	StartTimeUnixNano string      `json:"startTimeUnixNano"`
	EndTimeUnixNano   string      `json:"endTimeUnixNano"`
	Attributes        []KeyValue  `json:"attributes,omitempty"`
	Events            []WireEvent `json:"events,omitempty"`
	Status            WireStatus  `json:"status"`
}

type WireEvent struct {
	TimeUnixNano string     `json:"timeUnixNano"`
	Name         string     `json:"name"`
	Attributes   []KeyValue `json:"attributes,omitempty"`
}

type WireStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}
