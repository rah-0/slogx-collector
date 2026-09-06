package otlp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rah-0/slogx-collector/examples/internal/exampletest"
)

type TraceSpan struct {
	TraceID      string `json:"traceId"`
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId"`
	Name         string `json:"name"`
}

type TraceRequest struct {
	ResourceSpans []struct {
		Resource struct {
			Attributes []struct {
				Key   string `json:"key"`
				Value struct {
					String string `json:"stringValue"`
				} `json:"value"`
			} `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []struct {
			Spans []TraceSpan `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"resourceSpans"`
}

func TestScriptConvertsSpansAndSkipsOrdinaryLogs(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("the example script requires bash")
	}
	binary := exampletest.BuildCollector(t)
	var mu sync.Mutex
	var spans []TraceSpan
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/traces" || request.Method != http.MethodPost ||
			request.Header.Get("Authorization") != "Bearer fixture-token" ||
			request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Stream-Name") != "" {
			t.Error("request does not match the generic OTLP endpoint and forwarded header")
		}
		var payload TraceRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Error(err)
			http.Error(writer, "invalid payload", http.StatusBadRequest)
			return
		}
		for _, resource := range payload.ResourceSpans {
			found := false
			for _, attr := range resource.Resource.Attributes {
				if attr.Key == "service.name" && attr.Value.String == "catalog" {
					found = true
				}
			}
			if !found {
				t.Error("configured service.name was not included in trace resources")
			}
			for _, scope := range resource.ScopeSpans {
				mu.Lock()
				spans = append(spans, scope.Spans...)
				mu.Unlock()
			}
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprint(writer, `{}`)
	}))
	defer server.Close()

	script, err := filepath.Abs("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", script,
		"-header-env", "Authorization=EXAMPLE_AUTHORIZATION",
		"-max-retries", "1", "-retry-interval", "1ms", "-max-retry-interval", "1ms",
		"-request-timeout", "2s",
	)
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(),
		"COLLECTOR_BIN="+binary,
		"JOURNAL_DIR="+t.TempDir(),
		"TRACE_ENDPOINT="+server.URL+"/v1/traces",
		"EXAMPLE_AUTHORIZATION=Bearer fixture-token",
	)
	command.WaitDelay = 2 * time.Second
	if output, err := command.CombinedOutput(); err != nil || len(output) != 0 {
		t.Fatalf("script: %v\n%s", err, output)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(spans) != 3 {
		t.Fatalf("delivered %d spans, want 3 completed spans and no ordinary logs", len(spans))
	}
	byName := make(map[string]TraceSpan)
	for _, span := range spans {
		if _, duplicate := byName[span.Name]; duplicate {
			t.Fatalf("duplicate span %q", span.Name)
		}
		byName[span.Name] = span
	}
	traceID := byName["handle request"].TraceID
	var parentID string
	for _, name := range []string{"handle request", "load item", "query item"} {
		span := byName[name]
		if len(traceID) != 32 || span.TraceID != traceID || len(span.SpanID) != 16 || span.ParentSpanID != parentID {
			t.Fatalf("span %q lost its trace identity or parent: %+v", name, span)
		}
		parentID = span.SpanID
	}
}
