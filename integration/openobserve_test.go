package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/rah-0/slogx-collector/collector"
	"github.com/rah-0/slogx-collector/collector/openobserve"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const openObserveImage = "o2cr.ai/openobserve/openobserve:v0.92.2"

type openObserveServer struct {
	url, username, password string
	client                  *http.Client
}

func TestOpenObserve(t *testing.T) {
	server := startOpenObserve(t)
	t.Run("ingestion and timestamps", func(t *testing.T) { testIngestion(t, server) })
	t.Run("authentication failure", func(t *testing.T) {
		destination, err := openobserve.New(openobserve.Config{
			Endpoint: server.url + "/api/default/authentication/_json",
			Username: server.username,
			Password: "incorrect-integration-password",
		})
		if err != nil {
			t.Fatal(err)
		}
		err = destination.Send(t.Context(), []json.RawMessage{json.RawMessage(`{"msg":"unauthorized"}`)})
		if _, retry := errors.AsType[*collector.RetryError](err); !errors.Is(err, openobserve.ErrHTTPStatus) || retry {
			t.Fatalf("wrong-password delivery = %v; want terminal HTTP error", err)
		}
	})
	t.Run("partial rejection", func(t *testing.T) {
		destination := server.destination(t, "partial_rejection")
		now := time.Now().UTC().Truncate(time.Microsecond)
		err := destination.Send(t.Context(), []json.RawMessage{
			json.RawMessage(fmt.Sprintf(`{"_timestamp":%d,"msg":"accepted"}`, now.UnixMicro())),
			json.RawMessage(fmt.Sprintf(`{"_timestamp":%d,"msg":"too old"}`, now.Add(-48*time.Hour).UnixMicro())),
		})
		if _, retry := errors.AsType[*collector.RetryError](err); !errors.Is(err, openobserve.ErrRejectedRecords) || retry {
			t.Fatalf("partially accepted batch = %v; want terminal ErrRejectedRecords", err)
		}
		var got struct {
			Message string `json:"msg"`
		}
		if err := json.Unmarshal(server.hits(t, "partial_rejection", 1)[0], &got); err != nil {
			t.Fatal(err)
		}
		if got.Message != "accepted" {
			t.Fatalf("queried message = %q; want accepted", got.Message)
		}
	})
	t.Run("collector command", func(t *testing.T) { testCollectorCommand(t, server) })
}

func startOpenObserve(t *testing.T) openObserveServer {
	t.Helper()
	server := openObserveServer{
		username: "collector@example.com",
		password: "Disposable-integration-password-1!",
		client:   &http.Client{Timeout: 10 * time.Second},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	container, err := testcontainers.Run(ctx, openObserveImage,
		testcontainers.WithExposedPorts("5080/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ZO_LOCAL_MODE":           "true",
			"ZO_DATA_DIR":             "/data",
			"ZO_ROOT_USER_EMAIL":      server.username,
			"ZO_ROOT_USER_PASSWORD":   server.password,
			"ZO_TELEMETRY":            "false",
			"ZO_RESULT_CACHE_ENABLED": "false",
		}),
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForHTTP("/healthz").WithPort("5080/tcp")),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start OpenObserve (Docker is required): %v", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5080/tcp")
	if err != nil {
		t.Fatal(err)
	}
	server.url = "http://" + net.JoinHostPort(host, port.Port())
	t.Cleanup(server.client.CloseIdleConnections)
	return server
}

func (s openObserveServer) destination(t *testing.T, stream string) *openobserve.Destination {
	t.Helper()
	destination, err := openobserve.New(openobserve.Config{
		Endpoint:       s.url + "/api/default/" + stream + "/_json",
		Username:       s.username,
		Password:       s.password,
		TimestampField: "time",
	})
	if err != nil {
		t.Fatal(err)
	}
	return destination
}

// hits waits for newly ingested records to become searchable. The wide range also
// includes the deliberately old record used to exercise partial rejection.
func (s openObserveServer) hits(t *testing.T, stream string, count int) []json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	now := time.Now()
	payload, err := json.Marshal(map[string]any{
		"query": map[string]any{
			"sql":        fmt.Sprintf(`SELECT * FROM "%s" ORDER BY _timestamp`, stream),
			"start_time": now.Add(-72 * time.Hour).UnixMicro(),
			"end_time":   now.Add(time.Hour).UnixMicro(),
			"from":       0,
			"size":       count + 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/api/default/_search?type=logs", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth(s.username, s.password)
		response, err := s.client.Do(req)
		if err != nil {
			last = err.Error()
		} else {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			var result struct {
				Hits []json.RawMessage `json:"hits"`
			}
			switch {
			case readErr != nil:
				last = readErr.Error()
			case response.StatusCode != http.StatusOK:
				last = fmt.Sprintf("HTTP %d: %s", response.StatusCode, body)
			case json.Unmarshal(body, &result) != nil:
				t.Fatalf("invalid search response: %s", body)
			case len(result.Hits) > count:
				t.Fatalf("stream %s has more than %d records: %s", stream, count, body)
			case len(result.Hits) == count:
				return result.Hits
			default:
				last = fmt.Sprintf("found %d of %d records", len(result.Hits), count)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("query stream %s: %v; last response: %s", stream, ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func testIngestion(t *testing.T, server openObserveServer) {
	t.Helper()
	first := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
	second := first.Add(time.Microsecond)
	third := second.Add(time.Microsecond)
	records := []json.RawMessage{
		json.RawMessage(fmt.Sprintf(`{"time":%q,"msg":"slogx","n":9007199254740993,"http":{"status":201}}`, first.Format("2006-01-02 15:04:05.000000"))),
		json.RawMessage(fmt.Sprintf(`{"time":%q,"msg":"slog"}`, second.Format(time.RFC3339Nano))),
		json.RawMessage(fmt.Sprintf(`{"time":"ignored","_timestamp":%d,"msg":"explicit"}`, third.UnixMicro())),
		json.RawMessage(`{"msg":"generic","enabled":true}`),
	}
	destination := server.destination(t, "ingestion")
	if err := destination.Send(t.Context(), records); err != nil {
		t.Fatalf("send JSON batch: %v", err)
	}
	timestamps := map[string]int64{"slogx": first.UnixMicro(), "slog": second.UnixMicro(), "explicit": third.UnixMicro()}
	seen := make(map[string]bool)
	for _, raw := range server.hits(t, "ingestion", len(records)) {
		var got struct {
			Message   string      `json:"msg"`
			Timestamp int64       `json:"_timestamp"`
			Number    json.Number `json:"n"`
			Status    int         `json:"http_status"`
			Enabled   bool        `json:"enabled"`
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if seen[got.Message] {
			t.Fatalf("duplicate message: %q", got.Message)
		}
		seen[got.Message] = true
		if want, ok := timestamps[got.Message]; ok {
			if got.Timestamp != want {
				t.Errorf("%s timestamp = %d; want %d", got.Message, got.Timestamp, want)
			}
		} else if got.Message != "generic" {
			t.Fatalf("unexpected message: %q", got.Message)
		}
		if got.Message == "slogx" && (got.Number != "9007199254740993" || got.Status != 201) {
			t.Errorf("integer or nested field changed: %s", raw)
		}
		if got.Message == "generic" && (!got.Enabled || got.Timestamp < first.UnixMicro() || got.Timestamp > time.Now().UnixMicro()) {
			t.Errorf("generic record missing fields or ingestion timestamp: %s", raw)
		}
	}
}
