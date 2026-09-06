package cli

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/jsonx"
)

// routingDestination partitions a journal batch without changing its records.
// The batch is acknowledged only after both destinations accept their records.
// A retry can redeliver the successful partition, following at-least-once delivery.
type routingDestination struct {
	logs   collector.Destination
	traces collector.Destination
}

func (cfg *commandConfig) newRoutingDestination(logs collector.Destination, headers http.Header) (collector.Destination, error) {
	if cfg.tracesEndpoint == "" {
		return logs, nil
	}
	traces, err := cfg.newOTLPDestination(cfg.tracesEndpoint, headers)
	if err != nil {
		return nil, err
	}
	return routingDestination{logs: logs, traces: traces}, nil
}

func (d routingDestination) Send(ctx context.Context, records []json.RawMessage) error {
	var logs, traces []json.RawMessage
	for _, record := range records {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(record, &fields); err != nil || fields == nil {
			return jsonx.ErrInvalidRecord
		}
		_, span := fields["span"]
		_, envelope := fields["resourceSpans"]
		if span || envelope {
			traces = append(traces, record)
		} else {
			logs = append(logs, record)
		}
	}
	if len(traces) != 0 {
		if err := d.traces.Send(ctx, traces); err != nil {
			return err
		}
	}
	if len(logs) != 0 {
		return d.logs.Send(ctx, logs)
	}
	return nil
}
