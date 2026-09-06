package cli

import (
	"net/http"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/collector/openobserve"
)

func (cfg *commandConfig) newOpenObserveDestination(headers http.Header) (collector.Destination, error) {
	return openobserve.New(openobserve.Config{
		Endpoint:        cfg.endpoint,
		Username:        cfg.username,
		Password:        cfg.password,
		Headers:         headers,
		Timeout:         cfg.requestTimeout,
		TimestampField:  cfg.timestampField,
		TimestampLayout: cfg.timestampLayout,
	})
}
