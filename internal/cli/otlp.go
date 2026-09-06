package cli

import (
	"net/http"

	"github.com/rah-0/slogx-collector/internal/collector"
	"github.com/rah-0/slogx-collector/internal/collector/otlp"
)

func (cfg *commandConfig) newOTLPDestination(endpoint string, headers http.Header) (collector.Destination, error) {
	return otlp.New(otlp.Config{
		Endpoint: endpoint,
		Username: cfg.username,
		Password: cfg.password,
		Headers:  headers,
		Timeout:  cfg.requestTimeout,
		Resource: cfg.resource,
	})
}
