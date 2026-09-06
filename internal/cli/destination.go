package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/rah-0/slogx-collector/internal/collector"
)

func (cfg *commandConfig) newDestination() (collector.Destination, error) {
	headers, err := cfg.destinationHeaders()
	if err != nil {
		return nil, err
	}
	if err := cfg.loadPassword(); err != nil {
		return nil, err
	}
	key, err := cfg.destinationJournalKey(headers)
	if err != nil {
		return nil, err
	}
	cfg.options.JournalKey = key
	switch cfg.destination {
	case "openobserve":
		logs, err := cfg.newOpenObserveDestination(headers)
		if err != nil {
			return nil, err
		}
		return cfg.newRoutingDestination(logs, headers)
	case "otlp":
		return cfg.newOTLPDestination(cfg.endpoint, headers)
	default:
		return nil, ErrUnsupportedDestination
	}
}

func (cfg *commandConfig) destinationHeaders() (http.Header, error) {
	headers := make(http.Header)
	for _, setting := range cfg.headers {
		name, value, ok := strings.Cut(setting, "=")
		if !ok || name == "" {
			return nil, ErrInvalidHeader
		}
		headers.Add(name, value)
	}
	for _, setting := range cfg.headerEnv {
		name, variable, ok := strings.Cut(setting, "=")
		if !ok || name == "" || variable == "" {
			return nil, ErrInvalidHeaderEnv
		}
		value, ok := os.LookupEnv(variable)
		if !ok || value == "" {
			return nil, ErrEmptyHeaderEnv
		}
		headers.Add(name, value)
	}
	return headers, nil
}

func (cfg *commandConfig) loadPassword() error {
	if cfg.passwordEnv != "" {
		value, ok := os.LookupEnv(cfg.passwordEnv)
		if !ok || value == "" {
			return ErrEmptyPasswordEnv
		}
		cfg.password = value
	}
	if cfg.passwordFile != "" {
		data, err := os.ReadFile(cfg.passwordFile)
		if err != nil {
			return ErrPasswordFileRead
		}
		cfg.password = strings.TrimRight(string(data), "\r\n")
		if cfg.password == "" {
			return ErrEmptyPasswordFile
		}
	}
	return nil
}

func (cfg *commandConfig) destinationJournalKey(headers http.Header) (string, error) {
	identity := cfg.destination + ":" + cfg.endpoint
	if cfg.destination == "otlp" || cfg.tracesEndpoint != "" {
		// OpenObserve selects a traces stream by header. Bind that routing value
		// as well as the endpoint so a restart cannot redirect pending spans.
		identity += "\x00traces\x00" + strings.Join(headers.Values("Stream-Name"), "\x00")
		if cfg.tracesEndpoint != "" {
			identity += "\x00endpoint\x00" + cfg.tracesEndpoint
		}
		if len(cfg.resource) != 0 {
			resource, err := json.Marshal(cfg.resource)
			if err != nil {
				return "", ErrInvalidResource
			}
			identity += "\x00resource\x00" + string(resource)
		}
	}
	target := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(target[:]), nil
}
