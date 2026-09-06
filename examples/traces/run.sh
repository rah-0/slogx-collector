#!/usr/bin/env bash
set -euo pipefail

: "${JOURNAL_DIR:?Set JOURNAL_DIR to a persistent directory for this destination}"
: "${LOG_ENDPOINT:?Set LOG_ENDPOINT to the OpenObserve JSON ingestion URL}"
: "${TRACE_ENDPOINT:?Set TRACE_ENDPOINT to the OTLP traces ingestion URL}"

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd -- "$repo_dir"

go run ./examples/traces | "${COLLECTOR_BIN:-slogx-collector}" \
  -destination openobserve \
  -endpoint "$LOG_ENDPOINT" \
  -traces-endpoint "$TRACE_ENDPOINT" \
  -header stream-name=application \
  -resource service.name=catalog \
  -journal-dir "$JOURNAL_DIR" \
  "$@"
