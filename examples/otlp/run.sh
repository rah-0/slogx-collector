#!/usr/bin/env bash
set -euo pipefail

: "${JOURNAL_DIR:?Set JOURNAL_DIR to a persistent directory for this destination}"
: "${TRACE_ENDPOINT:?Set TRACE_ENDPOINT to the OTLP traces ingestion URL}"

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd -- "$repo_dir"

go run ./examples/traces | "${COLLECTOR_BIN:-slogx-collector}" \
  -destination otlp \
  -endpoint "$TRACE_ENDPOINT" \
  -resource service.name=catalog \
  -journal-dir "$JOURNAL_DIR" \
  "$@"
