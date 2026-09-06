#!/usr/bin/env bash
set -euo pipefail

: "${LOG_ENDPOINT:?Set LOG_ENDPOINT to the complete JSON ingestion URL}"
: "${JOURNAL_DIR:?Set JOURNAL_DIR to a persistent directory for this example}"

example_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

exec "${COLLECTOR_BIN:-slogx-collector}" \
  -destination openobserve \
  -endpoint "$LOG_ENDPOINT" \
  -journal-dir "$JOURNAL_DIR" \
  -field environment=development \
  -field environment=staging \
  -field service=catalog \
  "$@" < "$example_dir/events.jsonl"
