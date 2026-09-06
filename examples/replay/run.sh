#!/usr/bin/env bash
set -euo pipefail

example_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

case "${1:-}" in
  ingest) input="$example_dir/events.jsonl" ;;
  replay) input=/dev/null ;;
  *)
    printf 'Usage: %s ingest|replay [collector options]\n' "$0" >&2
    exit 2
    ;;
esac
shift

: "${LOG_ENDPOINT:?Set LOG_ENDPOINT to the complete JSON ingestion URL}"
: "${JOURNAL_DIR:?Set JOURNAL_DIR to a persistent directory for this example}"

exec "${COLLECTOR_BIN:-slogx-collector}" \
  -destination openobserve \
  -endpoint "$LOG_ENDPOINT" \
  -journal-dir "$JOURNAL_DIR" \
  -max-retries 1 \
  -retry-interval 250ms \
  -max-retry-interval 250ms \
  -request-timeout 2s \
  "$@" < "$input"
