#!/usr/bin/env bash
set -euo pipefail

: "${LOG_ENDPOINT:?Set LOG_ENDPOINT to the complete JSON ingestion URL}"
: "${JOURNAL_DIR:?Set JOURNAL_DIR to the intended journal directory}"

exec "${COLLECTOR_BIN:-slogx-collector}" \
  -destination openobserve \
  -endpoint "$LOG_ENDPOINT" \
  -journal-dir "$JOURNAL_DIR" \
  "$@" -check
