#!/usr/bin/env bash
# Start the full DataMind AI local stack
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
docker compose \
  --profile ingestion \
  --profile storage \
  --profile processing \
  --profile query \
  up -d "$@"
