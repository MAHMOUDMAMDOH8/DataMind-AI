#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=profiles.sh
source "$ROOT/scripts/profiles.sh"
cd "$ROOT"
# shellcheck disable=SC2046
docker compose $(compose_profiles_args) up -d "$@"
