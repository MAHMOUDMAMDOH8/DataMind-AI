#!/usr/bin/env bash
# DataMind AI — infrastructure smoke test
#
# Usage:
#   ./scripts/smoke-test.sh                    # test running services (skip stopped)
#   ./scripts/smoke-test.sh --strict           # require full stack from ./scripts/up.sh
#   ./scripts/smoke-test.sh --profile ml       # test one profile only
#   ./scripts/smoke-test.sh --with-producers   # publish sample Kafka events
#   ./scripts/smoke-test.sh --all-profiles     # run each profile group sequentially

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=profiles.sh
source "$ROOT/scripts/profiles.sh"

WITH_PRODUCERS=false
STRICT=false
ALL_PROFILES_MODE=false
SELECTED_PROFILE=""

args=("$@")
i=0
while [ $i -lt ${#args[@]} ]; do
  case "${args[$i]}" in
    --with-producers) WITH_PRODUCERS=true ;;
    --strict) STRICT=true ;;
    --all-profiles) ALL_PROFILES_MODE=true ;;
    --profile)
      i=$((i + 1))
      SELECTED_PROFILE="${args[$i]:-}"
      ;;
    --profile=*) SELECTED_PROFILE="${args[$i]#--profile=}" ;;
  esac
  i=$((i + 1))
done

if [ -n "$SELECTED_PROFILE" ] && ! profile_services "$SELECTED_PROFILE" >/dev/null 2>&1; then
  echo "Unknown profile: $SELECTED_PROFILE"
  echo "Valid profiles: ${ALL_PROFILES[*]}"
  exit 2
fi

PASS=0
FAIL=0
SKIP=0

green() { printf '\033[32m%s\033[0m\n' "$1"; }
red()   { printf '\033[31m%s\033[0m\n' "$1"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$1"; }

check() {
  local name="$1"
  shift
  if "$@"; then
    green "  PASS  $name"
    PASS=$((PASS + 1))
  else
    red "  FAIL  $name"
    FAIL=$((FAIL + 1))
  fi
}

skip() {
  yellow "  SKIP  $1"
  SKIP=$((SKIP + 1))
}

wait_http() {
  local url="$1"
  local retries="${2:-30}"
  local i=0
  while [ "$i" -lt "$retries" ]; do
    if curl -sf "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

is_running() {
  docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qx "$1"
}

profiles_to_test() {
  if [ -n "$SELECTED_PROFILE" ]; then
    echo "$SELECTED_PROFILE"
    return
  fi
  if [ "$ALL_PROFILES_MODE" = true ] || [ "$STRICT" = true ]; then
    printf '%s\n' "${ALL_PROFILES[@]}"
    return
  fi
  printf '%s\n' "${ALL_PROFILES[@]}"
}

test_container() {
  local svc="$1"
  local profile="$2"
  if is_running "$svc"; then
    check "[$profile] $svc running" true
    return 0
  fi
  if [ "$STRICT" = true ] || [ -n "$SELECTED_PROFILE" ]; then
    check "[$profile] $svc running" false
  else
    skip "[$profile] $svc (not running)"
  fi
  return 1
}

test_endpoints_for_profile() {
  local profile="$1"
  local any_running=false
  local svc
  for svc in $(profile_services "$profile"); do
    if is_running "$svc"; then
      any_running=true
      break
    fi
  done
  if [ "$any_running" = false ]; then
    return 0
  fi

  while IFS='|' read -r name target retries; do
    [ -z "$name" ] && continue
    retries="${retries:-5}"
    if [[ "$target" == http* ]]; then
      check "[$profile] $name" wait_http "$target" "$retries"
    else
      check "[$profile] $name" bash -c "$target"
    fi
  done < <(profile_endpoints "$profile")
}

test_profile() {
  local profile="$1"
  echo
  echo "── profile: $profile ──"

  local svc
  for svc in $(profile_services "$profile"); do
    test_container "$svc" "$profile" || true
  done

  test_endpoints_for_profile "$profile"

  case "$profile" in
    storage)
      if is_running mc; then
        for bucket in warehouse telecom-bronze telecom-silver telecom-gold landing; do
          check "[$profile] MinIO bucket: $bucket" \
            bash -c "docker exec mc mc ls minio/${bucket} >/dev/null 2>&1"
        done
      fi
      ;;
    query)
      if is_running trino; then
        check "[$profile] Trino CREATE SCHEMA iceberg.bronze" \
          bash -c 'docker exec trino trino --server localhost:8080 --user smoke --execute "CREATE SCHEMA IF NOT EXISTS iceberg.bronze" >/dev/null 2>&1'
      fi
      ;;
    ingestion)
      if [ "$WITH_PRODUCERS" = true ] && is_running kafka; then
        PYTHON=""
        if [ -x "$ROOT/.venv/bin/python" ]; then
          PYTHON="$ROOT/.venv/bin/python"
        elif python3 -c "import kafka" 2>/dev/null; then
          PYTHON="python3"
        fi
        if [ -n "$PYTHON" ]; then
          check "[$profile] Publish sample events" \
            bash -c "cd '$ROOT/source' && '$PYTHON' -m runners.run_all --rate 10 --duration-seconds 5 --bootstrap-servers localhost:9092 --clean"
        else
          skip "[$profile] Publish sample events (create .venv and pip install -r source/requirements.txt)"
        fi
        local topic
        for topic in customer_topic calls_topic sms_topic data_usage_topic network_metrics_topic payments_topic recharge_topic roaming_topic tickets_topic; do
          check "[$profile] Kafka topic: $topic" \
            bash -c "docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx '$topic'"
        done
      elif [ "$WITH_PRODUCERS" = true ]; then
        skip "[$profile] Kafka producers (kafka not running)"
      fi
      ;;
  esac
}

echo "=============================================="
echo " DataMind AI — Smoke Test"
echo "=============================================="
cd "$ROOT"

if [ "$ALL_PROFILES_MODE" = true ]; then
  echo "Mode: all profiles (sequential)"
elif [ -n "$SELECTED_PROFILE" ]; then
  echo "Mode: profile=$SELECTED_PROFILE"
elif [ "$STRICT" = true ]; then
  echo "Mode: strict (full stack)"
else
  echo "Mode: running services only"
fi

while IFS= read -r profile; do
  if [ -n "$SELECTED_PROFILE" ] && [ "$profile" != "$SELECTED_PROFILE" ]; then
    continue
  fi
  test_profile "$profile"
done < <(profiles_to_test)

echo
echo "=============================================="
printf " Results: %s passed, %s failed, %s skipped\n" "$PASS" "$FAIL" "$SKIP"
echo "=============================================="

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi

green "All smoke checks passed."
