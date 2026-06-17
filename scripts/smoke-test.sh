#!/usr/bin/env bash
# DataMind AI — infrastructure smoke test
#
# Usage:
#   ./scripts/smoke-test.sh
#   ./scripts/smoke-test.sh --with-producers   # also publish Kafka events

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WITH_PRODUCERS=false

for arg in "$@"; do
  case "$arg" in
    --with-producers) WITH_PRODUCERS=true ;;
  esac
done

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

echo "=============================================="
echo " DataMind AI — Smoke Test"
echo "=============================================="
echo

cd "$ROOT"

# ── Container health ──────────────────────────────────────────────────────────
echo "[1/4] Docker services"

EXPECTED=(
  zookeeper
  kafka
  schema-registry
  kafka-ui
  nifi
  minio
  mc
  nessie-postgres
  nessie
  iceberg-rest
  spark-iceberg
  trino
)

for svc in "${EXPECTED[@]}"; do
  if docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qx "$svc"; then
    check "$svc container running" true
  else
    check "$svc container running" false
  fi
done

echo
echo "[2/4] HTTP / API endpoints"

check "Kafka broker API" \
  bash -c 'docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1'

check "Schema Registry" \
  wait_http "http://localhost:8081/subjects" 5

check "Kafka UI" \
  wait_http "http://localhost:8090" 5

check "NiFi UI" \
  wait_http "http://localhost:8082/nifi" 30

check "MinIO health" \
  wait_http "http://localhost:9000/minio/health/live" 5

check "Nessie API" \
  wait_http "http://localhost:19120/api/v2/config" 5

check "Iceberg REST catalog" \
  wait_http "http://localhost:8181/v1/config" 5

check "Spark UI" \
  wait_http "http://localhost:8080" 5

check "Trino coordinator" \
  wait_http "http://localhost:8085/v1/info" 5

echo
echo "[3/4] MinIO buckets"

for bucket in warehouse telecom-bronze telecom-silver telecom-gold landing; do
  check "MinIO bucket: $bucket" \
    bash -c "docker exec mc mc ls minio/${bucket} >/dev/null 2>&1"
done

echo
echo "[4/4] Kafka topics + producers"

TOPICS=(
  customer_topic
  calls_topic
  sms_topic
  data_usage_topic
  network_metrics_topic
  payments_topic
  recharge_topic
  roaming_topic
  tickets_topic
)

if [ "$WITH_PRODUCERS" = true ]; then
  PYTHON=""
  if [ -x "$ROOT/.venv/bin/python" ]; then
    PYTHON="$ROOT/.venv/bin/python"
  elif python3 -c "import kafka" 2>/dev/null; then
    PYTHON="python3"
  fi
  if [ -n "$PYTHON" ]; then
    check "Publish sample events (run_all)" \
      bash -c "cd '$ROOT/source' && '$PYTHON' -m runners.run_all --rate 10 --duration-seconds 5 --bootstrap-servers localhost:9092 --clean"
  else
    skip "Publish sample events (run: python3 -m venv .venv && .venv/bin/pip install -r source/requirements.txt)"
  fi
else
  skip "Publish sample events (pass --with-producers to enable)"
fi

for topic in "${TOPICS[@]}"; do
  if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "$topic"; then
    check "Kafka topic exists: $topic" true
  else
    # Topics are auto-created on first publish; verify broker can describe topic after producer run
    if [ "$WITH_PRODUCERS" = true ]; then
      check "Kafka topic exists: $topic" \
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "$topic"
    else
      skip "Kafka topic: $topic (created on first publish)"
    fi
  fi
done

# Trino SQL smoke (create schema via CLI inside container)
echo
echo "[bonus] Trino SQL"

check "Trino CREATE SCHEMA iceberg.bronze" \
  bash -c 'docker exec trino trino --server localhost:8080 --user smoke --execute "CREATE SCHEMA IF NOT EXISTS iceberg.bronze" >/dev/null 2>&1'

echo
echo "=============================================="
printf " Results: %s passed, %s failed, %s skipped\n" "$PASS" "$FAIL" "$SKIP"
echo "=============================================="

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi

green "All smoke checks passed."
