#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
# DataMind AI — OpenMetadata Ingestion Runner
# ══════════════════════════════════════════════════════════════════════════════
# Runs all ingestion workflows in the correct dependency order.
# Execute this script INSIDE the openmetadata-ingestion container.
#
# Quick start (from host machine):
#   docker exec -it openmetadata-ingestion bash /opt/datamind/run_ingestion.sh
#
# Or run individual steps:
#   docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/iceberg-bronze.yaml
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

WORKFLOWS_DIR="/opt/workflows"
LOG_DIR="/opt/ingestion-logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

mkdir -p "$LOG_DIR"

# Color output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARN:${NC} $*"; }
fail() { echo -e "${RED}[$(date +'%H:%M:%S')] FAIL:${NC} $*"; }

run_workflow() {
    local name="$1"
    local yaml="$WORKFLOWS_DIR/$2"
    local logfile="$LOG_DIR/${TIMESTAMP}_${name}.log"

    log "Running: $name"
    if metadata ingest -c "$yaml" 2>&1 | tee "$logfile"; then
        log "✓ Done: $name"
        return 0
    else
        fail "✗ Failed: $name (log: $logfile)"
        return 1
    fi
}

run_profiler() {
    local name="$1"
    local yaml="$WORKFLOWS_DIR/$2"
    local logfile="$LOG_DIR/${TIMESTAMP}_${name}.log"

    log "Profiling: $name"
    if metadata profile -c "$yaml" 2>&1 | tee "$logfile"; then
        log "✓ Done: $name"
        return 0
    else
        fail "✗ Failed: $name (log: $logfile)"
        return 1
    fi
}

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  DataMind AI — OpenMetadata Full Ingestion Suite"
echo "  Timestamp: $TIMESTAMP"
echo "══════════════════════════════════════════════════════════"
echo ""

# ── Phase 3: Iceberg Catalog (Bronze / Silver / Gold) ─────────────────────────
log "=== PHASE 3: Iceberg Catalog Ingestion ==="
run_workflow "iceberg-bronze"  "iceberg-bronze.yaml"
run_workflow "iceberg-silver"  "iceberg-silver.yaml"
run_workflow "iceberg-gold"    "iceberg-gold.yaml"

# ── Phase 4: Kafka Topics ─────────────────────────────────────────────────────
log "=== PHASE 4: Kafka Topics Ingestion ==="
run_workflow "kafka-topics"    "kafka-topics.yaml"

# ── Phase 5: Shared PostgreSQL ────────────────────────────────────────────────
log "=== PHASE 5: PostgreSQL Ingestion ==="
run_workflow "postgres-shared" "postgres-shared.yaml"

# ── Phase 6: Trino Query Engine ───────────────────────────────────────────────
log "=== PHASE 6: Trino Engine Ingestion ==="
run_workflow "trino-engine"    "trino-query-engine.yaml"

# ── Phase 7: Airflow Pipelines ────────────────────────────────────────────────
log "=== PHASE 7: Airflow Pipelines Ingestion ==="
run_workflow "airflow-pipelines" "airflow-pipelines.yaml"

# ── Phase 13: Profiling ───────────────────────────────────────────────────────
log "=== PHASE 13: Column Profiling ==="
warn "Profiling may take several minutes depending on table sizes."
run_profiler "profiler-bronze"   "profiler-bronze.yaml"
run_profiler "profiler-silver"   "profiler-silver.yaml"
run_profiler "profiler-gold"     "profiler-gold.yaml"

echo ""
log "══════════════════════════════════════════════════════════"
log "  All ingestion workflows complete!"
log "  Logs: $LOG_DIR"
log ""
log "  Next steps to run on HOST machine:"
log "  export OM_JWT_TOKEN='<token>'"
log "  python governance/scripts/build_lineage.py"
log "  python governance/scripts/create_glossary.py"
log "  python governance/scripts/bulk_assign_owners.py"
log "  python governance/scripts/tag_pii_columns.py"
log "  python governance/scripts/create_dq_tests.py"
log "══════════════════════════════════════════════════════════"
