# DataMind AI — OpenMetadata Governance Layer

This directory contains all configuration and automation scripts for the OpenMetadata data catalog integration.

## Directory Structure

```
governance/
├── openmetadata.env                      # OM server configuration (MySQL, ES, auth)
├── ingestion-workflows/                  # Ingestion YAML configs
│   ├── iceberg-bronze.yaml              # Phase 3a: Bronze tables (9 tables)
│   ├── iceberg-silver.yaml             # Phase 3b: Silver tables (13 tables)
│   ├── iceberg-gold.yaml               # Phase 3c: Gold mart tables (9 tables)
│   ├── kafka-topics.yaml               # Phase 4: Kafka topics (9 topics + Schema Registry)
│   ├── postgres-shared.yaml            # Phase 5: shared-postgres
│   ├── trino-query-engine.yaml         # Phase 6: Trino (usage stats + schema)
│   ├── airflow-pipelines.yaml          # Phase 7: Airflow DAGs as pipeline entities
│   ├── profiler-bronze.yaml            # Phase 13a: Bronze column profiling
│   ├── profiler-silver.yaml            # Phase 13b: Silver column profiling
│   └── profiler-gold.yaml              # Phase 13c: Gold column profiling
├── scripts/                             # Automation scripts (Python + shell)
│   ├── run_ingestion.sh                # Master runner (runs inside OM container)
│   ├── build_lineage.py                # Phase 8: Kafka→Bronze lineage edges
│   ├── create_glossary.py              # Phase 9: DataMind Telecom Glossary (9 terms)
│   ├── bulk_assign_owners.py           # Phase 10: Team ownership assignment
│   ├── tag_pii_columns.py              # Phase 11: PII column tagging
│   └── create_dq_tests.py             # Phase 14: DQ test cases (15 tests)
└── tests/                               # (populated by create_dq_tests.py at runtime)
```

## Prerequisites

### 1. Start the governance profile
```bash
# From project root
docker compose --profile governance up -d
docker compose ps | grep openmetadata
```

Wait until `openmetadata-server` is healthy (UI available at http://localhost:8585).

### 2. Get the Ingestion Bot JWT Token
1. Open http://localhost:8585
2. Login: `admin` / `admin` (change password on first login)
3. Navigate to: **Settings → Bots → ingestion-bot → Generate New Token**
4. Copy the token

### 3. Copy workflow files into the OM ingestion container
```bash
docker cp governance/ingestion-workflows/. openmetadata-ingestion:/opt/workflows/
docker cp governance/scripts/run_ingestion.sh openmetadata-ingestion:/opt/datamind/
```

### 4. Replace the JWT token placeholder in all workflow YAMLs
```bash
# Quick replace on all YAMLs (run from project root)
JWT="<YOUR_TOKEN_HERE>"
find governance/ingestion-workflows/ -name "*.yaml" -exec \
  sed -i "s/REPLACE_WITH_INGESTION_BOT_JWT/${JWT}/g" {} \;
```

---

## Execution Order

### Container-side (run inside openmetadata-ingestion)

```bash
# Option A: Run everything at once
docker exec -it openmetadata-ingestion bash /opt/datamind/run_ingestion.sh

# Option B: Run individual phases
docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/iceberg-bronze.yaml
docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/iceberg-silver.yaml
docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/iceberg-gold.yaml
docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/kafka-topics.yaml
docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/postgres-shared.yaml
docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/trino-query-engine.yaml
docker exec openmetadata-ingestion metadata ingest -c /opt/workflows/airflow-pipelines.yaml
docker exec openmetadata-ingestion metadata profile -c /opt/workflows/profiler-bronze.yaml
docker exec openmetadata-ingestion metadata profile -c /opt/workflows/profiler-silver.yaml
docker exec openmetadata-ingestion metadata profile -c /opt/workflows/profiler-gold.yaml
```

### Host-side Python scripts

```bash
pip install requests
export OM_JWT_TOKEN="<your-ingestion-bot-jwt>"
export OM_HOST="http://localhost:8585/api/v1"   # or http://<machine-ip>:8585/api/v1

# Run in this order:
python governance/scripts/build_lineage.py       # Phase 8: Kafka→Bronze lineage
python governance/scripts/create_glossary.py     # Phase 9: Business glossary
python governance/scripts/bulk_assign_owners.py  # Phase 10: Team ownership
python governance/scripts/tag_pii_columns.py     # Phase 11: PII tagging
python governance/scripts/create_dq_tests.py    # Phase 14: DQ test cases
```

---

## Manual Steps (OM UI)

After all scripts run, complete these in the OM UI:

| Phase | Action | Location in UI |
|---|---|---|
| 2 | Create teams: `data-engineering`, `analytics`, `data-governance` | Settings → Teams |
| 2 | Assign roles to users | Settings → Roles |
| 9 | Link glossary terms to physical columns | Glossary → \<term\> → Add Assets |
| 12 | Create Data Domains (CRM, Billing, Network, Payments, Roaming, Support) | Explore → Domains |
| 15 | Set freshness SLAs on Gold tables | Table → Profiler → Data SLA |
| 16 | Configure Slack/email alerts | Settings → Alerts → Add Alert |

---

## What Gets Registered

| Asset Type | Count | Service Name |
|---|---|---|
| Bronze Iceberg tables | 9 | `datamind-iceberg-bronze` |
| Silver Iceberg tables | 13 | `datamind-iceberg-silver` |
| Gold Iceberg tables | 9 | `datamind-iceberg-gold` |
| Kafka topics | 9 | `datamind-kafka` |
| PostgreSQL databases | 2 | `datamind-postgres` |
| Trino catalogs/schemas | 3 | `datamind-trino` |
| Airflow DAGs | 2 | `datamind-airflow` |
| **Total assets** | **47+** | |

| Governance Item | Count |
|---|---|
| Lineage edges (Kafka→Bronze) | 9 |
| Glossary terms | 9 |
| PII-tagged columns | ~30 |
| DQ test cases | 15 |
| Team ownership assignments | 31 |

---

## Connection Reference

All endpoints use Docker internal network hostnames:

| Service | Internal Endpoint | Host Port |
|---|---|---|
| Nessie | `http://nessie:19120` | 19120 |
| MinIO | `http://minio:9000` | 9000 |
| Kafka | `kafka:29092` | 9092 |
| Schema Registry | `http://schema-registry:8081` | 8081 |
| Trino | `trino:8080` | 8085 |
| shared-postgres | `shared-postgres:5432` | — |
| OM Server | `http://openmetadata-server:8585` | **8585** |
| OM Ingestion (Airflow) | `http://openmetadata-ingestion:8080` | 8080 |
