# DataMind AI — Project Knowledge Base

## Platform Overview
Telecom data platform simulating 2–5M events/day across 7 source systems. Domain: CRM, Billing, Network, Payments, Recharge, Roaming, Customer Support.

## Architecture Stack (all containerized in docker-compose.yml)
| Profile | Service | Port |
|---|---|---|
| ingestion | Kafka KRaft | 9092 |
| ingestion | Schema Registry | 8081 |
| ingestion | Kafka UI | 8090 |
| ingestion | Apache NiFi | 8443/8082 |
| storage | MinIO | 9000/9001 |
| storage | Nessie (ghcr.io, 0.108.0) | 19120 |
| storage | Iceberg REST | 8181 |
| storage | shared-postgres | 5432 |
| processing | spark-iceberg (tabulario) | 8888/8080 |
| query | Trino (476) | 8085 |
| orchestration | Airflow 2.10.4-python3.11 | 8083 |
| governance | OpenMetadata 1.5.0 (MySQL + ES + migrate + server + ingestion) | 8585 |
| quality | gx-gateway (custom build) | 3000 |
| analytics | semantic-layer (custom build) | 8000 |

## Data Lake Layers
- **Bronze**: 9 Iceberg tables (customers, calls, sms, data_usage, network_metrics, payments, recharge, roaming, tickets) — NiFi writes from Kafka
- **Silver**: 13 domain tables (crm_customer_registrations, crm_profile_updates, billing_calls, billing_sms, network_data_sessions, network_metrics, qos_reports, payments, recharges, roaming_events, support_tickets, complaints, ticket_resolutions)
- **Gold**: 9 mart tables (customer_360, daily_revenue, customer_usage_daily, payment_analytics, recharge_analytics, roaming_analytics, network_performance, support_analytics, fraud_monitoring)

## Kafka Topics (9 total)
customer_topic, calls_topic, sms_topic, data_usage_topic, network_metrics_topic, payments_topic, recharge_topic, roaming_topic, tickets_topic — all Avro via Schema Registry

## Spark Jobs Present
- `spark/jobs/bronze_to_silver/` — 7 domain dirs
- `spark/jobs/silver_to_gold/marts/` — 9 gold scripts (customer_360, daily_revenue, customer_usage_daily, payment_analytics, recharge_analytics, roaming_analytics, network_performance, support_analytics, fraud_monitoring)
- `spark/jobs/silver_to_gold/Dims/` — dimension loaders
- `spark/jobs/load_dims.py`

## Airflow DAGs Present
- `airflow/dags/bronze_to_silver_dag.py`
- `airflow/dags/silver_to_gold_dag.py`

## OpenMetadata Status
- **Fully integrated in docker-compose.yml** under `governance` profile
  - openmetadata-mysql (db:1.5.0) — persistent volume
  - openmetadata-elasticsearch (8.10.2)
  - openmetadata-migrate (runs once)
  - openmetadata-server (server:1.5.0, port 8585)
  - openmetadata-ingestion (ingestion:1.5.0, port 8080, embedded Airflow)
- `governance/openmetadata.env` — full OM server config (MySQL, ES, auth, pipeline client)
- `governance/ingestion-workflows/` — **EMPTY** (workflows NOT yet written)
- `governance/tests/` — **EMPTY** (DQ tests NOT yet written)

## Catalog Connection Details (for OM ingestion configs)
- Nessie: `http://nessie:19120/api/v2`
- MinIO: `http://minio:9000` (S3-compatible)
- Kafka: `kafka:29092` (internal), Schema Registry: `http://schema-registry:8081`
- Trino: `trino:8080` (internal docker network)
- shared-postgres: `shared-postgres:5432`
- Airflow (OM embedded): `http://openmetadata-ingestion:8080`

## Semantic Layer
- Custom FastAPI service (`semantic-layer/`) with YAML definitions in `definitions/`
- Metrics: Revenue, ARPU, Active Customers, Payment Success Rate, Average Recharge Amount
- Dimensions: Customer, Date, Geography, Payment Method

## Great Expectations
- Minimal gateway service (`services/gx-gateway/`) — Flask app, port 3000
- No GE suites or checkpoints defined yet
