# 02 — Requirements

## Purpose
Specify functional and non-functional requirements for a production-grade Data + AI platform handling **millions of records per day** across Support Tickets, Payments, and Telecom CDRs.

## Responsibilities
- Define **scope** (what the platform must do) and **quality attributes** (how well it must do it).
- Establish **SLOs/SLAs** for ingestion, availability, query latency, and freshness.
- Capture **security/compliance** and **governance** requirements.
- Document **operational** requirements: monitoring, incident response, change management.

## Architecture Decisions
- Support **three ingestion modes**: batch, CDC, streaming.
- Implement **Bronze/Silver/Gold** with enforceable contracts and quality gates.
- Require **idempotency**, replayability, and lineage for all pipelines.
- Enforce **least privilege** access and **audit logging** for all data access.

## Technology Choices
- **Kafka** for streaming and event backbone; **Schema Registry** for compatibility controls.
- **Airflow** for orchestration; **Spark** for scalable processing.
- **MinIO** (S3) for lake storage; **Iceberg** for ACID tables and partition evolution.
- **Trino** for interactive SQL; warehouse option for BI at scale (see `08`).
- **Vector DB** for embeddings (see `19`); **LLM serving** for GenAI (see `16-18`).

## Tradeoffs
- Strict quality gates increase trust but may delay availability; mitigate with staged quality levels (warning → blocking).
- Centralized governance increases control but can slow teams; mitigate with self-service + policy-as-code guardrails.

## Risks
- Data contract drift without enforcement.
- Cost overruns from always-on streaming compute.
- Latency instability from mixed workloads (ETL + ad-hoc BI + GenAI queries).

## Future Improvements
- Define tiered dataset classes (Tier-0 critical, Tier-1 important, Tier-2 best-effort).
- Add workload isolation and resource governance for GenAI/BI.
- Automate schema and policy validation in CI/CD for pipelines.

## Functional Requirements
- **FR-01 Ingestion**: ingest Tickets, Payments, CDRs via batch, CDC, and streaming.
- **FR-02 Transformation**: standardize, deduplicate, enrich, and conform domain datasets.
- **FR-03 Data Modeling**: publish Gold marts with star schemas for BI and ML features.
- **FR-04 Query & Serving**: support interactive SQL, dashboards, APIs, and extracts.
- **FR-05 ML**: train, validate, deploy, and monitor models for fraud/churn/classification/anomaly.
- **FR-06 GenAI**: Text-to-SQL and RAG with safety controls and auditability.
- **FR-07 Governance**: catalog, glossary, lineage, access controls, and retention enforcement.

## Non-Functional Requirements
### Performance & Scale
- **Ingestion throughput**: handle peak bursts (e.g., CDR spikes) with backpressure and buffering.
- **Query**: support high concurrency for BI + bounded concurrency for GenAI with quotas.
- **Processing**: elastic scaling; avoid single points of bottleneck.

### Availability & Reliability
- **Platform availability**: target \(99.9\%\) for core query layer; define dataset freshness SLOs.
- **RPO/RTO**: define per system class (see `23-disaster-recovery.md`).
- **Idempotency**: all loads must be retry-safe.

### Security & Compliance
- **Encryption**: at rest and in transit.
- **PII**: classification, masking, tokenization where required.
- **Audit**: log query access; retain logs per policy.

### Operability
- End-to-end monitoring (pipeline health, lag, costs, data quality).
- Incident response playbooks; change management for schemas.

