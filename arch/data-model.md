# Data Model: Dual Data Delivery Paths

**Version**: 1.0.0 | **Last Updated**: 2026-06-06

## Overview

This document defines all entities across the medallion layers (Bronze, Silver, Gold) including shared governance artifacts (quality quarantine, lineage, run status). The model covers both the dashboard (BI star-schema) and RAG (document retrieval) consumption paths.

---

## Part 1 — Bronze Layer (Raw Ingestion)

### 1.1 Bronze Event Tables (8 domains)

**Purpose**: Append-only raw capture of source telecom events with ingest lineage.

**Common technical columns** (added at ingest):

| Column              | Type      | Description                                         |
| ------------------- | --------- | --------------------------------------------------- |
| `ingest_ts_utc`     | TIMESTAMP | When the record was ingested into Bronze            |
| `batch_id`          | STRING    | Unique batch identifier (e.g., `batch_20260606_v1`) |
| `source_file`       | STRING    | Original source file path                           |
| `source_row_number` | INT       | Row number within source file                       |
| `raw_record_hash`   | STRING    | SHA256 of raw JSON/CSV row for dedup detection      |

**1.1.1 `brz_call_events`**

| Column | Type | Source |
|--------|------|--------|
| `event_type` | STRING | `call` |
| `sid` | STRING | Unique event id (prefix `CA`) |
| `from` | STRING | Calling number |
| `to` | STRING | Called number |
| `call_duration_seconds` | INT | Duration in seconds |
| `status` | STRING | `initiated`, `ringing`, `in-progress`, `completed`, `failed`, `busy`, `no-answer` |
| `timestamp` | STRING | Raw event datetime text |
| `call_type` | STRING | `Local`, `International` |
| `phone_number` | STRING | Customer phone number |
| `customer` | STRING | Customer reference (duplicate of phone_number) |
| `behavior_profile` | STRING | User segment |
| `seasonal_multiplier` | FLOAT | Seasonality factor |
| `billing_info` | STRING | Stringified JSON `{"amount": N, "currency": "EGP"}` |
| `qos_metrics` | STRING | Stringified JSON `{"mos_score": N, "jitter_ms": N, "packet_loss_percent": N, "codec": S}` |

**1.1.2 `brz_sms_events`**

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `sms` |
| `sid` | STRING | Prefix `SM` |
| `from` | STRING | Sender |
| `to` | STRING | Receiver |
| `body` | STRING | Message body (nullable) |
| `status` | STRING | `queued`, `sending`, `sent`, `delivered`, `failed` |
| `timestamp` | STRING | Raw datetime |
| `phone_number` | STRING | |
| `customer` | STRING | |
| `registration_date` | STRING | Customer registration snapshot |
| `seasonal_multiplier` | FLOAT | |
| `billing_info` | STRING | Stringified JSON |
| `network_metrics` | STRING | Stringified JSON |

**1.1.3 `brz_data_usage_events`**

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `data_usage` |
| `sid` | STRING | Prefix `DU` |
| `customer` | STRING | |
| `data_used_mb` | FLOAT | |
| `data_type` | STRING | `Browsing`, `Streaming`, `Upload`, `Download`, `Background Sync` |
| `network_type` | STRING | `3G`, `4G`, `LTE`, `5G` |
| `status` | STRING | `active`, `completed`, `throttled`, `exceeded` |
| `phone_number` | STRING | |
| `behavior_profile` | STRING | |
| `session_duration_seconds` | INT | |
| `seasonal_multiplier` | FLOAT | |
| `billing_info` | STRING | Stringified JSON |
| `network_metrics` | STRING | Stringified JSON |
| `fraud_indicator` | STRING | Sparse fraud pattern marker |
| `risk_score` | INT | 1-100 scale |

**1.1.4 `brz_payment_events`**

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `payment` |
| `sid` | STRING | Prefix `PAY` |
| `customer` | STRING | |
| `payment_type` | STRING | `Bill Payment`, `Subscription`, `Plan Upgrade`, `Late Fee` |
| `payment_amount` | FLOAT | |
| `payment_method` | STRING | `Credit Card`, `Mobile Wallet`, `Bank Transfer`, `Cash` |
| `status` | STRING | `success`, `pending`, `failed`, `refunded` |
| `timestamp` | STRING | |
| `phone_number` | STRING | |
| `transaction_id` | STRING | |
| `invoice_number` | STRING | Nullable |
| `seasonal_multiplier` | FLOAT | |
| `billing_info` | STRING | Stringified JSON |
| `_requires_retry` | BOOLEAN | |

**1.1.5 `brz_recharge_events`**

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `recharge` |
| `sid` | STRING | Prefix `RC` |
| `customer` | STRING | |
| `recharge_amount` | FLOAT | |
| `balance_before` | FLOAT | |
| `balance_after` | FLOAT | |
| `payment_method` | STRING | |
| `status` | STRING | `success`, `pending`, `failed`, `processing` |
| `timestamp` | STRING | |
| `phone_number` | STRING | |
| `transaction_id` | STRING | |
| `seasonal_multiplier` | FLOAT | |
| `billing_info` | STRING | |
| `_requires_followup` | BOOLEAN | |

**1.1.6 `brz_roaming_events`**

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `roaming` |
| `sid` | STRING | Prefix `RO` |
| `customer` | STRING | |
| `roaming_country` | STRING | |
| `roaming_operator` | STRING | |
| `roaming_type` | STRING | `voice`, `data`, `sms`, `all` |
| `duration_seconds` | INT | |
| `data_used_mb` | FLOAT | |
| `roaming_charges` | FLOAT | |
| `status` | STRING | `active`, `completed`, `terminated` |
| `timestamp` | STRING | |
| `phone_number` | STRING | |
| `local_time` | STRING | |
| `billing_info` | STRING | |
| `seasonal_multiplier` | FLOAT | |

**1.1.7 `brz_security_events`**

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `security` |
| `sid` | STRING | Prefix `SE` |
| `customer` | STRING | |
| `security_type` | STRING | `failed_login`, `sim_swap_request`, `number_porting_request` |
| `severity` | STRING | `medium`, `high` |
| `timestamp` | STRING | |
| `phone_number` | STRING | |
| `ip_address` | STRING | |
| `user_agent` | STRING | |
| `action_taken` | STRING | `logged`, `notified`, `blocked`, `investigating` |
| `risk_score` | INT | |

**1.1.8 `brz_support_events`**

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `support` |
| `sid` | STRING | Prefix `SU` |
| `customer` | STRING | |
| `channel` | STRING | `phone`, `chat`, `email`, `store`, `social_media`, `ivr` |
| `reason` | STRING | `billing`, `technical`, `account`, `complaint`, `activation`, `general` |
| `wait_time_seconds` | INT | |
| `resolution_time_seconds` | INT | |
| `agent_id` | STRING | |
| `satisfaction_score` | INT | 1-5, nullable |
| `timestamp` | STRING | |
| `phone_number` | STRING | |
| `first_call_resolution` | BOOLEAN | |
| `escalated` | BOOLEAN | |
| `call_back_requested` | BOOLEAN | |

**Partitioning**: `event_date` (derived from `timestamp` at ingest time)

---

## Part 2 — Silver Layer (Conformed, Validated)

### 2.1 Silver Event Tables (8 domains)

**Purpose**: Clean, type-safe, conformed records with quality annotations. JSON payloads parsed into typed columns. Customer keys standardized. Invalid records flagged and routed to quarantine.

**Common Silver columns** (added to all domain tables):

| Column | Type | Description |
|--------|------|-------------|
| `slv_ingest_ts_utc` | TIMESTAMP | Silver processing timestamp |
| `batch_id` | STRING | Originating batch id |
| `source_file` | STRING | Source lineage |
| `event_date` | DATE | Parsed event date partition |
| `customer_id` | STRING | Canonical customer key (normalized phone) |
| `dq_is_valid_timestamp` | BOOLEAN | Timestamp parsed successfully |
| `dq_is_valid_customer` | BOOLEAN | Customer key is non-null and valid format |
| `dq_issue_codes` | ARRAY<STRING> | Quality issue codes if any |
| `dq_quality_score` | FLOAT | 0.0–1.0 composite quality score |
| `slv_schema_version` | STRING | Schema version used for transform |

**2.1.1 `slv_call_events`**

All Bronze columns plus:
- `qos_metrics` parsed to: `mos_score` (FLOAT), `jitter_ms` (FLOAT), `packet_loss_percent` (FLOAT), `codec` (STRING)
- `billing_info` parsed to: `amount_egp` (FLOAT), `currency_code` (STRING)
- `event_ts_utc` (TIMESTAMP) — parsed from `timestamp`
- `call_duration_seconds` clamped: negative → 0, null → 0

Quality rules:
- Reject if `sid` is null
- Reject if `timestamp` cannot be parsed
- Flag if `call_duration_seconds` is negative (quarantine if beyond threshold)

**2.1.2–2.1.8**: Analogous transforms for SMS, data_usage, payment, recharge, roaming, security, support.

### 2.2 Silver Shared Dimensions

**2.2.1 `slv_dim_customer`**

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | STRING | Canonical customer key (PK) |
| `phone_number` | STRING | Original phone number |
| `behavior_profile` | STRING | `light_user`, `business_user`, `heavy_streamer`, `gamer`, `social_media`, `international` |
| `plan_type` | STRING | `Prepaid`, `Postpaid` |
| `city` | STRING | |
| `region` | STRING | |
| `registration_date` | DATE | |
| `active_from_batch` | STRING | First batch where customer appeared |

**2.2.2 `slv_dim_date`**

| Column | Type |
|--------|------|
| `date_key` | DATE (PK) |
| `day` | INT |
| `month` | INT |
| `year` | INT |
| `week_of_year` | INT |
| `day_of_week` | INT |
| `is_weekend` | BOOLEAN |
| `quarter` | INT |

**2.2.3 `slv_dim_time`**

| Column | Type |
|--------|------|
| `time_key` | STRING (HH:MM format, PK) |
| `hour` | INT |
| `minute` | INT |
| `hour_bucket` | STRING (e.g., `morning`, `afternoon`, `evening`, `night`) |

**2.2.4 `slv_dim_geo`**

| Column | Type |
|--------|------|
| `geo_id` | STRING (PK) |
| `city` | STRING |
| `region` | STRING |
| `country` | STRING |
| `latitude` | FLOAT |
| `longitude` | FLOAT |

**2.2.5 `slv_dim_channel`**

| Column | Type |
|--------|------|
| `channel_id` | STRING (PK) |
| `channel_name` | STRING |
| `channel_type` | STRING (`support`, `payment`, `recharge`) |
| `channel_group` | STRING (`digital`, `physical`, `voice`) |

**2.2.6 `slv_dim_status`**

| Column | Type |
|--------|------|
| `status_code` | STRING (PK) |
| `status_description` | STRING |
| `domain` | STRING |
| `is_success` | BOOLEAN |
| `is_terminal` | BOOLEAN |

**2.2.7 `slv_dim_payment_method`**

| Column | Type |
|--------|------|
| `payment_method_id` | STRING (PK) |
| `payment_method_name` | STRING |
| `payment_method_type` | STRING (`card`, `wallet`, `cash`, `transfer`) |

### 2.3 Silver Quarantine

**`slv_quarantine_events`**

| Column              | Type          | Description                                                                    |
| ------------------- | ------------- | ------------------------------------------------------------------------------ |
| `quarantine_id`     | STRING        | UUID for quarantine record                                                     |
| `batch_id`          | STRING        | Originating batch                                                              |
| `source_table`      | STRING        | Bronze source table name                                                       |
| `sid`               | STRING        | Original event id (if available)                                               |
| `reason_codes`      | ARRAY<STRING> | e.g., `invalid_timestamp`, `missing_sid`, `negative_duration`, `null_customer` |
| `raw_record_json`   | STRING        | Full original record for debugging                                             |
| `quarantine_ts_utc` | TIMESTAMP     | When quarantined                                                               |
| `quarantine_action` | STRING        | `blocked` (default), `flagged_only`                                            |

### 2.4 Silver Helper Views (RAG)

**`slv_customer_event_timeline`** — ordered event stream per customer across all domains.
**`slv_customer_daily_metrics`** — daily aggregate per customer (event counts, totals).
**`slv_segment_daily_metrics`** — daily aggregate per behavior segment/region.

---

## Part 3 — Gold Layer — Dashboard Path (Star Schema)

### 3.1 Gold Dimension Tables

**3.1.1 `gld_dim_customer`**

| Column | Type | Notes |
|--------|------|-------|
| `customer_id` | STRING (PK) | Canonical key |
| `customer_phone` | STRING | Masked/tokenized for PII control |
| `behavior_profile` | STRING | |
| `plan_type` | STRING | |
| `city` | STRING | |
| `region` | STRING | |
| `customer_tenure_days` | INT | |
| `risk_band` | STRING | `low`, `medium`, `high`, `critical` |
| `is_active` | BOOLEAN | |
| `gold_valid_from` | DATE | Slowly changing dimension tracking |
| `gold_valid_to` | DATE | |
| `gold_is_current` | BOOLEAN | |

**3.1.2 `gld_dim_date`**

| Column | Type |
|--------|------|
| `date_key` | DATE (PK) |
| `day` | INT |
| `month` | INT |
| `year` | INT |
| `month_name` | STRING |
| `quarter` | INT |
| `week_of_year` | INT |
| `day_of_week` | INT |
| `is_weekend` | BOOLEAN |
| `is_holiday` | BOOLEAN |
| `is_current_month` | BOOLEAN |

**3.1.3 `gld_dim_geo`**

| Column | Type |
|--------|------|
| `geo_id` | STRING (PK) |
| `city` | STRING |
| `region` | STRING |
| `country` | STRING |

**3.1.4 `gld_dim_channel`**

| Column | Type |
|--------|------|
| `channel_id` | STRING (PK) |
| `channel_name` | STRING |
| `channel_group` | STRING |
| `is_digital` | BOOLEAN |

**3.1.5 `gld_dim_service`**

| Column | Type |
|--------|------|
| `service_id` | STRING (PK) |
| `service_name` | STRING |
| `service_category` | STRING (`voice`, `data`, `sms`, `roaming`, `value_added`) |
| `billing_model` | STRING (`prepaid`, `postpaid`, `hybrid`) |

**3.1.6 `gld_dim_risk_band`**

| Column | Type |
|--------|------|
| `risk_band_id` | STRING (PK) |
| `risk_band_name` | STRING |
| `min_risk_score` | INT |
| `max_risk_score` | INT |
| `requires_review` | BOOLEAN |
| `escalation_level` | INT |

### 3.2 Gold Fact Tables

**3.2.1 `gld_fact_usage_daily`** (grain: customer_id × date_key)

| Column | Type | Source |
|--------|------|--------|
| `usage_id` | STRING (PK) | Deterministic from grain |
| `customer_id` | STRING (FK) | |
| `date_key` | DATE (FK) | |
| `total_call_duration_sec` | INT | Sum of call durations |
| `call_count` | INT | |
| `sms_count` | INT | |
| `sms_delivery_rate` | FLOAT | |
| `total_data_used_mb` | FLOAT | |
| `data_session_count` | INT | |
| `avg_call_duration_sec` | FLOAT | |
| `avg_data_per_session_mb` | FLOAT | |
| `usage_quality_score` | FLOAT | Composite from qos metrics |
| `lineage_batch_id` | STRING | |
| `gold_ingest_ts_utc` | TIMESTAMP | |

**3.2.2 `gld_fact_revenue_daily`** (grain: customer_id × date_key)

| Column | Type | Source |
|--------|------|--------|
| `revenue_id` | STRING (PK) | |
| `customer_id` | STRING (FK) | |
| `date_key` | DATE (FK) | |
| `total_payment_amount_egp` | FLOAT | |
| `total_recharge_amount_egp` | FLOAT | |
| `total_usage_charges_egp` | FLOAT | From billing_info |
| `total_roaming_charges_egp` | FLOAT | |
| `payment_count` | INT | |
| `recharge_count` | INT | |
| `payment_success_count` | INT | |
| `payment_failure_count` | INT | |
| `payment_success_rate` | FLOAT | |
| `arpu_proxy_egp` | FLOAT | Total revenue / active customer days |
| `net_revenue_egp` | FLOAT | Payments + recharges - refunds |
| `lineage_batch_id` | STRING | |
| `gold_ingest_ts_utc` | TIMESTAMP | |

**3.2.3 `gld_fact_support_daily`** (grain: customer_id × date_key)

| Column | Type |
|--------|------|
| `support_id` | STRING (PK) |
| `customer_id` | STRING (FK) |
| `date_key` | DATE (FK) |
| `support_case_count` | INT |
| `avg_wait_time_sec` | FLOAT |
| `avg_resolution_time_sec` | FLOAT |
| `csat_avg` | FLOAT |
| `csat_count` | INT |
| `escalation_count` | INT |
| `escalation_rate` | FLOAT |
| `first_call_resolution_count` | INT |
| `first_call_resolution_rate` | FLOAT |
| `channel_distribution_json` | STRING | JSON map of channel→count |
| `lineage_batch_id` | STRING | |
| `gold_ingest_ts_utc` | TIMESTAMP | |

**3.2.4 `gld_fact_risk_daily`** (grain: customer_id × date_key)

| Column | Type |
|--------|------|
| `risk_id` | STRING (PK) |
| `customer_id` | STRING (FK) |
| `date_key` | DATE (FK) |
| `security_event_count` | INT |
| `max_risk_score` | INT |
| `high_severity_count` | INT |
| `fraud_indicator_count` | INT |
| `risk_band` | STRING |
| `has_sim_swap` | BOOLEAN |
| `has_porting_request` | BOOLEAN |
| `total_retry_required_amount_egp` | FLOAT |
| `lineage_batch_id` | STRING | |
| `gold_ingest_ts_utc` | TIMESTAMP | |

### 3.3 Gold KPI Marts (Pre-Aggregated)

**3.3.1 `gld_mart_exec_kpi_daily`** (grain: date_key)

| Column | Type |
|--------|------|
| `date_key` | DATE (PK) |
| `total_revenue_egp` | FLOAT |
| `arpu_proxy_egp` | FLOAT |
| `payment_success_rate` | FLOAT |
| `recharge_success_rate` | FLOAT |
| `active_customer_count` | INT |
| `avg_call_duration_sec` | FLOAT |
| `avg_data_mb_per_customer` | FLOAT |
| `sms_delivery_rate` | FLOAT |
| `support_case_count` | INT |
| `avg_wait_time_sec` | FLOAT |
| `avg_resolution_time_sec` | FLOAT |
| `csat_avg` | FLOAT |
| `escalation_rate` | FLOAT |
| `high_risk_event_rate` | FLOAT |
| `top_region_by_revenue` | STRING |
| `top_region_by_risk` | STRING |
| `lineage_batch_id` | STRING |
| `gold_ingest_ts_utc` | TIMESTAMP |

**3.3.2 `gld_mart_customer_360_daily`** (grain: customer_id × date_key)

All fields from fact tables joined + customer dimension attributes. Includes `total_csat`, `total_revenue_30d`, `churn_risk_score`, `days_since_last_interaction`.

**3.3.3 `gld_mart_network_quality_daily`** (grain: region × network_type × date_key)

Aggregates MOS, jitter, packet loss, throughput, call drop rate.

**3.3.4 `gld_mart_roaming_performance_daily`** (grain: country × operator × date_key)

Roaming event counts, total charges, avg duration, avg data used.

**3.3.5 `gld_mart_support_operations_daily`** (grain: channel × reason × date_key)

Case volumes, avg wait/resolution, CSAT, FCR, escalation rate by channel/reason.

### 3.4 Dashboard Path KPI Registry

| KPI ID | Name | Formula | Grain | Owner |
|--------|------|---------|-------|-------|
| `KPI001` | Total Revenue (EGP) | SUM payments + recharges - refunds | daily | Finance |
| `KPI002` | Payment Success Rate | successful payments / total payments | daily | Finance |
| `KPI003` | Recharge Success Rate | successful recharges / total recharges | daily | Finance |
| `KPI004` | ARPU Proxy (EGP) | total daily revenue / active customers | daily | Finance |
| `KPI005` | Avg Data per Customer (MB) | total data / active data customers | daily | Product |
| `KPI006` | Avg Call Duration (sec) | total duration / call count | daily | Product |
| `KPI007` | SMS Delivery Rate | delivered / sent | daily | Product |
| `KPI008` | Support Case Volume | COUNT(support cases) | daily | CX |
| `KPI009` | Avg Wait Time (sec) | AVG(wait_time) | daily | CX |
| `KPI010` | Avg Resolution Time (sec) | AVG(resolution_time) | daily | CX |
| `KPI011` | CSAT Average | AVG(satisfaction_score) | daily | CX |
| `KPI012` | Escalation Rate | escalated / total cases | daily | CX |
| `KPI013` | High Risk Event Rate | high-severity events / total events | daily | Risk |

---

## Part 4 — Gold Layer — RAG Path (Document Retrieval)

### 4.1 RAG Document Types

**4.1.1 `gld_rag_doc_customer_daily`**

Grain: one doc per `customer_id` per `event_date`. Provides day-level customer context.

| Column | Type | Description |
|--------|------|-------------|
| `doc_id` | STRING (PK) | Deterministic: `cust_daily_{customer_id}_{date_key}` |
| `doc_type` | STRING | `customer_daily` |
| `doc_schema_version` | STRING | e.g., `1.0.0` |
| `title` | STRING | e.g., `"Customer 0100XXXXXX daily summary — 2026-06-06"` |
| `summary_text` | STRING | Narrative summary of customer's day (calls, SMS, data, payments, support, risk) |
| `evidence_event_ids` | ARRAY<STRING> | List of `sid` values supporting the summary |
| `source_tables` | ARRAY<STRING> | Silver source tables used |
| `time_start_utc` | TIMESTAMP | Start of the day |
| `time_end_utc` | TIMESTAMP | End of the day |
| `customer_id` | STRING | Filter key |
| `region` | STRING | |
| `city` | STRING | |
| `business_domain` | STRING | `usage`, `finance`, `support`, `risk`, `roaming` |
| `risk_band` | STRING | |
| `metrics_json` | STRING | Snapshot of daily KPIs for this customer |
| `data_quality_score` | FLOAT | 0.0–1.0 |
| `generated_ts_utc` | TIMESTAMP | Doc generation timestamp |
| `batch_id` | STRING | Batch that produced this doc |

**4.1.2 `gld_rag_doc_customer_30d`**

Grain: one doc per `customer_id` per rolling 30-day window. Narrative account review.

| Column | Type | Notes |
|--------|------|-------|
| `doc_id` | STRING (PK) | `cust_30d_{customer_id}_{window_end_date}` |
| `doc_type` | STRING | `customer_30d` |
| `title` | STRING | |
| `summary_text` | STRING | 30-day narrative: trends, anomalies, churn signals |
| `evidence_event_ids` | ARRAY<STRING> | |
| `source_tables` | ARRAY<STRING> | |
| `time_start_utc` | TIMESTAMP | 30 days ago |
| `time_end_utc` | TIMESTAMP | Today |
| `customer_id` | STRING | |
| `region` | STRING | |
| `risk_band` | STRING | |
| `metrics_json` | STRING | 30-day aggregate metrics |
| `data_quality_score` | FLOAT | |
| `generated_ts_utc` | TIMESTAMP | |

**4.1.3 `gld_rag_doc_domain_daily`**

Grain: one doc per `domain` × `date_key` (with optional segment/region breakdown).

| Column | Type | Notes |
|--------|------|-------|
| `doc_id` | STRING (PK) | `domain_daily_{domain}_{date_key}` |
| `doc_type` | STRING | `domain_daily` |
| `title` | STRING | e.g., `"Payment domain summary — 2026-06-06"` |
| `summary_text` | STRING | Narrative: totals, trends, anomalies for this domain |
| `evidence_event_ids` | ARRAY<STRING> | Representative event ids |
| `source_tables` | ARRAY<STRING> | |
| `time_start_utc` | TIMESTAMP | |
| `time_end_utc` | TIMESTAMP | |
| `business_domain` | STRING | Filter key |
| `region` | STRING | |
| `segment` | STRING | e.g., `prepaid`, `postpaid` |
| `metrics_json` | STRING | Domain-level aggregates |
| `data_quality_score` | FLOAT | |
| `generated_ts_utc` | TIMESTAMP | |

**4.1.4 `gld_rag_doc_incident`**

Grain: one doc per notable event cluster/anomaly (fraud, outage, billing spike).

| Column | Type | Notes |
|--------|------|-------|
| `doc_id` | STRING (PK) | `incident_{uuid_short}` |
| `doc_type` | STRING | `incident` |
| `title` | STRING | e.g., `"Billing spike detected — payment failures +45%"` |
| `summary_text` | STRING | Incident narrative: what, when, impact, recommended actions |
| `evidence_event_ids` | ARRAY<STRING> | All related sids |
| `source_tables` | ARRAY<STRING> | |
| `time_start_utc` | TIMESTAMP | |
| `time_end_utc` | TIMESTAMP | |
| `customer_id` | STRING | If customer-specific |
| `region` | STRING | |
| `business_domain` | STRING | |
| `incident_type` | STRING | `fraud`, `outage`, `billing_anomaly`, `support_spike`, `security_alert` |
| `severity` | STRING | `low`, `medium`, `high`, `critical` |
| `risk_band` | STRING | |
| `metrics_json` | STRING | |
| `data_quality_score` | FLOAT | |
| `generated_ts_utc` | TIMESTAMP | |

**4.1.5 `gld_rag_doc_kpi_explainer`**

Grain: one doc per KPI definition version. Static document for grounding metric definitions.

| Column | Type |
|--------|------|
| `doc_id` | STRING (PK) | `kpi_explain_{kpi_id}_{version}` |
| `doc_type` | STRING | `kpi_explainer` |
| `title` | STRING | e.g., `"KPI001 — Total Revenue (EGP) v1"` |
| `summary_text` | STRING | KPI definition, formula, business owner, calculation rules, examples |
| `kpi_id` | STRING | |
| `kpi_version` | STRING | |
| `business_owner` | STRING | |
| `formula` | STRING | |
| `grain` | STRING | |
| `generated_ts_utc` | TIMESTAMP | |

### 4.2 RAG Metadata Filters

All filterable metadata fields must be first-class columns for efficient filtering:

| Filter Field | Applies To Doc Types | Type |
|-------------|---------------------|------|
| `customer_id` | customer_daily, customer_30d, incident | STRING |
| `region` | All | STRING |
| `city` | customer_daily, incident | STRING |
| `business_domain` | domain_daily, incident, kpi_explainer | STRING |
| `risk_band` | customer_daily, customer_30d, incident | STRING |
| `time_start_utc` / `time_end_utc` | All | TIMESTAMP |
| `doc_type` | All | STRING |
| `incident_type` | incident | STRING |
| `severity` | incident | STRING |
| `segment` | domain_daily | STRING |

### 4.3 Vector Index Schema

**`gld_rag_vector_index`** — embedding + metadata for vector DB ingestion (ChromaDB/Qdrant-compatible).

| Column | Type | Description |
|--------|------|-------------|
| `vector_id` | STRING (PK) | Matches `doc_id` |
| `doc_id` | STRING | FK to RAG document |
| `doc_type` | STRING | |
| `embedding_model` | STRING | e.g., `text-embedding-3-small` |
| `embedding_model_version` | STRING | |
| `embedding_dimension` | INT | |
| `embedding_values` | ARRAY<FLOAT> | The actual vector (for parquet storage; downstream DB stores natively) |
| `chunk_index` | INT | Chunk number for split documents |
| `chunk_text` | STRING | The text that was embedded |
| `parent_doc_id` | STRING | FK to parent doc if chunked |
| `metadata_json` | STRING | All filterable metadata in JSON |
| `generated_ts_utc` | TIMESTAMP | |

---

## Part 5 — Governance Entities

### 5.1 Path Run Status

**`run_logs/path_run_status/run_status_{path}.parquet`**

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING (PK) | Deterministic: `{path}_{batch_id}_{ts}` |
| `path_name` | STRING | `dashboard` or `rag` |
| `batch_id` | STRING | |
| `run_ts_utc` | TIMESTAMP | Run start timestamp |
| `run_status` | STRING | `success`, `failed`, `partial` |
| `run_duration_seconds` | INT | |
| `domains_processed` | INT | Count of domains successfully processed |
| `domains_total` | INT | Total expected domains |
| `records_read` | INT | |
| `records_written` | INT | |
| `records_quarantined` | INT | |
| `quality_summary_json` | STRING | Per-domain quality scores |
| `output_tables` | ARRAY<STRING> | Tables published in this run |
| `schema_version` | STRING | |
| `orchestrator_version` | STRING | |
| `error_details` | STRING | Null if success, error message if failed |

### 5.2 Lineage Records

Lineage is embedded in each output record via:
- `batch_id` — ties to manifest.json
- `source_file` — ties to original source
- `lineage_batch_id` — ties fact/mart records to their processing batch
- `gold_ingest_ts_utc` — when Gold record was created
- `slv_schema_version` — Silver transformation version

Additional lineage tracking is stored as `run_logs/lineage/` in JSON manifest format per batch.

---

## Part 6 — Entity-Relationship Summary

```
Bronze (8 tables) ──ingest──▶ Silver (8 tables + 7 dims) ──conform──▶ Gold Dashboard (6 dims + 4 facts + 5 marts)
                                                                  ──generate──▶ Gold RAG (5 doc types + vector index)
                                                                               
                                   slv_quarantine ◀── (silver DQ failures)
                                   run_status ◀────── (per-path orchestration)
```

Each layer is stored as Parquet with Hive-style partitioning. Silver and Gold outputs include explicit lineage fields back to their source batch and Bronze records.
