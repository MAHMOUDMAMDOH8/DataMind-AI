# RAG System Data Model — Clarification Document

**Based on**: Spec Kit feature `001-rag-dashboard-paths`  
**Document purpose**: Explain how data flows from Silver through to RAG-serving, what entities exist, how they connect, and how retrieval works

---

## 1. RAG Data Flow Overview

```
Silver Layer (source)
│
├── slv_call_events
├── slv_sms_events
├── slv_data_usage_events
├── slv_payment_events
├── slv_recharge_events
├── slv_roaming_events
├── slv_security_events
├── slv_support_events
│
├── slv_customer_event_timeline      ← ordered stream per customer (all domains)
├── slv_customer_daily_metrics       ← daily aggregates per customer
├── slv_segment_daily_metrics        ← daily aggregates per segment/region
│
└── slv_dim_customer, slv_dim_date, slv_dim_geo, slv_dim_channel
         │
         ▼
Gold RAG Layer (document generation)
         │
├── gld_rag_doc_customer_daily       ← one doc per customer per day
├── gld_rag_doc_customer_30d         ← one doc per customer per 30d window
├── gld_rag_doc_domain_daily         ← one doc per domain per day
├── gld_rag_doc_incident             ← one doc per detected anomaly
├── gld_rag_doc_kpi_explainer        ← static doc per KPI definition
│
└── gld_rag_vector_index             ← embeddings + metadata for vector DB
         │
         ▼
Serving Layer (vector DB + LLM orchestration)
         │
    ChromaDB / Qdrant / similar       ← hybrid search (semantic + metadata filters)
    ↓
    LLM (answer generation with evidence)
```

---

## 2. Core Entities in the RAG Data Model

### 2.1 Source Entity: Silver Event

This is what documents are built **from**. Each document cites one or more events.

| Attribute | Meaning | Used in RAG for |
|-----------|---------|-----------------|
| `sid` | Unique event id | **Evidence/citation** in `evidence_event_ids` |
| `customer_id` | Canonical customer key | **Filter** + doc grain key |
| `event_ts_utc` | Cleaned event timestamp | Time window calculation |
| `event_date` | Partition date | Daily doc grain |
| `event_type` | Domain name | Domain identification |
| `domain` (derived) | `usage`, `finance`, `support`, `risk`, `roaming` | **Metadata filter** |
| `amount_egp` | Parsed billing amount | Revenue context in narrative |
| `status` | Event status | Narrative (e.g., "failed payment") |
| `behavior_profile` | Customer segment | Segment breakdown |
| `region`, `city` | Geographic location | **Metadata filter** |
| `risk_score` | Risk value | Risk context |
| `dq_quality_score` | Record quality | `data_quality_score` in doc |

The Silver `slv_customer_event_timeline` view joins all 8 domain tables ordered by time per customer — this is the **primary input** for customer docs.

### 2.2 RAG Document

A document is a retrieval-ready knowledge unit. It is **not** a raw event — it is a structured narrative with evidence links.

Every document has three sections:

```
┌─ Document ──────────────────────────────────────────────┐
│                                                          │
│  doc_id        = "cust_daily_0100XXXXXX_2026-06-06"      │
│  doc_type      = "customer_daily"                        │
│                                                          │
│  title         = "Customer 0100XXXXXX daily — 2026-06-06" │
│                                                          │
│  summary_text =                                           │
│    "Customer had 3 calls (total 12 min), 2 SMS           │
│     (1 failed), 45 MB data usage, 1 successful           │
│     recharge of 50 EGP. No support or risk events."       │
│                                                          │
│  evidence_event_ids = ["CA123...", "SM456...", ...]       │
│                                                          │
│  metadata filters:                                        │
│    customer_id, region, business_domain, risk_band        │
│    doc_type, time_start_utc, time_end_utc                 │
│                                                          │
│  metrics_json = {                                         │
│    "call_count": 3, "total_duration_sec": 720,           │
│    "total_data_mb": 45, "total_payment_egp": 0,          │
│    "support_count": 0, "max_risk_score": 0                │
│  }                                                        │
│                                                          │
│  data_quality_score = 0.95                                │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 2.3 Vector Index Entry

When a document is chunked and embedded, each chunk becomes a vector index entry.

```
┌─ Vector Index Entry ─────────────────────────────────────┐
│                                                           │
│  vector_id       = "cust_daily_0100XXXXXX_2026-06-06_chunk_0" │
│  doc_id          = "cust_daily_0100XXXXXX_2026-06-06"    │
│  doc_type        = "customer_daily"                      │
│                                                           │
│  embedding_model = "text-embedding-3-small"               │
│  embedding_dim   = 1536                                   │
│                                                           │
│  chunk_index     = 0                                      │
│  chunk_text      = "Customer had 3 calls (total 12 min)…" │
│                                                           │
│  metadata_json   = { all filterable fields }              │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

---

## 3. Document Types — Generation Logic and Grain

### 3.1 `customer_daily`

| Aspect | Detail |
|--------|--------|
| **Grain** | One doc per `customer_id` per `event_date` |
| **Source** | `slv_customer_event_timeline` filtered to one day, grouped by `customer_id` |
| **Summary structure** | 1 paragraph per domain present + total metrics paragraph |
| **Evidence** | All `sid` values for events in that day for that customer |
| **Metrics** | Call count, total duration, SMS count, total data MB, payment total, support count, max risk score |
| **Risk band** | Derived from `max_risk_score`: 0=low, 1-30=medium, 31-70=high, 71+=critical |
| **Example question it answers** | "What did customer 0100XXXXXX do yesterday?" |

**How it is built**:
```
1. SELECT * FROM slv_customer_event_timeline
   WHERE customer_id = ? AND event_date = ?
2. Group events by domain
3. For each domain group → generate one sentence/paragraph
4. Collect all sids → evidence_event_ids
5. Compute metrics → metrics_json
6. Determine risk_band from max risk_score
```

### 3.2 `customer_30d`

| Aspect | Detail |
|--------|--------|
| **Grain** | One doc per `customer_id` per rolling 30-day window |
| **Source** | `slv_customer_event_timeline` filtered to last 30 days |
| **Summary structure** | Trends, week-over-week changes, anomalies, churn signals |
| **Evidence** | All sids from last 30 days for that customer |
| **Metrics** | 30-day totals + `total_revenue_30d`, `days_active`, `churn_risk_score` |
| **Example question** | "Summarize this customer's last month." |

**Churn risk heuristic**:
- If `days_active < 10` AND `total_revenue_30d < 50 AND support_count > 3` → `risk_flag = "high"`
- If `days_active > 25 AND total_revenue_30d > 200` → `risk_flag = "low"`
- Otherwise → `"medium"`

### 3.3 `domain_daily`

| Aspect | Detail |
|--------|--------|
| **Grain** | One doc per `business_domain` per `event_date` (optional segment/region split) |
| **Source** | `slv_segment_daily_metrics` + individual domain Silver tables |
| **Summary structure** | Domain totals, notable trends, segment breakdown, top regions |
| **Evidence** | Representative (top-5 by impact) sids |
| **Example question** | "What happened in payments today?" |

**Domains mapped from event types**:

| Event Type | Business Domain |
|------------|-----------------|
| call, sms, data_usage | `usage` |
| payment, recharge | `finance` |
| support | `support` |
| security | `risk` |
| roaming | `roaming` |

### 3.4 `incident`

| Aspect | Detail |
|--------|--------|
| **Grain** | One doc per detected anomaly cluster |
| **Source** | Silver aggregates + threshold rules |
| **Detection triggers** | See below |
| **Evidence** | All sids related to the anomaly |
| **Example question** | "Were there any security incidents yesterday?" |

**Incident detection rules**:
- `fraud`: midnight data spikes (`BOTNET_ACTIVITY` marker) or international short calls (`WANGIRI_FRAUD`)
- `billing_anomaly`: payment failure rate > 30% OR recharge failure rate > 30%
- `support_spike`: support case volume > 2x rolling 7-day average
- `security_alert`: security event count > 5 in one day or any `sim_swap`/`porting` cluster
- `outage`: network metrics show > 10% handover failure OR MOS < 2.0 AND > 50% of sessions affected

### 3.5 `kpi_explainer`

| Aspect | Detail |
|--------|--------|
| **Grain** | One doc per KPI per version |
| **Source** | KPI registry (`kpi_definitions.py`) |
| **Summary** | Definition, formula, interpretation, owner, data sources |
| **Evidence** | Empty (explainers do not cite events) |
| **Example question** | "How is ARPU calculated?" |

---

## 4. Evidence and Citation Model

This is the foundation of **grounded RAG**. Every claim in a document must be traceable.

```
Document claim:
  "Customer had 3 failed payment attempts today."
        │
        ▼
evidence_event_ids:
  ["PAY123456789", "PAY123456790", "PAY123456791"]
        │
        ▼
Silver slv_payment_events:
  PAY123456789: status="failed", amount=50, timestamp=2026-06-06 08:12:00
  PAY123456790: status="failed", amount=50, timestamp=2026-06-06 08:15:00
  PAY123456791: status="failed", amount=50, timestamp=2026-06-06 08:18:00
        │
        ▼
Bronze (original source preserved for full audit)
```

**Rules**:
- Every non-KPI doc must have >= 1 evidence `sid`
- At least 95% of documents must have valid evidence references
- KPI explainer docs are the only exception (no evidence needed)
- Evidence integrity is checked at contract validation time

---

## 5. Metadata Filtering Model

Metadata filters are the primary way to narrow retrieval scope **before** vector search.

### 5.1 Filter Hierarchy

```
Question: "What happened with high-risk customers in Cairo yesterday?"

Filters applied (pre-retrieval):
  doc_type IN ("customer_daily", "customer_30d")
  region = "Cairo"
  risk_band IN ("high", "critical")
  time_start_utc >= "2026-06-05"
  time_end_utc <= "2026-06-06"

Then: vector similarity on filtered subset
```

### 5.2 Filter Fields per Document Type

| Filter Field | customer_daily | customer_30d | domain_daily | incident | kpi_explainer |
|-------------|:-:|:-:|:-:|:-:|:-:|
| `customer_id` | ✓ | ✓ | | ✓ | |
| `region` | ✓ | ✓ | ✓ | ✓ | |
| `city` | ✓ | ✓ | | ✓ | |
| `business_domain` | | | ✓ | ✓ | ✓ |
| `risk_band` | ✓ | ✓ | ✓ | ✓ | |
| `doc_type` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `incident_type` | | | | ✓ | |
| `severity` | | | | ✓ | |
| `segment` | | | ✓ | | |
| `time_start_utc` | ✓ | ✓ | ✓ | ✓ | |
| `time_end_utc` | ✓ | ✓ | ✓ | ✓ | |

---

## 6. Chunking Model

Documents need to be chunked for embedding and retrieval. The chunking strategy depends on doc type.

| Doc Type | Chunk Size (tokens) | Strategy | Overlap |
|----------|-------------------|----------|---------|
| customer_daily | 400-600 | By domain paragraph | 50 tokens |
| customer_30d | 600-900 | By week or domain section | 100 tokens |
| domain_daily | 500-800 | By segment/region sub-section | 80 tokens |
| incident | Keep atomic | No split unless > 1200 tokens | N/A |
| kpi_explainer | Keep atomic | No split | N/A |

**Chunking must never split**: an evidence reference from its corresponding claim, or a metrics block from its narrative.

---

## 7. Embedding Model

### Format
The text that is embedded is the `chunk_text` field. This includes:
```
[Context header: doc_type, customer_id if present, date window, domain, region]
[Summary paragraph or section]
```

### What is NOT embedded by default
- `metrics_json` (too dense/structured for semantic search)
- `evidence_event_ids` (used for citation, not embedding)
- `metadata_json` (used for filtering, not embedding)
- Source table names

Exception: if metrics_json values are described in natural language in the summary, they are naturally embedded as part of `chunk_text`.

---

## 8. Retrieval Model (for Serving)

```
User Question
      │
      ▼
1. Classify intent (customer-level? domain-level? KPI definition?)
      │
      ▼
2. Apply metadata pre-filters:
   - date range from question context (or last 7 days default)
   - doc_type from intent classification
   - customer_id if mentioned
   - domain/region if mentioned
      │
      ▼
3. Vector similarity search on filtered subset
   - top-k candidates (k=5..15 depending on context budget)
      │
      ▼
4. Optional BM25/rerank step
   - boost docs with higher evidence count or data_quality_score
      │
      ▼
5. Assemble context:
   - selected docs ordered by relevance
   - include evidence_event_ids for citation
   - truncate if context budget exceeded
      │
      ▼
6. LLM generates answer with citations
```

### Intended Retrieval Modes

| Mode | Primary Filter | Example Question |
|------|---------------|------------------|
| Customer mode | `customer_id` + date range | "What happened with customer 0100XXXXXX this week?" |
| Operations mode | `business_domain` + date range + region | "How are support cases trending in Cairo?" |
| Executive mode | `doc_type IN (domain_daily, incident)` + date range | "What were the top incidents yesterday?" |
| Definition mode | `doc_type = kpi_explainer` | "How is ARPU calculated?" |

---

## 9. Data Quality Model

Quality is tracked per document based on the source Silver records that contributed.

| Score Range | Meaning | Action |
|-------------|---------|--------|
| 1.0 | All source records pass all DQ checks | Index normally |
| 0.8-0.99 | Some source records have minor issues (e.g., null optional field) | Index normally |
| 0.5-0.79 | Significant source quality issues (e.g., 20% records flagged) | Down-rank in retrieval |
| < 0.5 | Majority of source records have quality issues | Document is flagged, not indexed until reprocessed |

The `data_quality_score` is computed as:
```
quality_score = (records_passing_all_checks / total_records_used) * 1.0
```

---

## 10. Summary: What the RAG Data Model Enables

| Business Use Case | Relevant Doc Type(s) | How It Works |
|-------------------|---------------------|--------------|
| Customer account review | customer_daily, customer_30d | Filter by `customer_id` + date, retrieve narrative, cite events |
| Operational trend analysis | domain_daily | Filter by `business_domain` + date range, get segment/region breakdown |
| Incident investigation | incident | Filter by `incident_type` + `severity`, get root cause + impact |
| KPI grounding | kpi_explainer | Retrieve exact definition, formula, owner |
| Multi-hop reasoning (e.g., "why did ARPU drop?") | domain_daily + incident + customer_30d | Retrieve domain daily → find anomaly → drill into related incident → customer-level patterns |
| Time-series comparison | domain_daily, customer_30d | Retrieve same doc type across two date ranges, compare metrics |
| Churn risk exploration | customer_30d | Filter by `risk_band IN ("high","critical")`, get 30-day patterns |

---

## 11. File Layout in `gold_layer/rag/`

```
gold_layer/rag/
├── gld_rag_doc_customer_daily/
│   ├── batch_id=20260606_v1/
│   │   └── *.parquet                 ← partition by batch_id
│   └── ...
├── gld_rag_doc_customer_30d/
├── gld_rag_doc_domain_daily/
├── gld_rag_doc_incident/
├── gld_rag_doc_kpi_explainer/       ← static, updated on version change only
└── gld_rag_vector_index/
    ├── batch_id=20260606_v1/
    │   └── *.parquet                 ← chunked embeddings + metadata
    └── ...
```
