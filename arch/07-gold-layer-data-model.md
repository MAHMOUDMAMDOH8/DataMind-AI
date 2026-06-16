

# Gold Layer Data Model

## Purpose

The Gold Layer contains curated business-ready datasets optimized for:

- Executive Reporting
    
- Self-Service Analytics
    
- Power BI Dashboards
    
- Text-to-SQL
    
- Machine Learning
    
- AI Applications
    
- KPI Reporting
    

Unlike the Silver Layer, Gold tables are business-oriented rather than event-oriented.

---

# Design Principles

## Business-Centric Modeling

Silver:

```text
billing_calls
billing_sms
payments
recharges
```

Gold:

```text
customer_360
daily_revenue
roaming_analytics
network_performance
```

---

## Semantic Layer Ready

Each Gold table should expose:

- Business Metrics
    
- Business Dimensions
    
- KPI Definitions
    

This allows future integration with:

- Trino
    
- Semantic Layer
    
- Text-to-SQL
    
- Power BI
    

---

# Customer Domain

## gold.customer_360

### Purpose

Unified customer profile containing usage, revenue, roaming, support, and behavioral information.

### Grain

```text
One row per customer
```

### Business Key

```text
customer_id
```

### Columns

|Column|
|---|
|customer_id|
|phone_number|
|plan_type|
|city|
|region|
|behavior_profile|
|registration_date|
|customer_segment|
|total_calls|
|total_sms|
|total_data_usage_mb|
|total_roaming_events|
|total_roaming_charges|
|total_payments|
|total_recharges|
|total_revenue|
|avg_call_duration|
|avg_session_duration|
|total_tickets|
|total_complaints|
|last_activity_date|
|customer_lifetime_value|
|churn_score|

### Source Tables

```text
silver.crm_customer_registrations
silver.billing_calls
silver.billing_sms
silver.network_data_sessions
silver.payments
silver.recharges
silver.roaming_events
silver.support_tickets
silver.complaints
```

---

# Revenue Domain

## gold.daily_revenue

### Purpose

Daily financial performance reporting.

### Grain

```text
One row per day
```

### Columns

|Column|
|---|
|revenue_date|
|call_revenue|
|sms_revenue|
|roaming_revenue|
|payment_revenue|
|recharge_revenue|
|total_revenue|
|successful_payments|
|failed_payments|
|active_customers|

### Source Tables

```text
silver.billing_calls
silver.billing_sms
silver.payments
silver.recharges
silver.roaming_events
```

### Common Queries

```text
What is today's revenue?

Monthly revenue trend

Revenue by service type
```

---

# Customer Usage Domain

## gold.customer_usage_daily

### Purpose

Daily customer usage metrics.

### Grain

```text
One row per customer per day
```

### Columns

|Column|
|---|
|usage_date|
|customer_id|
|calls_count|
|total_call_duration_seconds|
|sms_count|
|total_data_usage_mb|
|roaming_usage_mb|
|roaming_events|
|session_count|
|avg_session_duration_seconds|

### Source Tables

```text
silver.billing_calls
silver.billing_sms
silver.network_data_sessions
silver.roaming_events
```

---

# Payment Analytics

## gold.payment_analytics

### Purpose

Payment performance monitoring.

### Grain

```text
One row per day and payment method
```

### Columns

|Column|
|---|
|payment_date|
|payment_method|
|transaction_count|
|successful_transactions|
|failed_transactions|
|total_amount|
|avg_transaction_amount|
|success_rate|

### Source Tables

```text
silver.payments
```

---

# Recharge Analytics

## gold.recharge_analytics

### Purpose

Recharge performance analysis.

### Grain

```text
One row per day and payment method
```

### Columns

|Column|
|---|
|recharge_date|
|payment_method|
|recharge_count|
|total_recharge_amount|
|avg_recharge_amount|
|unique_customers|

### Source Tables

```text
silver.recharges
```

---

# Roaming Analytics

## gold.roaming_analytics

### Purpose

International roaming performance reporting.

### Grain

```text
One row per country per day
```

### Columns

|Column|
|---|
|roaming_date|
|roaming_country|
|roaming_operator|
|roaming_type|
|roaming_events|
|unique_customers|
|total_duration_seconds|
|total_data_used_mb|
|total_roaming_charges|

### Source Tables

```text
silver.roaming_events
```

### Common Queries

```text
Top roaming countries

Highest roaming revenue

International usage trends
```

---

# Network Performance Domain

## gold.network_performance

### Purpose

Network health and service quality monitoring.

### Grain

```text
One row per cell site per day
```

### Columns

|Column|
|---|
|performance_date|
|cell_site_id|
|city|
|region|
|network_type|
|avg_active_subscribers|
|avg_throughput_mbps|
|avg_cpu_utilization_pct|
|avg_memory_utilization_pct|
|avg_mos_score|
|avg_jitter_ms|
|avg_packet_loss_pct|
|avg_latency_ms|
|network_health_score|

### Source Tables

```text
silver.network_metrics
silver.qos_reports
```

### Derived Metric

```text
network_health_score

Derived from:

MOS
Jitter
Packet Loss
Latency
```

---

# Support Analytics

## gold.support_analytics

### Purpose

Customer support performance reporting.

### Grain

```text
One row per day
```

### Columns

|Column|
|---|
|support_date|
|tickets_created|
|tickets_resolved|
|complaints_received|
|avg_resolution_time_seconds|
|avg_wait_time_seconds|
|avg_satisfaction_score|
|escalation_rate|
|callback_request_rate|
|first_call_resolution_rate|

### Source Tables

```text
silver.support_tickets
silver.complaints
silver.ticket_resolutions
```

---

# Fraud Monitoring

## gold.fraud_monitoring

### Purpose

Fraud and risk monitoring.

### Grain

```text
One row per customer per day
```

### Columns

|Column|
|---|
|fraud_date|
|customer_id|
|suspicious_calls|
|suspicious_sessions|
|avg_risk_score|
|max_risk_score|
|fraud_events|

### Source Tables

```text
silver.billing_calls
silver.network_data_sessions
```

### Future ML Usage

```text
Fraud Detection
Risk Classification
Anomaly Detection
```

---

# Semantic Layer Metrics

## Revenue

```sql
SUM(total_revenue)
```

---

## Active Customers

```sql
COUNT(DISTINCT customer_id)
```

---

## ARPU

```sql
SUM(total_revenue)
/ COUNT(DISTINCT customer_id)
```

---

## Churn Rate

```sql
AVG(churn_score)
```

---

## Average Revenue Per Recharge

```sql
AVG(recharge_amount)
```

---

## Payment Success Rate

```sql
successful_transactions
/
transaction_count
```

---

# Iceberg Partition Strategy

|Table|Partition|
|---|---|
|customer_360|region|
|daily_revenue|revenue_date|
|customer_usage_daily|usage_date|
|payment_analytics|payment_date|
|recharge_analytics|recharge_date|
|roaming_analytics|roaming_date|
|network_performance|performance_date|
|support_analytics|support_date|
|fraud_monitoring|fraud_date|

---

# AI Consumption Layer

These Gold tables are designed to power:

```text
Power BI
     │

Trino
     │

Semantic Layer
     │

Text-to-SQL
     │

LLM Applications
```

---

# Gold Layer Lineage

```text
Silver Layer
      ↓

customer_360

daily_revenue

customer_usage_daily

payment_analytics

recharge_analytics

roaming_analytics

network_performance

support_analytics

fraud_monitoring

      ↓

Trino

      ↓

Semantic Layer

      ↓

Power BI
Text-to-SQL
AI Assistant
ML Models
```