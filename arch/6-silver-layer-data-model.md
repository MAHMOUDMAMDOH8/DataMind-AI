# Silver Layer Data Model

## Purpose

The Silver Layer contains cleansed, validated, standardized, and enriched datasets derived from Bronze events.

Primary objectives:

* Schema standardization
* Data quality enforcement
* Duplicate removal
* Timestamp normalization
* Event enrichment
* Business key standardization
* Iceberg optimization

---

# Design Principles

## Standard Naming

All Silver tables follow:

```text
silver.<domain>_<entity>
```

Examples:

```text
silver.billing_calls
silver.billing_sms
silver.crm_customers
silver.network_data_sessions
```

---

## Standard Metadata Columns

All Silver tables include:

| Column              | Description               |
| ------------------- | ------------------------- |
| event_timestamp     | Parsed UTC timestamp      |
| event_date          | Date partition column     |
| event_hour          | Hour partition helper     |
| ingestion_timestamp | ETL ingestion timestamp   |
| source_system       | Originating source system |

---

# CRM Domain

## silver.crm_customer_registrations

### Business Key

```text
customer_id
```

### Columns

| Column              |
| ------------------- |
| registration_sid    |
| customer_id         |
| phone_number        |
| plan_type           |
| behavior_profile    |
| city                |
| region              |
| registration_date   |
| registration_status |
| channel             |
| seasonal_multiplier |
| event_timestamp     |
| event_date          |

### Transformations

* customer → customer_id
* sid → registration_sid
* registration_date converted to DATE
* timestamp converted to TIMESTAMP

---

## silver.crm_profile_updates

### Business Key

```text
update_sid
```

### Columns

| Column              |
| ------------------- |
| update_sid          |
| customer_id         |
| phone_number        |
| updated_fields      |
| seasonal_multiplier |
| event_timestamp     |
| event_date          |

---

# Billing Domain

## silver.billing_calls

### Business Key

```text
call_sid
```

### Flattened Schema

| Column                |
| --------------------- |
| call_sid              |
| customer_id           |
| phone_number          |
| caller_phone_number   |
| receiver_phone_number |
| caller_cell_site      |
| receiver_cell_site    |
| caller_imei           |
| receiver_imei         |
| call_duration_seconds |
| call_type             |
| status                |
| amount                |
| currency              |
| fraud_indicator       |
| risk_score            |
| seasonal_multiplier   |
| event_timestamp       |
| event_date            |
| event_hour            |

### Transformations

Nested fields flattened:

```text
from.phone_number
to.phone_number
from.cell_site
to.cell_site
from.imei
to.imei
```

Billing information flattened:

```text
billing_info.amount
billing_info.currency
```

### Data Quality Rules

* call_sid not null
* call_duration_seconds >= 0
* amount >= 0
* valid phone format

---

## silver.billing_sms

### Business Key

```text
sms_sid
```

### Columns

| Column                |
| --------------------- |
| sms_sid               |
| customer_id           |
| sender_phone_number   |
| receiver_phone_number |
| sender_cell_site      |
| receiver_cell_site    |
| sender_imei           |
| receiver_imei         |
| message_body          |
| status                |
| amount                |
| currency              |
| registration_date     |
| seasonal_multiplier   |
| event_timestamp       |
| event_date            |

---

# Network Domain

## silver.network_data_sessions

### Business Key

```text
session_sid
```

### Columns

| Column                   |
| ------------------------ |
| session_sid              |
| customer_id              |
| phone_number             |
| behavior_profile         |
| data_used_mb             |
| data_type                |
| network_type             |
| session_duration_seconds |
| status                   |
| fraud_indicator          |
| risk_score               |
| seasonal_multiplier      |
| event_timestamp          |
| event_date               |

### Data Quality Rules

* data_used_mb >= 0
* session_duration_seconds >= 0

---

## silver.network_metrics

### Business Key

```text
metric_sid
```

### Columns

| Column                 |
| ---------------------- |
| metric_sid             |
| cell_site_id           |
| city                   |
| region                 |
| network_type           |
| active_subscribers     |
| total_throughput_mbps  |
| cpu_utilization_pct    |
| memory_utilization_pct |
| event_timestamp        |
| event_date             |

---

## silver.qos_reports

### Business Key

```text
qos_sid
```

### Columns

| Column              |
| ------------------- |
| qos_sid             |
| cell_site_id        |
| city                |
| region              |
| network_type        |
| mos_score_avg       |
| jitter_ms_avg       |
| packet_loss_pct_avg |
| latency_ms_avg      |
| throughput_mbps_avg |
| sample_size         |
| event_timestamp     |
| event_date          |

---

# Payment Domain

## silver.payments

### Business Key

```text
payment_sid
```

### Columns

| Column              |
| ------------------- |
| payment_sid         |
| customer_id         |
| phone_number        |
| transaction_id      |
| invoice_number      |
| payment_type        |
| payment_method      |
| payment_amount      |
| status              |
| seasonal_multiplier |
| event_timestamp     |
| event_date          |

### Derived Columns

| Column        | Logic             |
| ------------- | ----------------- |
| is_successful | status='SUCCESS'  |
| is_failed     | status!='SUCCESS' |

---

# Recharge Domain

## silver.recharges

### Business Key

```text
recharge_sid
```

### Columns

| Column              |
| ------------------- |
| recharge_sid        |
| customer_id         |
| phone_number        |
| transaction_id      |
| recharge_amount     |
| balance_before      |
| balance_after       |
| payment_method      |
| status              |
| seasonal_multiplier |
| event_timestamp     |
| event_date          |

---

# Roaming Domain

## silver.roaming_events

### Business Key

```text
roaming_sid
```

### Columns

| Column              |
| ------------------- |
| roaming_sid         |
| customer_id         |
| phone_number        |
| roaming_country     |
| roaming_operator    |
| roaming_type        |
| duration_seconds    |
| data_used_mb        |
| roaming_charges     |
| status              |
| seasonal_multiplier |
| event_timestamp     |
| event_date          |

---

# Support Domain

## silver.support_tickets

### Business Key

```text
ticket_id
```

### Columns

| Column          |
| --------------- |
| ticket_sid      |
| ticket_id       |
| customer_id     |
| phone_number    |
| channel         |
| reason          |
| priority        |
| status          |
| event_timestamp |
| event_date      |

---

## silver.complaints

### Business Key

```text
complaint_sid
```

### Columns

| Column             |
| ------------------ |
| complaint_sid      |
| customer_id        |
| phone_number       |
| complaint_category |
| severity           |
| description        |
| event_timestamp    |
| event_date         |

---

## silver.ticket_resolutions

### Business Key

```text
resolution_sid
```

### Columns

| Column                  |
| ----------------------- |
| resolution_sid          |
| customer_id             |
| phone_number            |
| agent_id                |
| resolution_time_seconds |
| wait_time_seconds       |
| satisfaction_score      |
| first_call_resolution   |
| escalated               |
| call_back_requested     |
| event_timestamp         |
| event_date              |

---

# Iceberg Partition Strategy

| Table                 | Partition  |
| --------------------- | ---------- |
| billing_calls         | event_date |
| billing_sms           | event_date |
| network_data_sessions | event_date |
| network_metrics       | event_date |
| qos_reports           | event_date |
| payments              | event_date |
| recharges             | event_date |
| roaming_events        | event_date |
| support_tickets       | event_date |
| complaints            | event_date |
| ticket_resolutions    | event_date |

---

# Bronze to Silver Lineage

```text
CustomerRegistration
        ↓
silver.crm_customer_registrations

CustomerProfileUpdate
        ↓
silver.crm_profile_updates

CallCDR
        ↓
silver.billing_calls

SMSEvent
        ↓
silver.billing_sms

DataSessionEvent
        ↓
silver.network_data_sessions

NetworkMetric
        ↓
silver.network_metrics

QoSReport
        ↓
silver.qos_reports

PaymentEvent
        ↓
silver.payments

RechargeEvent
        ↓
silver.recharges

RoamingEvent
        ↓
silver.roaming_events

SupportTicketCreated
        ↓
silver.support_tickets

ComplaintFiled
        ↓
silver.complaints

TicketResolved
        ↓
silver.ticket_resolutions
```
