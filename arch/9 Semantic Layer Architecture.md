# Semantic Layer Architecture

# Overview

The Semantic Layer provides a business-friendly abstraction on top of Gold Layer datasets.

Its purpose is to bridge the gap between:

* Business Users
* BI Tools
* Text-to-SQL
* AI Assistants
* Physical Database Structures

Instead of exposing raw table and column names, the Semantic Layer exposes trusted business concepts, metrics, dimensions, and relationships.

---

# Why Semantic Layer

Without a Semantic Layer:

```text
User
  ↓

LLM

  ↓

Raw Database Schema

  ↓

Complex SQL
```

Problems:

* Ambiguous business terminology
* Inconsistent metric definitions
* SQL generation errors
* Duplicate business logic

---

With a Semantic Layer:

```text
User
  ↓

LLM

  ↓

Semantic Layer

  ↓

Trino

  ↓

Iceberg Gold
```

Benefits:

* Consistent business definitions
* Better SQL generation
* Reduced hallucinations
* Easier self-service analytics

---

# Position in DataMind AI

```text
Iceberg Gold
      │
      ▼

    Trino
      │
      ▼

Semantic Layer
      │

 ┌────┼──────────┬─────────┐
 │    │          │         │

BI Text-to-SQL APIs AI Chat
```

---

# Core Components

## Metrics

Metrics are reusable business calculations.

Examples:

```text
Revenue
ARPU
Active Customers
Churn Rate
Payment Success Rate
```

---

## Dimensions

Dimensions provide filtering and grouping capabilities.

Examples:

```text
Customer
Region
City
Date
Country
Payment Method
Plan Type
```

---

## Entities

Entities represent business objects.

Examples:

```text
Customer
Payment
Recharge
Roaming Event
Support Ticket
Cell Site
```

---

## Relationships

Relationships describe how entities connect.

Example:

```text
Customer
   │
   ├── Payments
   │
   ├── Recharges
   │
   ├── Roaming Events
   │
   └── Support Tickets
```

---

# Business Metrics

## Revenue

Description:

Total telecom revenue generated from all services.

Formula:

```sql
SUM(total_revenue)
```

Source:

```text
gold.daily_revenue
```

---

## Active Customers

Description:

Customers with activity during a given period.

Formula:

```sql
COUNT(DISTINCT customer_id)
```

Source:

```text
gold.customer_usage_daily
```

---

## ARPU

Description:

Average Revenue Per User.

Formula:

```sql
SUM(total_revenue)
/ COUNT(DISTINCT customer_id)
```

---

## Payment Success Rate

Description:

Percentage of successful payment transactions.

Formula:

```sql
SUM(successful_transactions)
/
SUM(transaction_count)
```

---

## Average Recharge Amount

Description:

Average customer recharge value.

Formula:

```sql
AVG(recharge_amount)
```

---

# Dimension Definitions

## Customer

Source:

```text
gold.customer_360
```

Attributes:

```text
customer_id
phone_number
plan_type
city
region
behavior_profile
customer_segment
```

---

## Date

Common fields:

```text
date
month
quarter
year
week
day_of_week
```

---

## Geography

Fields:

```text
city
region
country
```

---

## Payment Method

Fields:

```text
Credit Card
Debit Card
Wallet
Bank Transfer
Cash
```

---

# Semantic Model Example

## YAML Representation

```yaml
metrics:

  revenue:
    label: Revenue
    description: Total telecom revenue
    sql: SUM(total_revenue)
    table: gold.daily_revenue

  active_customers:
    label: Active Customers
    description: Distinct active customers
    sql: COUNT(DISTINCT customer_id)
    table: gold.customer_usage_daily

  arpu:
    label: ARPU
    description: Average Revenue Per User
    sql: SUM(total_revenue) /
         COUNT(DISTINCT customer_id)
    table: gold.customer_360
```

---

# Text-to-SQL Integration

User Question:

```text
What was the ARPU last month?
```

---

Semantic Layer Context:

```yaml
metric:
  arpu

definition:
  Average Revenue Per User

formula:
  revenue / active_customers
```

---

Generated SQL:

```sql
SELECT
SUM(total_revenue)
/ COUNT(DISTINCT customer_id)
AS arpu
FROM gold.customer_360
WHERE month(last_activity_date)=5;
```

---

# AI Assistant Flow

```text
User Question
        │
        ▼

     OpenAI
        │
        ▼

 Semantic Layer
        │
        ▼

 SQL Generation
        │
        ▼

      Trino
        │
        ▼

  Iceberg Gold
        │
        ▼

     Results
        │
        ▼

      User
```

---

# Power BI Integration

Power BI can use the Semantic Layer definitions to ensure consistent metrics.

Example:

```text
Revenue

ARPU

Active Customers

Churn Rate
```

Every dashboard uses the same business definitions.

---

# Governance Benefits

The Semantic Layer provides:

* Centralized metric definitions
* Consistent KPIs
* Reduced business logic duplication
* Better AI query accuracy
* Easier auditing

---

# Initial Implementation

For DataMind AI, the first version can be implemented using simple YAML files.

Example Structure:

```text
semantic-layer
│
├── metrics.yaml
├── dimensions.yaml
├── entities.yaml
└── relationships.yaml
```

No additional infrastructure is required.

---

# Future Evolution

The Semantic Layer can later be migrated to:

* dbt Semantic Layer
* Cube
* AtScale
* MetricFlow

without changing the Gold Layer design.

---

# Relationship with RAG

Semantic Layer and RAG serve different purposes.

## Semantic Layer

Used for:

```text
Analytics Questions
Business Metrics
SQL Generation
KPI Definitions
```

Example:

```text
What was revenue last month?
```

---

## RAG

Used for:

```text
Policies
Documentation
Runbooks
Knowledge Articles
Architecture Documents
```

Example:

```text
How is churn score calculated?
```

---

# Summary

The Semantic Layer is the business knowledge layer of DataMind AI.

It sits between Trino and AI consumers, providing:

* Business Metrics
* Dimensions
* KPI Definitions
* Entity Relationships
* Consistent SQL Generation

The Semantic Layer is a foundational component for reliable Text-to-SQL and future AI-powered analytics.
