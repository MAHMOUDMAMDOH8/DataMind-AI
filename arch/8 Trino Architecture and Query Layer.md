## Overview

Trino is the central query engine of the DataMind AI platform.

Its primary role is to provide fast, distributed SQL access to curated business datasets stored in Apache Iceberg.

Trino serves as the execution layer for:

- Power BI Dashboards
- Business Analytics
- Semantic Layer
- Text-to-SQL
- AI Assistant
- External APIs
# Trino Architecture and Query Layer (Sequence-Based View)  
  
## Overview  
  
Trino is the central query engine of the DataMind AI platform.  
  
It provides distributed SQL access over Iceberg Gold data and acts as the execution layer for analytics, BI, and AI workloads.  
  
---  
  
# SQL Query Execution Flow  
  
```mermaid  
sequenceDiagram  
participant User  
participant Trino  
participant IcebergGold as Iceberg Gold  
  
User->>Trino: Submit SQL Query  
Trino->>IcebergGold: Execute distributed scan  
IcebergGold-->>Trino: Return data blocks  
Trino-->>User: Return query result

```
Spark = Build Data  Trino = Query Data


```mermaid 
flowchart TD  
  
A[Source Systems] --> B[Kafka]  
B --> C[NiFi]  
C --> D[Iceberg Bronze]  
D --> E[Spark Processing]  
E --> F[Iceberg Silver]  
F --> G[Spark Processing]  
G --> H[Iceberg Gold]  
H --> I[Trino]  
  
I --> J[Semantic Layer]  
  
J --> K[BI Tools]  
J --> L[APIs]  
J --> M[Text-to-SQL]  
J --> N[AI Chat]
```

# Responsibilities

## SQL Query Execution

Trino executes analytical SQL queries against Iceberg tables.

# Power BI Integration
```mermaid 
sequenceDiagram  
participant PowerBI as Power BI  
participant Trino  
participant IcebergGold as Iceberg Gold  
  
PowerBI->>Trino: DAX/SQL Query Request  
Trino->>IcebergGold: Translate + Execute SQL  
IcebergGold-->>Trino: Aggregated Data  
Trino-->>PowerBI: Visualization-ready dataset
```

Benefits:

- Fast dashboard performance
- Centralized query engine
- Consistent business metrics
- Reduced direct storage access

# Semantic Layer Backend
```mermaid 
sequenceDiagram  
participant Semantic as Semantic Layer  
participant Trino  
participant IcebergGold as Iceberg Gold  
  
Semantic->>Trino: Business metric request  
Trino->>IcebergGold: Run optimized SQL  
IcebergGold-->>Trino: Raw + aggregated data  
Trino-->>Semantic: Consistent metric response
```

# Text-to-SQL Execution Engine

```mermaid
sequenceDiagram
    participant User
    participant LLM as OpenAI LLM
    participant SQLGen as SQL Generator
    participant Trino
    participant IcebergGold as Iceberg Gold

    User->>LLM: Ask natural language question
    LLM->>SQLGen: Interpret intent
    SQLGen-->>LLM: Generate SQL query
    LLM->>Trino: Execute SQL
    Trino->>IcebergGold: Query tables
    IcebergGold-->>Trino: Dataset result
    Trino-->>LLM: Return results
    LLM-->>User: Natural language answer
```

# AI Assistant Query Flow
```mermaid
sequenceDiagram
    participant Assistant as AI Assistant
    participant TS as Text-to-SQL Layer
    participant Trino
    participant IcebergGold as Iceberg Gold

    Assistant->>TS: User question
    TS->>Trino: Generated SQL query
    Trino->>IcebergGold: Execute query
    IcebergGold-->>Trino: Results
    Trino-->>TS: Structured data
    TS-->>Assistant: Context + answer generation
```

# # Iceberg Catalog Resolution Flow

```mermaid
sequenceDiagram
    participant Trino
    participant Catalog as Iceberg Catalog
    participant Storage as S3 / MinIO
    participant Tables as Bronze/Silver/Gold

    Trino->>Catalog: Resolve table metadata
    Catalog->>Storage: Fetch metadata files
    Storage-->>Catalog: Table schema + snapshots
    Catalog-->>Trino: Resolved table plan

    Trino->>Tables: Execute query plan
    Tables-->>Trino: Data fragments
```

# Summary

Trino acts as a **query execution layer only**, responsible for:

- SQL execution
- BI query serving
- Semantic layer access
- Text-to-SQL execution
- AI-assisted analytics
- Secure data access over Iceberg

It does NOT perform ingestion, streaming, or storage operations.