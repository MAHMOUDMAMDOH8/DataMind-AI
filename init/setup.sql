-- DataMind AI — initial lakehouse namespaces (run via Trino when tables are created)
-- Bronze / Silver / Gold schemas will be created by Spark or Trino jobs in later phases.

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;
