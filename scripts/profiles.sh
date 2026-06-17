#!/usr/bin/env bash
# DataMind AI — compose profile definitions
set -euo pipefail

ALL_PROFILES=(
  ingestion
  storage
  processing
  query
  orchestration
  governance
  quality
  ml
  ai
)

profile_services() {
  case "$1" in
    ingestion)
      echo "zookeeper kafka schema-registry kafka-ui nifi"
      ;;
    storage)
      echo "minio mc nessie-postgres nessie iceberg-rest"
      ;;
    processing)
      echo "spark-iceberg"
      ;;
    query)
      echo "trino"
      ;;
    orchestration)
      echo "airflow-postgres airflow-webserver airflow-scheduler"
      ;;
    governance)
      echo "openmetadata-mysql openmetadata-elasticsearch openmetadata-server"
      ;;
    quality)
      echo "gx-gateway"
      ;;
    ml)
      echo "mlflow"
      ;;
    ai)
      echo "qdrant ollama"
      ;;
    *)
      return 1
      ;;
  esac
}

profile_endpoints() {
  case "$1" in
    ingestion)
      cat <<'EOF'
Kafka broker API|bash -c 'docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1'
Schema Registry|http://localhost:8081/subjects|5
Kafka UI|http://localhost:8090|5
NiFi UI|http://localhost:8082/nifi|30
EOF
      ;;
    storage)
      cat <<'EOF'
MinIO health|http://localhost:9000/minio/health/live|5
Nessie API|http://localhost:19120/api/v2/config|5
Iceberg REST catalog|http://localhost:8181/v1/config|5
EOF
      ;;
    processing)
      echo "Spark UI|http://localhost:8080|5"
      ;;
    query)
      echo "Trino coordinator|http://localhost:8085/v1/info|5"
      ;;
    orchestration)
      echo "Airflow webserver|http://localhost:8083/health|15"
      ;;
    governance)
      echo "OpenMetadata API|http://localhost:8585/api/v1/system/version|15"
      ;;
    quality)
      echo "GX gateway|http://localhost:3000/health|5"
      ;;
    ml)
      echo "MLflow server|http://localhost:5000/health|5"
      ;;
    ai)
      cat <<'EOF'
Qdrant health|http://localhost:6333/healthz|5
Ollama API|http://localhost:11434/api/tags|10
EOF
      ;;
  esac
}

compose_profiles_args() {
  local args=()
  for p in "${ALL_PROFILES[@]}"; do
    args+=(--profile "$p")
  done
  printf '%s\n' "${args[@]}"
}
