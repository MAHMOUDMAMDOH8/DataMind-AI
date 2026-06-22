#!/bin/sh
set -e

until mc alias set minio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}; do
  echo "Waiting for MinIO..."
  sleep 2
done

for bucket in warehouse telecom-bronze telecom-silver telecom-gold landing; do
  mc mb --ignore-existing "minio/${bucket}"
  mc anonymous set download "minio/${bucket}" || true
done

echo "MinIO buckets ready: warehouse, telecom-bronze, telecom-silver, telecom-gold, landing"
tail -f /dev/null
