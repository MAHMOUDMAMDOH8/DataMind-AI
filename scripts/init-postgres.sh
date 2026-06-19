#!/bin/bash
# Creates nessie and airflow databases in shared-postgres
set -e

function create_db() {
  local db=$1 user=$2 pass=$3
  echo "-> Creating database '$db' and user '$user'"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $user WITH PASSWORD '$pass';
    CREATE DATABASE $db OWNER $user;
    GRANT ALL PRIVILEGES ON DATABASE $db TO $user;
EOSQL
}

create_db nessie  nessie  nessie
create_db airflow airflow airflow
