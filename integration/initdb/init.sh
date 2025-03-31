#!/bin/bash
set -euo pipefail

export POSTGRES_USER=postgres
dbs=($(for i in {1..7}; do echo "d${i}"; done))
tab=($(for i in {1..14}; do echo "t${i}"; done))

for db in "${dbs[@]}"; do
  echo "CREATING DB: ${db}"
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" <<-EOSQL
    CREATE DATABASE ${db} encoding 'UTF8' template template0;
EOSQL
done

for db in "${dbs[@]}"; do
  for t in "${tab[@]}"; do
    echo "CREATING TABLE: ${db} -- ${t}"
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${db}" <<-EOSQL
    create table "${t}" (
        id serial primary key
    );
    insert into "${t}" select from generate_series(1, 10000);
EOSQL
  done
done
