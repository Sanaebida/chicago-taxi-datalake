#!/usr/bin/env bash
# Runs the whole pipeline end to end (BRONZE -> SILVER -> GOLD) for the
# configured period. Executed inside the Dagster image: runs are recorded in the
# Dagster instance (Postgres) and are visible in the UI (http://localhost:3000).
set -euo pipefail

MODULE=taxi_pipeline.definitions
read -r FIRST LAST < <(python - <<'PY'
from taxi_pipeline.partitions import monthly_partitions as p
print(p.get_first_partition_key(), p.get_last_partition_key())
PY
)

echo "==> [1/2] BRONZE + SILVER for months ${FIRST} ... ${LAST} (single Spark run)"
dagster asset materialize -m "$MODULE" \
  --select 'bronze_taxi_trips,silver_taxi_trips' \
  --partition-range "${FIRST}...${LAST}"

echo "==> [2/2] Reference dimension + GOLD tables"
dagster asset materialize -m "$MODULE" \
  --select 'bronze_community_areas,dim_community_area,group:gold'

echo "==> Done. Dagster UI: http://localhost:3000 | Dashboard: http://localhost:8501 | MinIO: http://localhost:9001"
