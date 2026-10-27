#!/usr/bin/env bash
# Run from any cwd. Requires Docker Desktop WSL integration and Docker Compose v2.
set -Eeuo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"
mkdir -p artifacts
if ! command -v docker >/dev/null 2>&1; then echo 'Docker CLI missing' >&2; exit 2; fi
docker compose version
echo '[1/6] Build the pinned dbt service'
docker compose build dbt
echo '[2/6] Start local PostgreSQL and initialize raw schemas'
docker compose up -d --wait postgres
echo '[3/6] Load synthetic CSVs (idempotent upsert)'
docker compose run --rm -T dbt python /workspace/warehouse/load.py
echo '[4/6] Run transformations + dbt data tests'
docker compose run --rm -T dbt dbt build --project-dir /workspace/warehouse/dbt --profiles-dir /workspace/warehouse/dbt
echo '[5/6] Build dbt documentation'
docker compose run --rm -T dbt dbt docs generate --project-dir /workspace/warehouse/dbt --profiles-dir /workspace/warehouse/dbt
echo '[6/6] Enforce strict source-to-mart reconciliation'
docker compose run --rm -T dbt python /workspace/warehouse/quality.py
echo 'PASS: Project07 CP2 synthetic local warehouse. See warehouse/artifacts/'
