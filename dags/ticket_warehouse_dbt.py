"""Optional additive Airflow DAG for Project 07's Dockerized local warehouse.

This DAG does not replace the original eight-task SQL/AI DAG. It requires Docker
Compose v2 on the Airflow host and is disabled until explicitly triggered.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow.sdk import dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
WAREHOUSE = PROJECT_ROOT / "warehouse"


def docker_compose(*args: str) -> str:
    command = ["docker", "compose", *args]
    completed = subprocess.run(command, cwd=WAREHOUSE, capture_output=True,
                               text=True, check=False, timeout=1800)
    if completed.returncode:
        raise RuntimeError(f"Docker compose stage failed (exit {completed.returncode}):\n{completed.stderr[-1800:]}")
    return completed.stdout[-1800:]


@dag(
    dag_id="project07_governed_warehouse",
    description="Synthetic-only Postgres/dbt analytical warehouse; no external AI calls",
    start_date=datetime(2026, 1, 1),
    schedule=None, catchup=False, max_active_runs=1,
    tags=["portfolio", "postgres", "dbt", "analytics-engineering"],
)
def project07_governed_warehouse():
    @task(retries=1, retry_delay=timedelta(seconds=30))
    def build_dbt_runtime():
        return docker_compose("build", "dbt")

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def postgres_ready(_built_image):
        return docker_compose("up", "-d", "--wait", "postgres")

    @task(retries=0)
    def validate_synthetic_source(_ready):
        from warehouse.load import validate_fixture
        _, _, details = validate_fixture()
        return details["source_sha256"]

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def load_source(_digest):
        return docker_compose("run", "--rm", "-T", "dbt", "python", "/workspace/warehouse/load.py")

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def dbt_build(_loaded):
        return docker_compose("run", "--rm", "-T", "dbt", "dbt", "build",
                              "--project-dir", "/workspace/warehouse/dbt",
                              "--profiles-dir", "/workspace/warehouse/dbt")

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def dbt_document(_built):
        return docker_compose("run", "--rm", "-T", "dbt", "dbt", "docs", "generate",
                              "--project-dir", "/workspace/warehouse/dbt",
                              "--profiles-dir", "/workspace/warehouse/dbt")

    @task(retries=0)
    def reconcile(_docs):
        return docker_compose("run", "--rm", "-T", "dbt", "python", "/workspace/warehouse/quality.py")

    image = build_dbt_runtime()
    ready = postgres_ready(image)
    source = validate_synthetic_source(ready)
    loaded = load_source(source)
    built = dbt_build(loaded)
    docs = dbt_document(built)
    reconcile(docs)


project07_governed_warehouse()
