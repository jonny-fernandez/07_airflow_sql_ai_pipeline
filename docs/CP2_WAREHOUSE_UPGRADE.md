# Project 07 — CP2 Governed SQL Warehouse Upgrade

**Scope:** Additive synthetic-only local portfolio demonstration. Base commit `f2866b49408f61362d7398ce9e6e96df986802e0`. The original eight-task SQLite/AI DAG and all 20 legacy baseline tests are retained without changes.

## Boundary / lineage

```
Synthetic ticket CSV + synthetic final-result CSV
    |  warehouse/load.py (schema/relationship checks, atomic upserts, row SHA-256)
    v
PostgreSQL raw.incoming_tickets + raw.ticket_outcomes
    |  dbt source contracts
    v
staging.stg_tickets + staging.stg_ticket_outcomes
    |
    v
intermediate.int_ticket_resolution
    |
    +--> mart.fct_ticket_outcomes (incremental, unique ticket_id)
    +--> mart.dim_category
    +--> mart.daily_ticket_metrics
    +--> mart.category_metrics
    +--> mart.overall_ticket_metrics
    +--> mart.ticket_daily_trends (CTEs, LAG, 7 observed-date rolling SUM)
              |
              v
warehouse/quality.py (50=42+6+2, all denominators, source parity)
              |
              v
warehouse/artifacts/quality_report.json + dbt/target/docs
```

The source data is an **existing, static synthetic demonstration**, not live production data. No new AI calls or accounts are required. PostgreSQL is local Docker on `127.0.0.1:54329`; do not expose its port publicly. The fallback password is only for isolated local portfolio use and is not a real secret.

## Run on WSL Ubuntu with Docker Desktop WSL integration

1. Extract/patch to a separate `07_airflow_sql_ai_pipeline_CP2_Upgrade_Working` folder. The original repository remains untouched.
2. In Ubuntu/WSL, `cd '/mnt/c/Users/Jonny Fernandez/Documents/github files/07_airflow_sql_ai_pipeline_CP2_Upgrade_Working'` (adjust actual path).
3. Run `~/.venvs/project07-cp1-py3.14/bin/python scripts/project07_cp2_evidence.py` (adjust venv path if different). This collects temporary Airflow baseline + CP2 tests and runs the PostgreSQL/dbt warehouse twice, including idempotent replay. Alternatively run only `bash warehouse/run_pipeline.sh` for manual execution.
4. Review `warehouse/artifacts/{load_manifest,quality_report}.json`; inspect `warehouse/artifacts/dbt_target/manifest.json`, `run_results.json`, and `catalog.json` for dbt evidence.
5. Run `docker compose -f warehouse/docker-compose.yml -p project07warehouse down` only if you explicitly used project flag; otherwise in `warehouse/` use `docker compose down` to stop containers; leave volume for incremental replay.
6. Rerun `bash warehouse/run_pipeline.sh`. Expect `source_changed_rows=0` and `outcome_changed_rows=0`, with stable counts. When testing corrected/later rows, use synthetic-only copies and document expected updates.
7. Generate query-plan evidence with `cd warehouse && docker compose exec -T postgres psql -U portfolio -d project07 < explain.sql > artifacts/explain_analyze.txt`.

No Docker execution or query-plan timings are claimed until your machine produces logs. Docker Desktop and WSL integration must be installed and enabled. On the original WSL Python venv, run `python -m pytest -o addopts= -q` from the working repo to verify the original 20 and the new contract tests; initialize a temporary Airflow DB before DAG tests if required by your setup.

## Data model definitions

- `total_tickets`: all unique records including human-review cases; not only successful classifications.
- `deterministic_tickets`: `final_handling='DETERMINISTIC'`.
- `ai_candidate_tickets`: original `ai_used=true`, including AI-accepted and human-review cases (not number of API calls).
- `ai_accepted_tickets`: `final_handling='AI_ACCEPTED'`.
- `human_review_tickets`: `final_handling='HUMAN_REVIEW'`.
- `deterministic_rate_pct`: deterministic tickets / total tickets × 100.
- `ai_candidate_rate_pct`: AI candidate tickets / total tickets × 100.
- Base synthetic fixture: total=50, deterministic=42 (84%), AI candidates=8 (16%), AI accepted=6, human review=2.

The incremental fact upserts changed data by ticket ID and `source_loaded_at`. It does not implement deletion CDC. Deletions require a documented full-refresh/migration, not silent acceptance. No claims of measured large-scale performance on 50 records.

## Orchestration / failure controls

- Existing `sql_ai_ticket_pipeline` remains unchanged, with the original eight tasks, routes and AI review path.
- New optional manual DAG `project07_governed_warehouse` builds container, waits for PostgreSQL, checks source, loads, builds dbt models, generates docs and reconciles. It is **not scheduled** to avoid background Docker/LLM expense. Retries are bounded, shell execution uses argument arrays (no `shell=True`), errors block the DAG.
- The CLI runner fails immediately on dbt/source/quality errors. `warehouse/load.py` validates the entire fixture before a database write, then uses a single transaction.
- Security: synthetic-only, no real personnel tickets, no `.env`, no cloud keys, no paid API calls. Base repo may contain a private `.env`; never upload or commit it.

## Limits and remaining verification

- Local warehouse code and unit tests can be checked offline, but full PostgreSQL/dbt builds, SQL query plans, Airflow DAG runs, Docker execution, and CI matrix status must be proven on your environment in CP3.
- dbt Core is tested conceptually by a container build; only actual `dbt build`/`dbt test` output verifies model execution.
- Data volume is 50 synthetic records. Benchmarking beyond this needs generated fixtures, hardware/timing evidence, and reproducible methodology.
- No professional/employer SQL ownership or live production deployment is asserted.
