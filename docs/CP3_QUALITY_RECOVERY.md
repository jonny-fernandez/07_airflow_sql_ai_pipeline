# Project 07 | Checkpoint 3 — Quality, negative scenarios and recovery evidence

**Scope:** An additive validation and evidence layer around the already verified CP2 PostgreSQL/dbt warehouse. The legacy eight-task Airflow/AI path, its locked fixtures, CP2 loader, dbt models, Docker configuration, and quality gate are unchanged.

## Evidence carried forward

- CP1 baseline: 20/20 tests at commit `f2866b49408f61362d7398ce9e6e96df986802e0`.
- CP2 verified on Ubuntu WSL2 with Docker: 47/47 tests, nine dbt models, 24 dbt tests, 12/12 quality checks, zero-row replay writes, 50 synthetic tickets (42 deterministic, six AI-accepted, two human review).
- CP3 requires a **new** local execution. Previous CP2 results are not claims that the new checks have passed in WSL or CI.

## CP3 quality gates

| Gate | Action | Success |
|---|---|---|
| Legacy + CP2 + CP3 regressions | Airflow metadata migration in temporary `AIRFLOW_HOME`; pytest full suite | All collected tests pass |
| Negative input contracts | 12 named fixture mutations in temporary copies | All rejected before database connection; zero fixture modifications |
| Warehouse build | Existing `warehouse/run_pipeline.sh` | PostgreSQL health, dbt build/test/docs, CP2 quality all pass |
| Rerun/recovery | Repeat same pipeline against existing local warehouse | Replay has zero source/outcome writes, quality reports byte-identical |
| Set-level identity reconciliation | Read-only PostgreSQL transaction | No unmatched ticket IDs in either direction among raw sources, outcomes and fact |
| Evidence integrity | SHA-256 digest manifest of captured CP3 logs/reports | Every file verifies independently |
| Source security | Scan Git tracked files plus five new CP3 sources for sensitive filenames and key patterns | Zero findings; review untracked files separately before GitHub publication |

The negative cases **fail closed**. They do *not* claim the pipeline currently has a durable quarantine table or supports a live DLQ. The read-only probe does not attempt destructive failure injection. A real Airflow scheduler DAG run is not claimed solely from DAG import and configuration tests; record separate evidence if executed.

## Reproduce from the CP2 working copy

Inside Ubuntu WSL2, with Docker Desktop integration active and the existing Python environment:

```bash
cd '/mnt/c/Users/Jonny Fernandez/Documents/github files/07_airflow_sql_ai_pipeline_CP2_Upgrade_Working'
~/.venvs/project07-cp1-py3.14/bin/python -m pytest -o addopts= -q tests/test_cp3_warehouse_quality.py
~/.venvs/project07-cp1-py3.14/bin/python scripts/project07_cp3_evidence.py
```

The second command generates `warehouse/artifacts/Project07_CP3_Evidence_<timestamp>_UTC.zip` even when a step fails. Upload the ZIP and final terminal screenshot for review. A CP3 `PASS` requires all steps, including Docker and PostgreSQL identity checks, to return success.

## Data protection

- Inputs use **synthetic tickets only**. Never use employer, employee, I-9 or payroll data.
- No external OpenAI calls are needed for CP3 tests. The original AI branch is unchanged.
- The check scripts accept no arbitrary SQL or shell commands from a model/user API.
- `warehouse/artifacts/`, `.env`, database volumes, temporary staging and private credentials are excluded from release commits. `.env.example` may be public if scrubbed.
- Review local log paths for account names before publishing any evidence images.

## Known limitations and extensions

- WSL Docker Compose integration and full Airflow provider tests must be run on the user's workstation. No Docker execution was performed by the authoring environment.
- This tests corruption *of input fixtures and evidence* safely, not physical database rollback under a killed process. Future controlled restart/rollback testing must use a disposable test database, not the user's working volume.
- The synthetic 50-ticket dataset and zero-cost demo cannot establish production-scale throughput, customer impact, professional SQL tenure, or a deployed SaaS integration.
- CP4 screenshot handoff and GitHub publication require separate user review/approval.
