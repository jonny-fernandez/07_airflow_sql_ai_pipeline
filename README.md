# Airflow SQL + AI Data Pipeline

A production-style data pipeline that demonstrates how **Apache Airflow, SQL, Python, SQLite, deterministic validation, and selective AI enrichment** can work together without making generative AI responsible for core data-engineering logic.

The project uses synthetic employee-support ticket data and follows a deterministic-first engineering principle:

> **Use Airflow to orchestrate the workflow, SQL/Python to perform deterministic processing and validation, and AI only where semantic interpretation adds value.**

---

## Portfolio Highlights

The latest validated pipeline run processed **50 synthetic tickets**:

| Metric | Result |
|---|---:|
| Pipeline status | SUCCESS |
| Source records | 50 |
| Clean records | 50 |
| Deterministically resolved | 42 |
| Deterministic rate | 84% |
| AI candidates | 8 |
| AI escalation rate | 16% |
| AI results accepted | 6 |
| Human-review cases | 2 |
| AI failures | 0 |
| Final mismatches | 0 |
| Original pipeline baseline tests | 20 / 20 passed |

The pipeline intentionally avoids sending every record to AI. Only **8 of 50 records** required semantic enrichment.

![Pipeline Overview](docs/screenshots/01_pipeline_overview.png)

---

# Architecture

![Pipeline Architecture](docs/screenshots/02_pipeline_architecture.png)

```text
Synthetic Source Data
        |
        v
Schema / Data Quality Validation
        |
        v
SQLite Staging Tables
        |
        v
SQL Transformations
        |
        v
Deterministic Classification
        |
        +-------------------------+
        |                         |
        v                         v
Resolved Records            AI Candidates
                                  |
                                  v
                         Structured AI Review
                                  |
                      +-----------+-----------+
                      |                       |
                      v                       v
                 AI Accepted             Human Review
                                             /
                                          Fallback
                      |                       |
                      +-----------+-----------+
                                  |
                                  v
                         Final Output Tables
                                  |
                                  v
                        Analytical Summaries
                                  |
                                  v
                      Ground-Truth Evaluation
                                  |
                                  v
                         Airflow Completion

---

## CP2–CP3 upgrade: local governed analytics warehouse

The original eight-task Airflow/SQLite/AI pipeline remains available. A **separate,
optional, local-only** warehouse path adds PostgreSQL 16 and dbt Core models for
synthetic tickets. The workflow is not a real customer product or a cloud deployment.

- **Models:** raw PostgreSQL ingestion, staging, intermediate resolution, and nine
  dbt transformations/marts. The original Airflow DAG is unchanged; a separate
  `project07_governed_warehouse` DAG documents manual warehouse orchestration.
- **Metrics:** 50 synthetic tickets; 42 deterministic outcomes (84%), six
  AI-accepted outcomes, and two human-review outcomes; source, outcomes and fact
  table IDs reconcile with zero orphans in the verified local run.
- **Quality:** schema/contract validation rejects 12 malformed synthetic inputs;
  12 warehouse checks passed. This rejects invalid input but does not implement a
  durable quarantine table or a real dead-letter queue.
- **Reproducibility:** the second Docker/dbt run wrote zero changed source/outcome
  rows and saved identical quality reports and manifests.
- **Tests:** CP1 baseline 20/20; CP2 47/47; CP3 67/67 on local Ubuntu WSL2,
  Python 3.14.4, Docker Desktop and PostgreSQL/dbt (October 2026).
- **Security:** synthetic data only; localhost-bound database port, no provider
  API calls needed to verify warehouse results, no credential files published.

See [CP2 architecture](docs/CP2_WAREHOUSE_UPGRADE.md),
[CP3 quality and recovery](docs/CP3_QUALITY_RECOVERY.md), and
[CP4 release/evidence notes](docs/CP4_RELEASE_REVIEW.md).

### Run locally

With Docker Desktop's Ubuntu WSL integration enabled:

```bash
# From the project repository root in Ubuntu WSL
bash warehouse/run_pipeline.sh
python -m pytest -q
python scripts/project07_cp3_evidence.py
```

The last command additionally requires the packages in `requirements.txt`, a
working Airflow metadata database, and the connected local Docker engine.
Generated reports remain under ignored `warehouse/artifacts/`. Store real
credentials only in ignored `.env` files; never upload them to GitHub.

### Evidence and limitations

Eight panels under `screenshots/cp4_upgrade/` are **rendered evidence summaries**
from verified CP3 logs, not live console screenshots. See their README and
SHA-256 manifest. Full Docker/dbt and database-replay outcomes were checked in
WSL; cloud infrastructure, live scheduling by an Airflow service, professional
SQL employment usage, provider-integrated AI execution, and production-scale
throughput are **not claimed**. GitHub CI checks must be verified on the actual
published commit and may cover a different subset than the WSL Docker evaluation.
