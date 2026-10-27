# Governed Local Warehouse (Project 07 CP2)

Additive PostgreSQL 16 + dbt Core transformation path using existing synthetic ticket fixtures.

Launch from WSL Ubuntu with Docker Desktop integrated:

```bash
cd /mnt/c/Users/your-user/Documents/github\ files/07_airflow_sql_ai_pipeline_CP2_Upgrade_Working
bash warehouse/run_pipeline.sh
```

The scripts do **not** send records to OpenAI. The original `sql_ai_ticket_pipeline` DAG and data sources are preserved. Quality reports and dbt artifacts appear under `warehouse/artifacts/` and must not be staged or published by default. Only port `127.0.0.1:54329` is exposed for local inspection. Never use real employee/customer data.

For detailed runbook, model contracts, lineage, metrics and caveats see `docs/CP2_WAREHOUSE_UPGRADE.md`.
