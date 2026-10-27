"""New offline/stdlib test cases; original 20 tests are unchanged."""
from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "warehouse/load.py"
spec = importlib.util.spec_from_file_location("project07_warehouse_loader", MODULE)
assert spec and spec.loader
loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loader)


def fixtures(tmp_path):
    one = tmp_path / "tickets.csv"
    two = tmp_path / "final.csv"
    one.write_bytes(loader.SOURCE.read_bytes())
    two.write_bytes(loader.OUTCOMES.read_bytes())
    return one, two


def rewrite_csv(path, mutator):
    with path.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
        keys = list(rows[0])
    mutator(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def test_original_fixture_counts():
    _, _, m = loader.validate_fixture()
    assert (m["source_rows"], m["deterministic_rows"], m["ai_accepted_rows"], m["human_review_rows"]) == (50, 42, 6, 2)


def test_fixed_source_checksum():
    assert loader.canonical_hash({"a": 1, "b": 2}) == loader.canonical_hash({"b": 2, "a": 1})


def test_duplicate_source_id_is_rejected(tmp_path):
    source, output = fixtures(tmp_path)
    rewrite_csv(source, lambda rows: rows.append(dict(rows[0])))
    with pytest.raises(ValueError, match="duplicated"):
        loader.validate_fixture(source, output)


def test_unmatched_outcome_is_rejected(tmp_path):
    source, output = fixtures(tmp_path)
    rewrite_csv(output, lambda rows: rows.pop())
    with pytest.raises(ValueError, match="Missing outcomes"):
        loader.validate_fixture(source, output)


def test_source_outcome_tampering_is_rejected(tmp_path):
    source, output = fixtures(tmp_path)
    rewrite_csv(source, lambda rows: rows[0].update(ticket_text="ALTERED"))
    with pytest.raises(ValueError, match="conflict"):
        loader.validate_fixture(source, output)


def test_invalid_date_is_rejected(tmp_path):
    source, output = fixtures(tmp_path)
    rewrite_csv(source, lambda rows: rows[0].update(created_date="not-a-date"))
    with pytest.raises(ValueError):
        loader.validate_fixture(source, output)


def test_invalid_handling_is_rejected(tmp_path):
    source, output = fixtures(tmp_path)
    rewrite_csv(output, lambda rows: rows[0].update(final_handling="AUTO_DELETED"))
    with pytest.raises(ValueError, match="Unsupported"):
        loader.validate_fixture(source, output)


def test_wrong_ai_used_is_rejected(tmp_path):
    source, output = fixtures(tmp_path)
    rewrite_csv(output, lambda rows: rows[0].update(ai_used="1"))
    with pytest.raises(ValueError, match="Inconsistent"):
        loader.validate_fixture(source, output)


def test_empty_ticket_is_rejected(tmp_path):
    source, output = fixtures(tmp_path)
    rewrite_csv(source, lambda rows: rows[0].update(ticket_text="  "))
    with pytest.raises(ValueError, match="Empty ticket"):
        loader.validate_fixture(source, output)


def test_dbt_marts_reference_staging_graph():
    models = ROOT / "warehouse/dbt/models"
    assert "ref('int_ticket_resolution')" in (models / "marts/fct_ticket_outcomes.sql").read_text()
    assert "ref('stg_tickets')" in (models / "intermediate/int_ticket_resolution.sql").read_text()
    assert "ref('stg_ticket_outcomes')" in (models / "intermediate/int_ticket_resolution.sql").read_text()


def test_incremental_fact_has_key_and_watermark():
    sql = (ROOT / "warehouse/dbt/models/marts/fct_ticket_outcomes.sql").read_text()
    assert "unique_key='ticket_id'" in sql
    assert "source_loaded_at >=" in sql


def test_compose_is_local_only():
    compose = (ROOT / "warehouse/docker-compose.yml").read_text()
    assert '127.0.0.1:54329:5432' in compose
    assert "portfolio_dev_only" in compose
    assert "./init:/docker-entrypoint-initdb.d:ro" in compose


def test_no_remote_model_service_in_warehouse():
    warehouse = ROOT / "warehouse"
    code = "\n".join(p.read_text() for p in warehouse.glob("*.py"))
    assert "OpenAI(" not in code and "requests.post(" not in code


def test_original_airflow_dag_is_unchanged():
    dag = ROOT / "dags/sql_ai_pipeline.py"
    body = dag.read_text(encoding="utf-8-sig")
    assert 'dag_id="sql_ai_ticket_pipeline"' in body
    assert 'task_id="enrich_ai_candidates"' in body


def test_warehouse_airflow_is_separate_and_manual():
    dag = (ROOT / "dags/ticket_warehouse_dbt.py").read_text()
    assert 'dag_id="project07_governed_warehouse"' in dag
    assert "schedule=None" in dag
    assert "subprocess.run(command" in dag


def test_loader_consistent_repeated_validation():
    assert loader.validate_fixture()[2] == loader.validate_fixture()[2]


def test_sql_plan_fixture_exists():
    statement = (ROOT / "warehouse/explain.sql").read_text()
    assert "EXPLAIN (ANALYZE, BUFFERS" in statement


def test_human_review_kept_in_metric_denominator():
    metric_sql = (ROOT / "warehouse/dbt/models/marts/overall_ticket_metrics.sql").read_text()
    assert "count(*) as total_tickets" in metric_sql
    assert "human_review_tickets" in metric_sql


def test_advanced_sql_windows_are_explicitly_observed_dates():
    sql = (ROOT / "warehouse/dbt/models/marts/ticket_daily_trends.sql").read_text()
    assert "with daily as" in sql
    assert "lag(tickets) over" in sql
    assert "rows between 6 preceding and current row" in sql
    assert "observed" in sql
