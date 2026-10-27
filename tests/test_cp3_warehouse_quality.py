"""CP3 adversarial, replay and identity-parity tests, no Docker credentials required."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from warehouse import cp3_checks


def test_mutation_suite_blocks_every_invalid_synthetic_case():
    report = cp3_checks.run_negative_contracts()
    assert report["status"] == "PASS", report
    assert report["scenario_count"] == report["rejected_count"] == 12
    assert report["baseline_source_rows"] == 50


def test_negative_suite_does_not_change_original_fixtures():
    original = (cp3_checks.sha256(cp3_checks.load.SOURCE), cp3_checks.sha256(cp3_checks.load.OUTCOMES))
    cp3_checks.run_negative_contracts()
    assert original == (cp3_checks.sha256(cp3_checks.load.SOURCE), cp3_checks.sha256(cp3_checks.load.OUTCOMES))


def _counts():
    return dict(raw_source_count=50, raw_outcome_count=50, fact_count=50,
                source_not_outcome=0, outcome_not_source=0, source_not_fact=0, fact_not_source=0)


def test_readonly_identity_passes_valid_counts():
    assert cp3_checks.evaluate_identity(_counts(), 50)["status"] == "PASS"


@pytest.mark.parametrize("difference", ["source_not_outcome", "outcome_not_source", "source_not_fact", "fact_not_source"])
def test_same_counts_wrong_ticket_ids_are_blocked(difference):
    counts = _counts()
    counts[difference] = 1
    result = cp3_checks.evaluate_identity(counts, 50)
    assert result["status"] == "BLOCK"
    assert result["checks"][difference] is False


@pytest.mark.parametrize("difference", ["raw_source_count", "raw_outcome_count", "fact_count"])
def test_missing_or_extra_rows_are_blocked(difference):
    counts = _counts()
    counts[difference] = 49
    assert cp3_checks.evaluate_identity(counts, 50)["status"] == "BLOCK"


def test_sql_probes_execute_selects_only():
    class Cursor:
        sql = []
        def execute(self, statement):
            self.sql.append(statement)
        def fetchone(self):
            return (0,)
    fake = Cursor()
    cp3_checks._read_only_counts(fake)
    assert len(fake.sql) == 7
    assert all(item.strip().upper().startswith("SELECT") for item in fake.sql)
    assert all("EXCEPT" in item.upper() for item in fake.sql[3:])


def _make_replay(tmp_path):
    fixture = {"status":"PASS","checks":{"example":True},
               "expected_fixture":{"source_sha256":"src","outcomes_sha256":"out"}}
    replay = {"source_changed_rows":0,"outcome_changed_rows":0,
              "source_sha256":"src","outcomes_sha256":"out"}
    files = [tmp_path / i for i in ('first.json', 'second.json', 'load.json')]
    for path, data in zip(files, (fixture, fixture, replay)):
        path.write_text(json.dumps(data, sort_keys=True) + '\n', encoding='utf-8')
    return files


def test_replay_evidence_passes_when_identical(tmp_path):
    assert cp3_checks.compare_replay(*_make_replay(tmp_path))["status"] == "PASS"


def test_evidence_tamper_blocks_mismatched_report(tmp_path):
    first, second, manifest = _make_replay(tmp_path)
    item = json.loads(second.read_text())
    item["checks"]["example"] = False
    second.write_text(json.dumps(item, sort_keys=True) + '\n')
    assert cp3_checks.compare_replay(first, second, manifest)["status"] == "BLOCK"


def test_replay_non_idempotent_write_blocks(tmp_path):
    first, second, manifest = _make_replay(tmp_path)
    data = json.loads(manifest.read_text())
    data["outcome_changed_rows"] = 1
    manifest.write_text(json.dumps(data, sort_keys=True) + '\n')
    assert cp3_checks.compare_replay(first, second, manifest)["status"] == "BLOCK"


def test_replay_digest_drift_blocks(tmp_path):
    first, second, manifest = _make_replay(tmp_path)
    data = json.loads(manifest.read_text())
    data["source_sha256"] = "tampered"
    manifest.write_text(json.dumps(data, sort_keys=True) + '\n')
    assert cp3_checks.compare_replay(first, second, manifest)["status"] == "BLOCK"


def test_source_code_is_additive_and_not_modifying_cp2_dag():
    root = Path(__file__).resolve().parents[1]
    dag = (root / 'dags/ticket_warehouse_dbt.py').read_text(encoding='utf-8')
    assert 'schedule=None' in dag and 'project07_governed_warehouse' in dag
    original = (root / 'dags/sql_ai_pipeline.py').read_text(encoding='utf-8-sig')
    assert 'dag_id="sql_ai_ticket_pipeline"' in original


def test_security_scanner_rejects_secret_filename(tmp_path):
    from warehouse import cp3_security
    path = tmp_path / '.env'
    path.write_text('REDACTED_TEST=DO_NOT_LOG')
    result = cp3_security.audit_paths(['.env'], tmp_path)
    assert result['status'] == 'BLOCK'
    assert 'REDACTED_TEST' not in json.dumps(result)


def test_security_scanner_rejects_known_style_token_without_value_in_report(tmp_path):
    from warehouse import cp3_security
    token = 'sk-' + 'x'*36
    (tmp_path/'bad.py').write_text('key = "' + token + '"')
    result = cp3_security.audit_paths(['bad.py'],tmp_path)
    assert result['status'] == 'BLOCK'
    assert token not in json.dumps(result)


def test_security_scanner_accepts_regular_source(tmp_path):
    from warehouse import cp3_security
    (tmp_path/'okay.py').write_text('print("synthetic only")\n')
    assert cp3_security.audit_paths(['okay.py'],tmp_path)['status'] == 'PASS'
