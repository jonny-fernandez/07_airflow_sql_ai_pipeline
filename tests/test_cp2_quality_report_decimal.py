"""Regression tests: psycopg2 numeric SUM returns Decimal and must serialize cleanly."""
from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

WAREHOUSE = Path(__file__).resolve().parents[1] / "warehouse"
if str(WAREHOUSE) not in sys.path:
    sys.path.insert(0, str(WAREHOUSE))

import quality  # noqa: E402


def test_integer_conversions_are_exact():
    assert quality._whole_count(Decimal("50.00")) == 50
    assert quality._whole_count(50) == 50
    assert isinstance(quality._whole_count(Decimal("50")), int)


@pytest.mark.parametrize("bad", [Decimal("50.01"), Decimal("NaN"), -1, True, None, "50"])
def test_invalid_aggregate_does_not_silently_round_or_serialize(bad):
    with pytest.raises(ValueError):
        quality._whole_count(bad)


def test_audit_writes_json_with_decimal_daily_total(monkeypatch, tmp_path):
    expected = {
        "source_rows": 50,
        "outcome_rows": 50,
        "deterministic_rows": 42,
        "ai_accepted_rows": 6,
        "human_review_rows": 2,
        "source_sha256": "synthetic_source_hash",
        "outcomes_sha256": "synthetic_outcomes_hash",
    }
    monkeypatch.setattr(quality, "validate_fixture", lambda: ([], [], expected))
    monkeypatch.setenv("P07_OUTPUT", str(tmp_path))

    class Cursor:
        index = 0

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, sql):
            self.index += 1

        def fetchone(self):
            values = {1: (50,), 2: (50,), 4: (50,), 5: (Decimal("50.00"),)}
            return values[self.index]

        def fetchall(self):
            assert self.index == 3
            return [(50, 42, 8, 6, 2, Decimal("84.00"), Decimal("16.00"))]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(quality, "connect", Connection)
    result = quality.audit()
    saved = json.loads((tmp_path / "quality_report.json").read_text(encoding="utf-8"))
    assert result["status"] == saved["status"] == "PASS"
    assert saved["warehouse"]["daily_rows"] == 50
    assert type(saved["warehouse"]["daily_rows"]) is int
    assert all(saved["checks"].values())
