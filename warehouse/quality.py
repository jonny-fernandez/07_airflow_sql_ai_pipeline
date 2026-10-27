"""Read-only, fail-closed reconciliation of governed warehouse marts."""
from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path

from load import connect, validate_fixture


def _whole_count(value: int | Decimal) -> int:
    """Convert a PostgreSQL COUNT/SUM aggregate into an exact JSON-safe integer.

    psycopg2 can return Decimal for SUM(numeric). Never silently truncate
    fractional/negative results, and do not stringify metrics in JSON.
    """
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise ValueError(f"Warehouse count must be numeric, got {type(value).__name__}")
    if isinstance(value, Decimal) and not value.is_finite():
        raise ValueError("Warehouse count is non-finite")
    as_int = int(value)
    if as_int < 0 or as_int != value:
        raise ValueError(f"Warehouse count must be a non-negative whole number: {value}")
    return as_int


def audit() -> dict:
    _raw, _outcomes, expected = validate_fixture()
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM raw.incoming_tickets")
            source_count = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM raw.ticket_outcomes")
            outcome_count = cursor.fetchone()[0]
            cursor.execute("""SELECT total_tickets, deterministic_tickets, ai_candidate_tickets,
                              ai_accepted_tickets, human_review_tickets,
                              deterministic_rate_pct, ai_candidate_rate_pct
                              FROM mart.overall_ticket_metrics""")
            rows = cursor.fetchall()
            cursor.execute("SELECT count(*) FROM mart.fct_ticket_outcomes")
            fact_rows = cursor.fetchone()[0]
            cursor.execute("SELECT COALESCE(sum(total_tickets),0) FROM mart.daily_ticket_metrics")
            daily_rows = _whole_count(cursor.fetchone()[0])
    checks = {
        "raw_ticket_count": source_count == expected["source_rows"],
        "raw_outcome_count": outcome_count == expected["outcome_rows"],
        "single_overall_metric": len(rows) == 1,
        "fact_matches_source": fact_rows == expected["source_rows"],
        "daily_matches_fact": daily_rows == fact_rows,
    }
    if len(rows) == 1:
        overall = rows[0]
        checks.update({
            "total": overall[0] == expected["source_rows"],
            "deterministic": overall[1] == expected["deterministic_rows"],
            "ai_candidates": overall[2] == expected["ai_accepted_rows"] + expected["human_review_rows"],
            "ai_accepted": overall[3] == expected["ai_accepted_rows"],
            "human_review": overall[4] == expected["human_review_rows"],
            "deterministic_rate": float(overall[5]) == round(100 * overall[1] / overall[0], 2),
            "ai_candidate_rate": float(overall[6]) == round(100 * overall[2] / overall[0], 2),
        })
    result = {
        "status": "PASS" if all(checks.values()) else "BLOCK",
        "checks": checks,
        "expected_fixture": expected,
        "warehouse": {"raw_rows": source_count, "outcome_rows": outcome_count,
                      "fact_rows": fact_rows, "daily_rows": daily_rows,
                      "overall": [str(x) for x in rows[0]] if len(rows) == 1 else None},
    }
    output = Path(os.environ.get("P07_OUTPUT", "warehouse/artifacts"))
    output.mkdir(parents=True, exist_ok=True)
    (output / "quality_report.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] != "PASS":
        raise SystemExit("Data quality gate BLOCKED")
    return result


if __name__ == "__main__":
    audit()
