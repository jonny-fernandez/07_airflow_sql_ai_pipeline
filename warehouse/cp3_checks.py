"""Project 07 CP3: synthetic-only negative contracts and read-only database identity checks.

No external API calls, production input, schema mutations or arbitrary SQL accepted.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import tempfile
from pathlib import Path

try:
    from warehouse import load
except ImportError:
    import load  # dbt container runs this file directly


def _copy_rows(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        return list(reader.fieldnames or ()), list(reader)


def _write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


# Each mutation is isolated to temporary synthetic CSV copies. These scenarios
# must be blocked BEFORE connect()/INSERT can be called.
NEGATIVE_CASES = (
    "duplicate_source_id", "duplicate_outcome_id", "missing_outcome",
    "source_text_tampered", "invalid_date", "invalid_handling",
    "invalid_ai_flag", "empty_ticket_text", "unmatched_outcome_id",
    "missing_required_source_header", "empty_outcome_category", "routing_mismatch",
)


def run_negative_contracts(source_path: Path = load.SOURCE, outcomes_path: Path = load.OUTCOMES) -> dict:
    # Require a valid baseline before a negative scenario is meaningful.
    _, _, baseline = load.validate_fixture(source_path, outcomes_path)
    results: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="p07_cp3_negative_") as temp:
        root = Path(temp)
        for case in NEGATIVE_CASES:
            sh, sources = _copy_rows(source_path)
            oh, outcomes = _copy_rows(outcomes_path)
            if case == "duplicate_source_id":
                sources.append(dict(sources[0]))
            elif case == "duplicate_outcome_id":
                outcomes.append(dict(outcomes[0]))
            elif case == "missing_outcome":
                outcomes.pop()
            elif case == "source_text_tampered":
                sources[0]["ticket_text"] = "synthetic altered ticket"
            elif case == "invalid_date":
                sources[0]["created_date"] = "2026-99-99"
            elif case == "invalid_handling":
                outcomes[0]["final_handling"] = "AUTO_DELETE"
            elif case == "invalid_ai_flag":
                outcomes[0]["ai_used"] = "invalid"
            elif case == "empty_ticket_text":
                sources[0]["ticket_text"] = "   "
            elif case == "unmatched_outcome_id":
                outcomes[0]["ticket_id"] = "999999"
            elif case == "missing_required_source_header":
                sh.remove("ticket_text")
                sources = [{k: v for k, v in item.items() if k != "ticket_text"} for item in sources]
            elif case == "empty_outcome_category":
                record = next(item for item in outcomes if item["final_handling"] != "HUMAN_REVIEW")
                record["final_category"] = ""
            elif case == "routing_mismatch":
                record = next(item for item in outcomes if item["final_handling"] == "DETERMINISTIC")
                record["classification_source"] = "AI"
            sp, op = root / f"{case}.source.csv", root / f"{case}.outcome.csv"
            _write_csv(sp, sh, sources)
            _write_csv(op, oh, outcomes)
            try:
                load.validate_fixture(sp, op)
            except (ValueError, KeyError, TypeError) as error:
                results.append({"case": case, "blocked": True, "exception_type": type(error).__name__})
            else:
                results.append({"case": case, "blocked": False, "exception_type": None})
    return {
        "status": "PASS" if all(item["blocked"] for item in results) else "BLOCK",
        "scenario_count": len(results),
        "rejected_count": sum(item["blocked"] for item in results),
        "baseline_source_rows": baseline["source_rows"],
        "baseline_source_sha256": baseline["source_sha256"],
        "baseline_outcomes_sha256": baseline["outcomes_sha256"],
        "scenarios": results,
        "scope": "temporary synthetic CSV mutation; no warehouse writes or persistent quarantine table",
    }


def _read_only_counts(cursor) -> dict[str, int]:
    queries = {
        "raw_source_count": "SELECT count(*) FROM raw.incoming_tickets",
        "raw_outcome_count": "SELECT count(*) FROM raw.ticket_outcomes",
        "fact_count": "SELECT count(*) FROM mart.fct_ticket_outcomes",
        "source_not_outcome": (
            "SELECT count(*) FROM (SELECT ticket_id FROM raw.incoming_tickets "
            "EXCEPT SELECT ticket_id FROM raw.ticket_outcomes) q"
        ),
        "outcome_not_source": (
            "SELECT count(*) FROM (SELECT ticket_id FROM raw.ticket_outcomes "
            "EXCEPT SELECT ticket_id FROM raw.incoming_tickets) q"
        ),
        "source_not_fact": (
            "SELECT count(*) FROM (SELECT ticket_id FROM raw.incoming_tickets "
            "EXCEPT SELECT ticket_id FROM mart.fct_ticket_outcomes) q"
        ),
        "fact_not_source": (
            "SELECT count(*) FROM (SELECT ticket_id FROM mart.fct_ticket_outcomes "
            "EXCEPT SELECT ticket_id FROM raw.incoming_tickets) q"
        ),
    }
    result = {}
    for key, query in queries.items():
        cursor.execute(query)
        result[key] = int(cursor.fetchone()[0])
    return result


def evaluate_identity(counts: dict[str, int], expected: int) -> dict:
    assert expected > 0
    checks = {
        "raw_source_count": counts["raw_source_count"] == expected,
        "raw_outcome_count": counts["raw_outcome_count"] == expected,
        "fact_count": counts["fact_count"] == expected,
        "source_not_outcome": counts["source_not_outcome"] == 0,
        "outcome_not_source": counts["outcome_not_source"] == 0,
        "source_not_fact": counts["source_not_fact"] == 0,
        "fact_not_source": counts["fact_not_source"] == 0,
    }
    return {"status": "PASS" if all(checks.values()) else "BLOCK", "checks": checks, "counts": counts,
            "expected_rows": expected, "mode": "PostgreSQL read-only transaction"}


def audit_postgres() -> dict:
    _source, _outcomes, stats = load.validate_fixture()
    with load.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            result = evaluate_identity(_read_only_counts(cursor), stats["source_rows"])
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compare_replay(first_quality: Path, second_quality: Path, second_load: Path) -> dict:
    for path in (first_quality, second_quality, second_load):
        if not path.is_file():
            raise FileNotFoundError(path.name)
    first = json.loads(first_quality.read_bytes())
    second = json.loads(second_quality.read_bytes())
    replay = json.loads(second_load.read_bytes())
    checks = {
        "quality_both_pass": first.get("status") == second.get("status") == "PASS",
        "all_quality_checks_true": bool(first.get("checks")) and all(first["checks"].values())
        and bool(second.get("checks")) and all(second["checks"].values()),
        "byte_identical_quality": sha256(first_quality) == sha256(second_quality),
        "replay_changed_zero": replay.get("source_changed_rows") == 0 and replay.get("outcome_changed_rows") == 0,
        "source_digest_equal": first.get("expected_fixture", {}).get("source_sha256")
        == replay.get("source_sha256") == second.get("expected_fixture", {}).get("source_sha256"),
        "outcomes_digest_equal": first.get("expected_fixture", {}).get("outcomes_sha256")
        == replay.get("outcomes_sha256") == second.get("expected_fixture", {}).get("outcomes_sha256"),
    }
    return {"status": "PASS" if all(checks.values()) else "BLOCK", "checks": checks,
            "quality_sha256": sha256(first_quality), "replay_manifest_sha256": sha256(second_load)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("negative", "database"), required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    try:
        data = run_negative_contracts() if args.mode == "negative" else audit_postgres()
    except Exception as exc:
        # Avoid printing raw SQL connections, record content, local source paths or credentials.
        data = {"status": "BLOCK", "error_type": type(exc).__name__, "mode": args.mode}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(data, indent=2, sort_keys=True))
    return 0 if data["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
