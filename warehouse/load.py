"""Synthetic-only, idempotent Postgres loader for Project 07's governed warehouse.

Never send source ticket text to external providers. No real personnel data is allowed.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/incoming/tickets_batch_001.csv"
OUTCOMES = ROOT / "outputs/final_ticket_results.csv"
REQUIRED_SOURCE = ("ticket_id", "created_date", "ticket_text")
REQUIRED_OUTCOMES = (
    "ticket_id", "created_date", "ticket_text", "final_category", "sentiment",
    "summary", "classification_source", "ai_used", "final_handling",
)
ALLOWED_HANDLING = {"DETERMINISTIC", "AI_ACCEPTED", "HUMAN_REVIEW"}
ALLOWED_SOURCE = {"DETERMINISTIC", "AI", "HUMAN_REVIEW"}


def canonical_hash(record: dict) -> str:
    payload = json.dumps(record, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_csv(path: Path, required: tuple[str, ...]) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None or any(key not in reader.fieldnames for key in required):
            raise ValueError(f"Invalid columns in {path.name}; required={required}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"Empty fixture: {path.name}")
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError(f"Malformed CSV fields in {path.name}")
    return rows


def validate_fixture(source_path: Path = SOURCE, outcomes_path: Path = OUTCOMES) -> tuple[list[dict], list[dict], dict]:
    """Fail closed on missing, duplicate, mismatched or invalid synthetic records."""
    sources = read_csv(source_path, REQUIRED_SOURCE)
    outcomes = read_csv(outcomes_path, REQUIRED_OUTCOMES)
    ids: set[int] = set()
    by_id: dict[int, dict] = {}
    for row in sources:
        ticket = int(row["ticket_id"])
        if ticket < 1 or ticket in ids:
            raise ValueError(f"Invalid or duplicated ticket ID: {ticket}")
        ids.add(ticket)
        date.fromisoformat(row["created_date"])
        if not row["ticket_text"].strip():
            raise ValueError(f"Empty ticket_text for {ticket}")
        by_id[ticket] = row
    seen: set[int] = set()
    for row in outcomes:
        ticket = int(row["ticket_id"])
        if ticket in seen or ticket not in by_id:
            raise ValueError(f"Duplicate or unmatched outcome: {ticket}")
        seen.add(ticket)
        if any(row[key] != by_id[ticket][key] for key in ("created_date", "ticket_text")):
            raise ValueError(f"Source/outcome conflict for ticket {ticket}")
        if row["classification_source"] not in ALLOWED_SOURCE or row["final_handling"] not in ALLOWED_HANDLING:
            raise ValueError(f"Unsupported classification contract for ticket {ticket}")
        if row["ai_used"] not in {"0", "1"}:
            raise ValueError(f"Invalid ai_used flag for ticket {ticket}")
        if (row["classification_source"], row["ai_used"], row["final_handling"]) not in {
            ("DETERMINISTIC", "0", "DETERMINISTIC"),
            ("AI", "1", "AI_ACCEPTED"),
            ("HUMAN_REVIEW", "1", "HUMAN_REVIEW"),
        }:
            raise ValueError(f"Inconsistent AI vs deterministic routing for ticket {ticket}")
        if not row["final_category"] and row["final_handling"] != "HUMAN_REVIEW":
            raise ValueError(f"Missing accepted category for ticket {ticket}")
    if ids != seen:
        raise ValueError(f"Missing outcomes: {sorted(ids - seen)}")
    stats = {
        "source_rows": len(sources),
        "outcome_rows": len(outcomes),
        "deterministic_rows": sum(x["final_handling"] == "DETERMINISTIC" for x in outcomes),
        "ai_accepted_rows": sum(x["final_handling"] == "AI_ACCEPTED" for x in outcomes),
        "human_review_rows": sum(x["final_handling"] == "HUMAN_REVIEW" for x in outcomes),
        "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "outcomes_sha256": hashlib.sha256(outcomes_path.read_bytes()).hexdigest(),
    }
    return sources, outcomes, stats


def connect():
    import psycopg2  # installed inside the isolated dbt service
    return psycopg2.connect(
        host=os.environ.get("P07_DB_HOST", "postgres"),
        port=int(os.environ.get("P07_DB_PORT", "5432")),
        dbname=os.environ.get("P07_DB_NAME", "project07"),
        user=os.environ.get("P07_DB_USER", "portfolio"),
        password=os.environ.get("P07_DB_PASSWORD", "portfolio_dev_only"),
        connect_timeout=10,
    )


def load(source_path: Path = SOURCE, outcomes_path: Path = OUTCOMES) -> dict:
    sources, outcomes, stats = validate_fixture(source_path, outcomes_path)
    source_batch = "fixture_" + stats["source_sha256"][:12]
    counts = {"source_changed_rows": 0, "outcome_changed_rows": 0}
    with connect() as conn:
        with conn.cursor() as cursor:
            for row in sources:
                digest = canonical_hash({k: row[k] for k in REQUIRED_SOURCE})
                cursor.execute(
                    """INSERT INTO raw.incoming_tickets(ticket_id,created_date,ticket_text,content_sha256,source_batch)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT(ticket_id) DO UPDATE SET
                      created_date=EXCLUDED.created_date,
                      ticket_text=EXCLUDED.ticket_text,
                      content_sha256=EXCLUDED.content_sha256,
                      source_batch=EXCLUDED.source_batch,
                      source_loaded_at=CURRENT_TIMESTAMP
                    WHERE raw.incoming_tickets.content_sha256 IS DISTINCT FROM EXCLUDED.content_sha256""",
                    (int(row["ticket_id"]), row["created_date"], row["ticket_text"], digest, source_batch),
                )
                counts["source_changed_rows"] += cursor.rowcount
            for row in outcomes:
                digest = canonical_hash({k: row[k] for k in REQUIRED_OUTCOMES})
                cursor.execute(
                    """INSERT INTO raw.ticket_outcomes (
                       ticket_id, final_category, sentiment, summary, classification_source,
                       ai_used, final_handling, content_sha256, source_batch
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(ticket_id) DO UPDATE SET
                      final_category=EXCLUDED.final_category,
                      sentiment=EXCLUDED.sentiment,
                      summary=EXCLUDED.summary,
                      classification_source=EXCLUDED.classification_source,
                      ai_used=EXCLUDED.ai_used,
                      final_handling=EXCLUDED.final_handling,
                      content_sha256=EXCLUDED.content_sha256,
                      source_batch=EXCLUDED.source_batch,
                      source_loaded_at=CURRENT_TIMESTAMP
                    WHERE raw.ticket_outcomes.content_sha256 IS DISTINCT FROM EXCLUDED.content_sha256""",
                    (
                        int(row["ticket_id"]), row["final_category"] or None, row["sentiment"] or None,
                        row["summary"] or None, row["classification_source"], row["ai_used"] == "1",
                        row["final_handling"], digest, source_batch,
                    ),
                )
                counts["outcome_changed_rows"] += cursor.rowcount
    result = {"status": "PASS", "batch": source_batch, **stats, **counts}
    output = Path(os.environ.get("P07_OUTPUT", "warehouse/artifacts"))
    output.mkdir(parents=True, exist_ok=True)
    (output / "load_manifest.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


if __name__ == "__main__":
    load()
