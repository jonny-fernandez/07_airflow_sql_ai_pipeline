from pathlib import Path

import pytest

import src.database as database

from src.ai_enrichment import (
    MODEL,
    PROMPT_VERSION,
)
from src.final_routing import route_final_results
from src.summaries import build_daily_summary


@pytest.fixture
def isolated_database(
    tmp_path: Path,
    monkeypatch,
):
    """
    Redirect all database operations to a temporary SQLite DB.
    The real portfolio database is never modified.
    """

    test_db = (
        tmp_path
        / "final_routing_test.db"
    )

    monkeypatch.setattr(
        database,
        "DB_PATH",
        test_db,
    )

    database.initialize_database()

    return test_db


def seed_candidate(
    *,
    run_id: str,
    ticket_id: int,
    category: str | None,
    confidence: float | None,
    result_status: str,
):
    """
    Create the minimum valid pipeline state required
    to test final deterministic handling.
    """

    connection = database.get_connection()

    try:
        connection.execute(
            """
            INSERT INTO pipeline_run (
                run_id,
                source_file,
                started_at,
                status
            )
            VALUES (
                ?,
                'test.csv',
                '2026-01-01T00:00:00+00:00',
                'RUNNING'
            );
            """,
            (run_id,),
        )

        connection.execute(
            """
            INSERT INTO ticket_clean (
                ticket_id,
                created_date,
                ticket_text,
                normalized_text,
                source_hash,
                cleaned_at
            )
            VALUES (
                ?,
                '2026-01-01',
                'Synthetic test ticket.',
                'synthetic test ticket.',
                'HASH-001',
                '2026-01-01T00:00:00+00:00'
            );
            """,
            (ticket_id,),
        )

        connection.execute(
            """
            INSERT INTO ai_candidate (
                ticket_id,
                evidence_text,
                candidate_reason,
                input_hash,
                created_at
            )
            VALUES (
                ?,
                'Synthetic test ticket.',
                'TEST_AI_CANDIDATE',
                'HASH-001',
                '2026-01-01T00:00:00+00:00'
            );
            """,
            (ticket_id,),
        )

        connection.execute(
            """
            INSERT INTO ai_classification (
                ticket_id,
                evidence_text,
                input_hash,
                prompt_version,
                model,
                category,
                sentiment,
                summary,
                confidence,
                ai_used,
                result_status,
                error_type,
                error_message,
                processed_at
            )
            VALUES (
                ?,
                'Synthetic test ticket.',
                'HASH-001',
                ?,
                ?,
                ?,
                'NEGATIVE',
                'Synthetic structured result.',
                ?,
                1,
                ?,
                NULL,
                NULL,
                '2026-01-01T00:00:00+00:00'
            );
            """,
            (
                ticket_id,
                PROMPT_VERSION,
                MODEL,
                category,
                confidence,
                result_status,
            ),
        )

        connection.commit()

    finally:
        connection.close()


def test_failed_ai_result_routes_to_fallback_and_summary(
    isolated_database,
):
    """
    A record-level AI failure must not become a pipeline
    failure. It should route to AI_FAILURE_FALLBACK and
    remain reconcilable in analytical summaries.
    """

    run_id = "TEST-AI-FAILURE"

    seed_candidate(
        run_id=run_id,
        ticket_id=1,
        category=None,
        confidence=None,
        result_status="FAILED",
    )

    result = route_final_results(
        run_id=run_id
    )

    assert result[
        "ai_failure_fallback_rows"
    ] == 1

    assert result[
        "human_review_rows"
    ] == 0

    connection = database.get_connection()

    try:
        final_row = connection.execute(
            """
            SELECT
                classification_source,
                ai_used,
                final_handling
            FROM final_ticket_classification
            WHERE ticket_id = 1;
            """
        ).fetchone()

    finally:
        connection.close()

    assert (
        final_row["classification_source"]
        == "FALLBACK"
    )

    assert (
        final_row["final_handling"]
        == "AI_FAILURE_FALLBACK"
    )

    assert final_row["ai_used"] == 1

    summary = build_daily_summary(
        run_id=run_id
    )

    assert (
        summary[
            "ai_failure_fallback_tickets"
        ]
        == 1
    )

    assert (
        summary["summarized_tickets"]
        == 1
    )

    assert (
        summary["row_count_match"]
        == 1
    )

    assert (
        summary["categories"][0][
            "category"
        ]
        == "AI_FAILURE_FALLBACK"
    )


def test_low_confidence_ai_routes_to_human_review(
    isolated_database,
):
    """
    A structurally valid AI result below the configured
    confidence threshold must be routed to human review.
    """

    run_id = "TEST-LOW-CONFIDENCE"

    seed_candidate(
        run_id=run_id,
        ticket_id=1,
        category="PAYROLL",
        confidence=0.40,
        result_status="SUCCESS",
    )

    result = route_final_results(
        run_id=run_id
    )

    assert (
        result["human_review_rows"]
        == 1
    )

    assert (
        result[
            "ai_failure_fallback_rows"
        ]
        == 0
    )

    connection = database.get_connection()

    try:
        final_row = connection.execute(
            """
            SELECT
                final_category,
                classification_source,
                final_handling
            FROM final_ticket_classification
            WHERE ticket_id = 1;
            """
        ).fetchone()

        review_row = connection.execute(
            """
            SELECT
                review_reason,
                proposed_category,
                confidence,
                review_status
            FROM human_review_queue
            WHERE ticket_id = 1;
            """
        ).fetchone()

    finally:
        connection.close()

    assert final_row["final_category"] is None

    assert (
        final_row["classification_source"]
        == "HUMAN_REVIEW"
    )

    assert (
        final_row["final_handling"]
        == "HUMAN_REVIEW"
    )

    assert (
        review_row["review_reason"]
        == "LOW_AI_CONFIDENCE"
    )

    assert (
        review_row["proposed_category"]
        == "PAYROLL"
    )

    assert (
        review_row["confidence"]
        == pytest.approx(0.40)
    )

    assert (
        review_row["review_status"]
        == "PENDING"
    )

    summary = build_daily_summary(
        run_id=run_id
    )

    assert (
        summary["human_review_tickets"]
        == 1
    )

    assert (
        summary[
            "ai_failure_fallback_tickets"
        ]
        == 0
    )

    assert (
        summary["summarized_tickets"]
        == 1
    )
