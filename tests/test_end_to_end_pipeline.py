from pathlib import Path

import pandas as pd
import pytest

import src.ai_enrichment as ai_enrichment
import src.database as database
import src.evaluation as evaluation

from src.batch_ai_enrichment import enrich_candidate_batch
from src.final_routing import route_final_results
from src.ingestion import ingest_ticket_file
from src.routing import route_tickets
from src.run_status import finalize_pipeline_run
from src.summaries import build_daily_summary
from src.transformations import transform_tickets


@pytest.fixture
def isolated_environment(
    tmp_path: Path,
    monkeypatch,
):
    """
    Isolate the complete pipeline from production state.

    - temporary SQLite database
    - temporary outputs directory
    - temporary ground-truth file
    - no real OpenAI API calls
    """

    test_db = (
        tmp_path
        / "end_to_end_test.db"
    )

    monkeypatch.setattr(
        database,
        "DB_PATH",
        test_db,
    )

    database.initialize_database()

    output_dir = (
        tmp_path
        / "outputs"
    )

    output_dir.mkdir()

    ground_truth_path = (
        tmp_path
        / "ground_truth.csv"
    )

    monkeypatch.setattr(
        evaluation,
        "GROUND_TRUTH_PATH",
        ground_truth_path,
    )

    monkeypatch.setattr(
        evaluation,
        "OUTPUTS_DIR",
        output_dir,
    )

    monkeypatch.setattr(
        evaluation,
        "EVALUATION_OUTPUT",
        output_dir
        / "pipeline_evaluation.csv",
    )

    monkeypatch.setattr(
        evaluation,
        "MISMATCH_OUTPUT",
        output_dir
        / "evaluation_mismatches.csv",
    )

    return {
        "tmp_path": tmp_path,
        "db_path": test_db,
        "output_dir": output_dir,
        "ground_truth_path":
            ground_truth_path,
    }


def create_source_file(
    tmp_path: Path,
) -> Path:
    """
    Create seven synthetic records:

    4 deterministic classifications
    3 AI candidates
    """

    source_file = (
        tmp_path
        / "tickets_e2e.csv"
    )

    pd.DataFrame(
        [
            {
                "ticket_id": "1",
                "created_date": "2026-01-01",
                "ticket_text":
                    "My paycheck amount looks incorrect.",
            },
            {
                "ticket_id": "2",
                "created_date": "2026-01-02",
                "ticket_text":
                    "My password reset is not working.",
            },
            {
                "ticket_id": "3",
                "created_date": "2026-01-03",
                "ticket_text":
                    "Where can I find the required training?",
            },
            {
                "ticket_id": "4",
                "created_date": "2026-01-04",
                "ticket_text":
                    "I do not know which onboarding forms are left.",
            },
            {
                "ticket_id": "5",
                "created_date": "2026-01-05",
                "ticket_text":
                    "The application crashes when I upload a file.",
            },
            {
                "ticket_id": "6",
                "created_date": "2026-01-06",
                "ticket_text":
                    "The pay rate in my profile is wrong.",
            },
            {
                "ticket_id": "7",
                "created_date": "2026-01-07",
                "ticket_text":
                    "My checklist still says incomplete.",
            },
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    return source_file


def create_ground_truth(
    path: Path,
) -> None:
    """
    Lock expected behavior before the simulated AI stage.
    """

    pd.DataFrame(
        [
            {
                "ticket_id": 1,
                "expected_category": "PAYROLL",
                "expected_route": "DETERMINISTIC",
                "expected_ai_needed": 0,
                "expected_human_review": 0,
            },
            {
                "ticket_id": 2,
                "expected_category": "IT_SUPPORT",
                "expected_route": "DETERMINISTIC",
                "expected_ai_needed": 0,
                "expected_human_review": 0,
            },
            {
                "ticket_id": 3,
                "expected_category": "TRAINING",
                "expected_route": "DETERMINISTIC",
                "expected_ai_needed": 0,
                "expected_human_review": 0,
            },
            {
                "ticket_id": 4,
                "expected_category": "ONBOARDING",
                "expected_route": "DETERMINISTIC",
                "expected_ai_needed": 0,
                "expected_human_review": 0,
            },
            {
                "ticket_id": 5,
                "expected_category": "IT_SUPPORT",
                "expected_route": "AI",
                "expected_ai_needed": 1,
                "expected_human_review": 0,
            },
            {
                "ticket_id": 6,
                "expected_category": "PAYROLL",
                "expected_route": "AI",
                "expected_ai_needed": 1,
                "expected_human_review": 0,
            },
            {
                "ticket_id": 7,
                "expected_category": "",
                "expected_route": "HUMAN_REVIEW",
                "expected_ai_needed": 1,
                "expected_human_review": 1,
            },
        ]
    ).to_csv(
        path,
        index=False,
    )


def mock_ai_call(
    evidence_text: str,
) -> dict:
    """
    Deterministic replacement for the OpenAI API.

    This tests pipeline behavior without network calls
    or API cost.
    """

    text = evidence_text.lower()

    if "application crashes" in text:
        return {
            "category": "IT_SUPPORT",
            "sentiment": "NEGATIVE",
            "summary":
                "Application crashes during file upload.",
            "confidence": 0.99,
        }

    if "pay rate" in text:
        return {
            "category": "PAYROLL",
            "sentiment": "NEGATIVE",
            "summary":
                "Employee reports an incorrect pay rate.",
            "confidence": 0.99,
        }

    if "checklist" in text:
        return {
            "category": "UNRESOLVED",
            "sentiment": "NEUTRAL",
            "summary":
                "Checklist remains incomplete but context is insufficient.",
            "confidence": 0.97,
        }

    raise AssertionError(
        f"Unexpected AI candidate: "
        f"{evidence_text}"
    )


def test_full_pipeline_end_to_end(
    isolated_environment,
    monkeypatch,
):
    """
    Execute the complete Project 7 processing pipeline
    without touching the production database or network.
    """

    environment = (
        isolated_environment
    )

    source_file = create_source_file(
        environment["tmp_path"]
    )

    create_ground_truth(
        environment[
            "ground_truth_path"
        ]
    )

    # Prevent any real OpenAI API request.
    monkeypatch.setattr(
        ai_enrichment,
        "call_ai",
        mock_ai_call,
    )

    run_id = "TEST-E2E-001"

    # ---------------------------------------------------------
    # 1. INGEST
    # ---------------------------------------------------------

    ingestion = ingest_ticket_file(
        source_file,
        run_id=run_id,
    )

    assert ingestion["source_rows"] == 7
    assert ingestion["valid_rows"] == 7
    assert ingestion["invalid_rows"] == 0
    assert ingestion["inserted_rows"] == 7

    # ---------------------------------------------------------
    # 2. TRANSFORM
    # ---------------------------------------------------------

    transformation = transform_tickets(
        run_id=run_id
    )

    assert (
        transformation["clean_rows"]
        == 7
    )

    assert (
        transformation["row_count_match"]
        == 1
    )

    # ---------------------------------------------------------
    # 3. ROUTE
    # ---------------------------------------------------------

    routing = route_tickets(
        run_id=run_id
    )

    assert (
        routing["deterministic_rows"]
        == 4
    )

    assert (
        routing["ai_candidate_rows"]
        == 3
    )

    assert routing["overlap_rows"] == 0
    assert routing["unrouted_rows"] == 0

    # ---------------------------------------------------------
    # 4. SELECTIVE AI
    # ---------------------------------------------------------

    ai_result = enrich_candidate_batch(
        run_id=run_id
    )

    assert (
        ai_result["candidate_rows"]
        == 3
    )

    assert (
        ai_result["successful_rows"]
        == 3
    )

    assert (
        ai_result["failed_rows"]
        == 0
    )

    assert (
        ai_result["api_calls"]
        == 3
    )

    # These are simulated calls, not network calls.
    assert (
        ai_result["reused_results"]
        == 0
    )

    # ---------------------------------------------------------
    # 5. FINAL ROUTING
    # ---------------------------------------------------------

    final = route_final_results(
        run_id=run_id
    )

    assert final["final_rows"] == 7

    assert (
        final["deterministic_rows"]
        == 4
    )

    assert (
        final["ai_accepted_rows"]
        == 2
    )

    assert (
        final["human_review_rows"]
        == 1
    )

    assert (
        final[
            "ai_failure_fallback_rows"
        ]
        == 0
    )

    # ---------------------------------------------------------
    # 6. SUMMARY
    # ---------------------------------------------------------

    summary = build_daily_summary(
        run_id=run_id
    )

    assert (
        summary["summarized_tickets"]
        == 7
    )

    assert (
        summary["deterministic_tickets"]
        == 4
    )

    assert (
        summary["ai_enriched_tickets"]
        == 2
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
        summary["row_count_match"]
        == 1
    )

    # ---------------------------------------------------------
    # 7. EVALUATION
    # ---------------------------------------------------------

    evaluation_result = (
        evaluation.evaluate_pipeline()
    )

    assert (
        evaluation_result[
            "evaluated_rows"
        ]
        == 7
    )

    assert (
        evaluation_result[
            "deterministic_accuracy_pct"
        ]
        == pytest.approx(100.0)
    )

    assert (
        evaluation_result[
            "ai_candidate_precision_pct"
        ]
        == pytest.approx(100.0)
    )

    assert (
        evaluation_result[
            "ai_candidate_recall_pct"
        ]
        == pytest.approx(100.0)
    )

    assert (
        evaluation_result[
            "ai_category_accuracy_pct"
        ]
        == pytest.approx(100.0)
    )

    assert (
        evaluation_result[
            "human_review_recall_pct"
        ]
        == pytest.approx(100.0)
    )

    assert (
        evaluation_result[
            "final_handling_accuracy_pct"
        ]
        == pytest.approx(100.0)
    )

    assert (
        evaluation_result[
            "overall_category_accuracy_pct"
        ]
        == pytest.approx(100.0)
    )

    assert (
        evaluation_result[
            "mismatch_rows"
        ]
        == 0
    )

    # ---------------------------------------------------------
    # 8. FINALIZE
    # ---------------------------------------------------------

    completion = finalize_pipeline_run(
        run_id
    )

    assert (
        completion["status"]
        == "SUCCESS"
    )

    # ---------------------------------------------------------
    # FINAL DATABASE INTEGRITY
    # ---------------------------------------------------------

    connection = (
        database.get_connection()
    )

    try:
        run = connection.execute(
            """
            SELECT
                status,
                source_row_count,
                clean_row_count,
                deterministic_count,
                ai_candidate_count,
                ai_success_count,
                ai_failure_count,
                human_review_count
            FROM pipeline_run
            WHERE run_id = ?;
            """,
            (run_id,),
        ).fetchone()

        staging_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM stg_raw_tickets;
                """
            ).fetchone()[0]
        )

        clean_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM ticket_clean;
                """
            ).fetchone()[0]
        )

        final_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM final_ticket_classification;
                """
            ).fetchone()[0]
        )

    finally:
        connection.close()

    assert run["status"] == "SUCCESS"
    assert run["source_row_count"] == 7
    assert run["clean_row_count"] == 7
    assert run["deterministic_count"] == 4
    assert run["ai_candidate_count"] == 3
    assert run["ai_success_count"] == 3
    assert run["ai_failure_count"] == 0
    assert run["human_review_count"] == 1

    assert staging_count == 7
    assert clean_count == 7
    assert final_count == 7
