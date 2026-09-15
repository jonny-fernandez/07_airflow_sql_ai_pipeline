from datetime import datetime, timezone

from src.batch_ai_enrichment import enrich_candidate_batch
from src.config import INCOMING_DIR
from src.database import get_connection, initialize_database
from src.evaluation import evaluate_pipeline
from src.final_routing import route_final_results
from src.ingestion import ingest_ticket_file
from src.routing import route_tickets
from src.summaries import build_daily_summary
from src.transformations import transform_tickets


SOURCE_FILE = (
    INCOMING_DIR
    / "tickets_batch_001.csv"
)


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(
        timezone.utc
    ).isoformat()


def complete_pipeline_run(
    run_id: str,
    status: str,
) -> None:
    """
    Mark the overall pipeline execution complete.

    A run may succeed cleanly or succeed while retaining
    quarantined data-quality exceptions.
    """

    connection = get_connection()

    try:
        connection.execute(
            """
            UPDATE pipeline_run
            SET
                completed_at = ?,
                status = ?
            WHERE run_id = ?;
            """,
            (
                utc_now(),
                status,
                run_id,
            ),
        )

        connection.commit()

    finally:
        connection.close()


def fail_pipeline_run(
    run_id: str,
    stage: str,
    error: Exception,
) -> None:
    """
    Persist an unrecoverable pipeline-stage failure.

    This is distinct from record-level data-quality,
    AI-failure, and human-review outcomes.
    """

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO pipeline_errors (
                run_id,
                task_name,
                error_type,
                error_message,
                occurred_at
            )
            VALUES (?, ?, ?, ?, ?);
            """,
            (
                run_id,
                stage,
                type(error).__name__,
                str(error),
                utc_now(),
            ),
        )

        connection.execute(
            """
            UPDATE pipeline_run
            SET
                completed_at = ?,
                status = 'FAILED'
            WHERE run_id = ?;
            """,
            (
                utc_now(),
                run_id,
            ),
        )

        connection.commit()

    finally:
        connection.close()


def print_stage(
    number: int,
    name: str,
) -> None:
    """Print a visible pipeline stage header."""

    print()
    print(
        f"[{number}] {name}"
    )
    print(
        "-" * (len(name) + 4)
    )


def run_pipeline() -> dict:
    """
    Execute Project 7 end-to-end outside Airflow.

    Airflow will later orchestrate these same modular
    functions rather than duplicating their logic.
    """

    initialize_database()

    run_id = None
    current_stage = "ingestion"

    try:
        # =====================================================
        # 1. INGESTION
        # =====================================================

        print_stage(
            1,
            "INGEST SOURCE",
        )

        ingestion = ingest_ticket_file(
            SOURCE_FILE
        )

        run_id = ingestion["run_id"]

        print(
            f"Run ID:          {run_id}"
        )
        print(
            f"Inserted:        "
            f"{ingestion['inserted_rows']}"
        )
        print(
            f"Updated:         "
            f"{ingestion['updated_rows']}"
        )
        print(
            f"Unchanged:       "
            f"{ingestion['unchanged_rows']}"
        )
        print(
            f"Invalid:         "
            f"{ingestion['invalid_rows']}"
        )

        # =====================================================
        # 2. TRANSFORM
        # =====================================================

        current_stage = "transform_tickets"

        print_stage(
            2,
            "TRANSFORM TICKETS",
        )

        transformation = transform_tickets(
            run_id=run_id
        )

        print(
            f"Clean rows:      "
            f"{transformation['clean_rows']}"
        )
        print(
            f"Row-count match: "
            f"{bool(transformation['row_count_match'])}"
        )

        # =====================================================
        # 3. DETERMINISTIC ROUTING
        # =====================================================

        current_stage = "route_tickets"

        print_stage(
            3,
            "DETERMINISTIC ROUTING",
        )

        routing = route_tickets(
            run_id=run_id
        )

        print(
            f"Deterministic:   "
            f"{routing['deterministic_rows']}"
        )
        print(
            f"AI candidates:   "
            f"{routing['ai_candidate_rows']}"
        )

        # =====================================================
        # 4. SELECTIVE AI ENRICHMENT
        # =====================================================

        current_stage = "ai_enrichment"

        print_stage(
            4,
            "SELECTIVE AI ENRICHMENT",
        )

        ai = enrich_candidate_batch(
            run_id=run_id
        )

        print(
            f"Candidates:      "
            f"{ai['candidate_rows']}"
        )
        print(
            f"API calls:       "
            f"{ai['api_calls']}"
        )
        print(
            f"Reused:          "
            f"{ai['reused_results']}"
        )
        print(
            f"AI failures:     "
            f"{ai['failed_rows']}"
        )

        # =====================================================
        # 5. FINAL VALIDATION / ROUTING
        # =====================================================

        current_stage = "validate_and_route"

        print_stage(
            5,
            "VALIDATE AND ROUTE",
        )

        final = route_final_results(
            run_id=run_id
        )

        print(
            f"Deterministic:   "
            f"{final['deterministic_rows']}"
        )
        print(
            f"AI accepted:     "
            f"{final['ai_accepted_rows']}"
        )
        print(
            f"Human review:    "
            f"{final['human_review_rows']}"
        )
        print(
            f"AI fallback:     "
            f"{final['ai_failure_fallback_rows']}"
        )

        # =====================================================
        # 6. ANALYTICAL SUMMARIES
        # =====================================================

        current_stage = "build_summaries"

        print_stage(
            6,
            "BUILD SUMMARIES",
        )

        summary = build_daily_summary(
            run_id=run_id
        )

        print(
            f"Summarized:      "
            f"{summary['summarized_tickets']}"
        )
        print(
            f"Row-count match: "
            f"{bool(summary['row_count_match'])}"
        )

        # =====================================================
        # 7. EVALUATION
        # =====================================================

        current_stage = "evaluate_pipeline"

        print_stage(
            7,
            "EVALUATE PIPELINE",
        )

        evaluation = evaluate_pipeline()

        print(
            f"Final handling:  "
            f"{evaluation['final_handling_accuracy_pct']:.1f}%"
        )
        print(
            f"Category:        "
            f"{evaluation['overall_category_accuracy_pct']:.1f}%"
        )
        print(
            f"Mismatches:      "
            f"{evaluation['mismatch_rows']}"
        )

        # =====================================================
        # PIPELINE COMPLETION
        # =====================================================

        completion_status = (
            "COMPLETED_WITH_EXCEPTIONS"
            if ingestion["invalid_rows"] > 0
            else "SUCCESS"
        )

        complete_pipeline_run(
            run_id,
            completion_status,
        )

        print()
        print("=" * 55)
        print("PIPELINE COMPLETE")
        print("=" * 55)

        print(
            f"Run ID:          {run_id}"
        )
        print(
            f"Status:          "
            f"{completion_status}"
        )
        print(
            f"Source rows:     "
            f"{ingestion['source_rows']}"
        )
        print(
            f"Clean rows:      "
            f"{transformation['clean_rows']}"
        )
        print(
            f"Deterministic:   "
            f"{routing['deterministic_rows']}"
        )
        print(
            f"AI candidates:   "
            f"{routing['ai_candidate_rows']}"
        )
        print(
            f"AI accepted:     "
            f"{final['ai_accepted_rows']}"
        )
        print(
            f"Human review:    "
            f"{final['human_review_rows']}"
        )
        print(
            f"Mismatches:      "
            f"{evaluation['mismatch_rows']}"
        )

        return {
            "run_id": run_id,
            "status": completion_status,
        }

    except Exception as error:
        if run_id is not None:
            fail_pipeline_run(
                run_id,
                current_stage,
                error,
            )

        print()
        print("=" * 55)
        print("PIPELINE FAILED")
        print("=" * 55)

        print(
            f"Stage: "
            f"{current_stage}"
        )
        print(
            f"Error: "
            f"{type(error).__name__}: "
            f"{error}"
        )

        raise


if __name__ == "__main__":
    run_pipeline()
