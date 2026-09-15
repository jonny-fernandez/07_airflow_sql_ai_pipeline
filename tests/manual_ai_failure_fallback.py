from src.ai_enrichment import MODEL, PROMPT_VERSION
from src.database import get_connection
from src.final_routing import route_final_results


TEST_TICKET_ID = 4


def get_latest_successful_run_id() -> str:
    """
    Use the most recent completed pipeline execution for
    test metrics without creating a new business-data run.
    """

    connection = get_connection()

    try:
        row = connection.execute(
            """
            SELECT run_id
            FROM pipeline_run
            WHERE status IN (
                'SUCCESS',
                'COMPLETED_WITH_EXCEPTIONS'
            )
            ORDER BY started_at DESC
            LIMIT 1;
            """
        ).fetchone()

        if row is None:
            raise RuntimeError(
                "No successful pipeline run exists."
            )

        return row["run_id"]

    finally:
        connection.close()


def run_ai_failure_fallback_test() -> None:
    """
    Simulate a failed AI enrichment for one existing candidate.

    The original AI result is restored at the end so this test
    does not permanently alter the working pipeline state.
    """

    connection = get_connection()

    try:
        original = connection.execute(
            """
            SELECT
                ai_result_id,
                ticket_id,
                category,
                sentiment,
                summary,
                confidence,
                result_status,
                error_type,
                error_message,
                processed_at
            FROM ai_classification
            WHERE ticket_id = ?
              AND prompt_version = ?
              AND model = ?
            LIMIT 1;
            """,
            (
                TEST_TICKET_ID,
                PROMPT_VERSION,
                MODEL,
            ),
        ).fetchone()

        if original is None:
            raise RuntimeError(
                f"No AI result found for ticket "
                f"{TEST_TICKET_ID}."
            )

        original = dict(original)

        print("AI FAILURE FALLBACK TEST")
        print("------------------------")

        print(
            f"Ticket ID:              "
            f"{TEST_TICKET_ID}"
        )

        print(
            f"Original result status: "
            f"{original['result_status']}"
        )

        print(
            f"Original category:      "
            f"{original['category']}"
        )

        # -----------------------------------------------------
        # Simulate an AI-specific failure.
        # -----------------------------------------------------

        connection.execute(
            """
            UPDATE ai_classification
            SET
                result_status = 'FAILED',
                error_type = 'SimulatedAIError',
                error_message =
                    'Controlled portfolio failure test.'
            WHERE ai_result_id = ?;
            """,
            (
                original["ai_result_id"],
            ),
        )

        connection.commit()

    finally:
        connection.close()

    run_id = get_latest_successful_run_id()

    try:
        # -----------------------------------------------------
        # Run deterministic final handling.
        # -----------------------------------------------------

        result = route_final_results(
            run_id=run_id
        )

        connection = get_connection()

        try:
            final_row = connection.execute(
                """
                SELECT
                    ticket_id,
                    final_category,
                    classification_source,
                    ai_used,
                    final_handling
                FROM final_ticket_classification
                WHERE ticket_id = ?;
                """,
                (
                    TEST_TICKET_ID,
                ),
            ).fetchone()

            if final_row is None:
                raise RuntimeError(
                    "Test ticket disappeared from "
                    "final output."
                )

            final_row = dict(final_row)

        finally:
            connection.close()

        print()
        print("SIMULATED FAILURE RESULT")
        print("------------------------")

        print(
            f"Final category:         "
            f"{final_row['final_category']}"
        )

        print(
            f"Classification source: "
            f"{final_row['classification_source']}"
        )

        print(
            f"AI used:                "
            f"{final_row['ai_used']}"
        )

        print(
            f"Final handling:         "
            f"{final_row['final_handling']}"
        )

        print(
            f"AI fallback count:      "
            f"{result['ai_failure_fallback_rows']}"
        )

        # -----------------------------------------------------
        # Assertions
        # -----------------------------------------------------

        if (
            final_row["classification_source"]
            != "FALLBACK"
        ):
            raise AssertionError(
                "Expected classification_source=FALLBACK."
            )

        if (
            final_row["final_handling"]
            != "AI_FAILURE_FALLBACK"
        ):
            raise AssertionError(
                "Expected "
                "final_handling=AI_FAILURE_FALLBACK."
            )

        if final_row["ai_used"] != 1:
            raise AssertionError(
                "Expected ai_used=1."
            )

        if (
            result["ai_failure_fallback_rows"]
            < 1
        ):
            raise AssertionError(
                "Expected at least one AI fallback."
            )

        print()
        print("AI FAILURE FALLBACK TEST: PASS")

    finally:
        # -----------------------------------------------------
        # Restore the original AI result.
        # -----------------------------------------------------

        connection = get_connection()

        try:
            connection.execute(
                """
                UPDATE ai_classification
                SET
                    category = ?,
                    sentiment = ?,
                    summary = ?,
                    confidence = ?,
                    result_status = ?,
                    error_type = ?,
                    error_message = ?,
                    processed_at = ?
                WHERE ai_result_id = ?;
                """,
                (
                    original["category"],
                    original["sentiment"],
                    original["summary"],
                    original["confidence"],
                    original["result_status"],
                    original["error_type"],
                    original["error_message"],
                    original["processed_at"],
                    original["ai_result_id"],
                ),
            )

            connection.commit()

        finally:
            connection.close()

        # Rebuild the final business layer from the restored
        # successful AI result.
        restored = route_final_results(
            run_id=run_id
        )

        connection = get_connection()

        try:
            restored_row = connection.execute(
                """
                SELECT
                    final_category,
                    final_handling
                FROM final_ticket_classification
                WHERE ticket_id = ?;
                """,
                (
                    TEST_TICKET_ID,
                ),
            ).fetchone()

        finally:
            connection.close()

        print()
        print("STATE RESTORED")
        print("--------------")

        print(
            f"Ticket {TEST_TICKET_ID} category: "
            f"{restored_row['final_category']}"
        )

        print(
            f"Ticket {TEST_TICKET_ID} handling: "
            f"{restored_row['final_handling']}"
        )

        print(
            f"Final rows: "
            f"{restored['final_rows']}"
        )


if __name__ == "__main__":
    run_ai_failure_fallback_test()
