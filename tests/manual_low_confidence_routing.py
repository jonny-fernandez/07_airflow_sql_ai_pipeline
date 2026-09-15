from src.ai_enrichment import MODEL, PROMPT_VERSION
from src.database import get_connection
from src.final_routing import route_final_results


TEST_TICKET_ID = 29
TEST_CONFIDENCE = 0.40


def get_latest_successful_run_id() -> str:
    """Return the latest completed pipeline execution."""

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


def run_low_confidence_test() -> None:
    """
    Temporarily lower the confidence of one valid AI result.

    Final deterministic routing should send the record to
    human review. The original state is restored afterward.
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
                result_status
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

        print("LOW-CONFIDENCE ROUTING TEST")
        print("---------------------------")

        print(
            f"Ticket ID:            "
            f"{TEST_TICKET_ID}"
        )

        print(
            f"Original category:    "
            f"{original['category']}"
        )

        print(
            f"Original confidence:  "
            f"{original['confidence']}"
        )

        connection.execute(
            """
            UPDATE ai_classification
            SET confidence = ?
            WHERE ai_result_id = ?;
            """,
            (
                TEST_CONFIDENCE,
                original["ai_result_id"],
            ),
        )

        connection.commit()

    finally:
        connection.close()

    run_id = get_latest_successful_run_id()

    try:
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

            review_row = connection.execute(
                """
                SELECT
                    ticket_id,
                    review_reason,
                    proposed_category,
                    confidence,
                    review_status
                FROM human_review_queue
                WHERE ticket_id = ?;
                """,
                (
                    TEST_TICKET_ID,
                ),
            ).fetchone()

            if final_row is None:
                raise RuntimeError(
                    "Test ticket missing from final output."
                )

            if review_row is None:
                raise RuntimeError(
                    "Test ticket was not added to "
                    "human_review_queue."
                )

            final_row = dict(final_row)
            review_row = dict(review_row)

        finally:
            connection.close()

        print()
        print("SIMULATED LOW-CONFIDENCE RESULT")
        print("-------------------------------")

        print(
            f"Final category:         "
            f"{final_row['final_category']}"
        )

        print(
            f"Classification source: "
            f"{final_row['classification_source']}"
        )

        print(
            f"Final handling:         "
            f"{final_row['final_handling']}"
        )

        print(
            f"Review reason:          "
            f"{review_row['review_reason']}"
        )

        print(
            f"Proposed category:      "
            f"{review_row['proposed_category']}"
        )

        print(
            f"Confidence:             "
            f"{review_row['confidence']}"
        )

        print(
            f"Human-review count:     "
            f"{result['human_review_rows']}"
        )

        # -----------------------------------------------------
        # Assertions
        # -----------------------------------------------------

        if (
            final_row["classification_source"]
            != "HUMAN_REVIEW"
        ):
            raise AssertionError(
                "Expected classification_source="
                "HUMAN_REVIEW."
            )

        if (
            final_row["final_handling"]
            != "HUMAN_REVIEW"
        ):
            raise AssertionError(
                "Expected final_handling=HUMAN_REVIEW."
            )

        if (
            review_row["review_reason"]
            != "LOW_AI_CONFIDENCE"
        ):
            raise AssertionError(
                "Expected review_reason="
                "LOW_AI_CONFIDENCE."
            )

        if (
            review_row["proposed_category"]
            != original["category"]
        ):
            raise AssertionError(
                "Expected proposed category to preserve "
                "the AI classification."
            )

        print()
        print("LOW-CONFIDENCE ROUTING TEST: PASS")

    finally:
        # -----------------------------------------------------
        # Restore original AI confidence.
        # -----------------------------------------------------

        connection = get_connection()

        try:
            connection.execute(
                """
                UPDATE ai_classification
                SET confidence = ?
                WHERE ai_result_id = ?;
                """,
                (
                    original["confidence"],
                    original["ai_result_id"],
                ),
            )

            connection.commit()

        finally:
            connection.close()

        # Rebuild final output from restored state.
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
            f"Human-review rows: "
            f"{restored['human_review_rows']}"
        )


if __name__ == "__main__":
    run_low_confidence_test()
