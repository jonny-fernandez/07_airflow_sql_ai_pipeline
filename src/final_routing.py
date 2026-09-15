import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from src.ai_enrichment import (
    ALLOWED_CATEGORIES,
    ALLOWED_SENTIMENTS,
    MODEL,
    PROMPT_VERSION,
)
from src.config import PROJECT_ROOT
from src.database import (
    get_connection,
    initialize_database,
)


load_dotenv(PROJECT_ROOT / ".env")

CONFIDENCE_THRESHOLD = float(
    os.getenv(
        "AI_CONFIDENCE_THRESHOLD",
        "0.85",
    )
)

if not 0.0 <= CONFIDENCE_THRESHOLD <= 1.0:
    raise ValueError(
        "AI_CONFIDENCE_THRESHOLD must be between 0 and 1."
    )


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(
        timezone.utc
    ).isoformat()


def get_latest_run_id(connection) -> str:
    """Return the most recent pipeline run."""

    row = connection.execute(
        """
        SELECT run_id
        FROM pipeline_run
        ORDER BY started_at DESC
        LIMIT 1;
        """
    ).fetchone()

    if row is None:
        raise RuntimeError(
            "No pipeline run exists."
        )

    return row["run_id"]


def record_metric(
    connection,
    run_id: str,
    metric_name: str,
    metric_value: float,
) -> None:
    """Persist final-routing metrics."""

    connection.execute(
        """
        INSERT INTO task_run_metrics (
            run_id,
            task_name,
            metric_name,
            metric_value,
            recorded_at
        )
        VALUES (?, 'validate_and_route', ?, ?, ?)

        ON CONFLICT (
            run_id,
            task_name,
            metric_name
        )
        DO UPDATE SET
            metric_value = excluded.metric_value,
            recorded_at = excluded.recorded_at;
        """,
        (
            run_id,
            metric_name,
            metric_value,
            utc_now(),
        ),
    )


def validate_ai_result(row) -> tuple[bool, str | None]:
    """
    Deterministically validate one successful AI result.

    Returns:
        (is_valid, validation_error)
    """

    if row["category"] not in ALLOWED_CATEGORIES:
        return (
            False,
            "INVALID_CATEGORY",
        )

    if row["sentiment"] not in ALLOWED_SENTIMENTS:
        return (
            False,
            "INVALID_SENTIMENT",
        )

    if row["summary"] is None:
        return (
            False,
            "NULL_SUMMARY",
        )

    if not row["summary"].strip():
        return (
            False,
            "BLANK_SUMMARY",
        )

    if row["confidence"] is None:
        return (
            False,
            "NULL_CONFIDENCE",
        )

    if not 0.0 <= float(row["confidence"]) <= 1.0:
        return (
            False,
            "INVALID_CONFIDENCE",
        )

    return (
        True,
        None,
    )


def route_final_results(
    run_id: str | None = None,
) -> dict:
    """
    Build the final ticket-classification layer.

    Deterministic rules and Python validation decide
    final handling. The AI does not decide whether its
    own output is accepted.
    """

    initialize_database()

    connection = get_connection()

    try:
        if run_id is None:
            run_id = get_latest_run_id(
                connection
            )

        clean_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM ticket_clean;
            """
        ).fetchone()[0]

        if clean_count == 0:
            raise RuntimeError(
                "ticket_clean contains no records."
            )

        # -----------------------------------------------------
        # Rebuild derived final-routing tables transactionally
        # -----------------------------------------------------

        connection.execute(
            "DELETE FROM human_review_queue;"
        )

        connection.execute(
            "DELETE FROM final_ticket_classification;"
        )

        # -----------------------------------------------------
        # 1. Deterministic classifications
        # -----------------------------------------------------

        deterministic_rows = connection.execute(
            """
            SELECT
                dc.ticket_id,
                dc.category
            FROM deterministic_classification AS dc
            ORDER BY dc.ticket_id;
            """
        ).fetchall()

        for row in deterministic_rows:
            connection.execute(
                """
                INSERT INTO final_ticket_classification (
                    ticket_id,
                    final_category,
                    sentiment,
                    summary,
                    classification_source,
                    ai_used,
                    final_handling,
                    updated_at
                )
                VALUES (
                    ?,
                    ?,
                    NULL,
                    NULL,
                    'DETERMINISTIC',
                    0,
                    'DETERMINISTIC',
                    ?
                );
                """,
                (
                    row["ticket_id"],
                    row["category"],
                    utc_now(),
                ),
            )

        # -----------------------------------------------------
        # 2. Process every AI candidate
        # -----------------------------------------------------

        candidates = connection.execute(
            """
            SELECT
                ticket_id,
                evidence_text,
                input_hash
            FROM ai_candidate
            ORDER BY ticket_id;
            """
        ).fetchall()

        ai_accepted = 0
        human_review = 0
        ai_failure_fallback = 0
        invalid_ai_output = 0
        missing_ai_result = 0

        review_details = []

        for candidate in candidates:
            ai_row = connection.execute(
                """
                SELECT
                    ticket_id,
                    category,
                    sentiment,
                    summary,
                    confidence,
                    result_status,
                    error_type,
                    error_message
                FROM ai_classification
                WHERE ticket_id = ?
                  AND input_hash = ?
                  AND prompt_version = ?
                  AND model = ?
                LIMIT 1;
                """,
                (
                    candidate["ticket_id"],
                    candidate["input_hash"],
                    PROMPT_VERSION,
                    MODEL,
                ),
            ).fetchone()

            # -------------------------------------------------
            # Missing AI result
            # -------------------------------------------------

            if ai_row is None:
                missing_ai_result += 1
                ai_failure_fallback += 1

                connection.execute(
                    """
                    INSERT INTO final_ticket_classification (
                        ticket_id,
                        final_category,
                        sentiment,
                        summary,
                        classification_source,
                        ai_used,
                        final_handling,
                        updated_at
                    )
                    VALUES (
                        ?,
                        NULL,
                        NULL,
                        NULL,
                        'FALLBACK',
                        1,
                        'AI_FAILURE_FALLBACK',
                        ?
                    );
                    """,
                    (
                        candidate["ticket_id"],
                        utc_now(),
                    ),
                )

                continue

            # -------------------------------------------------
            # Explicit AI failure
            # -------------------------------------------------

            if ai_row["result_status"] != "SUCCESS":
                ai_failure_fallback += 1

                connection.execute(
                    """
                    INSERT INTO final_ticket_classification (
                        ticket_id,
                        final_category,
                        sentiment,
                        summary,
                        classification_source,
                        ai_used,
                        final_handling,
                        updated_at
                    )
                    VALUES (
                        ?,
                        NULL,
                        NULL,
                        NULL,
                        'FALLBACK',
                        1,
                        'AI_FAILURE_FALLBACK',
                        ?
                    );
                    """,
                    (
                        candidate["ticket_id"],
                        utc_now(),
                    ),
                )

                continue

            # -------------------------------------------------
            # Deterministic validation of successful AI output
            # -------------------------------------------------

            is_valid, validation_error = (
                validate_ai_result(ai_row)
            )

            if not is_valid:
                invalid_ai_output += 1
                ai_failure_fallback += 1

                connection.execute(
                    """
                    INSERT INTO final_ticket_classification (
                        ticket_id,
                        final_category,
                        sentiment,
                        summary,
                        classification_source,
                        ai_used,
                        final_handling,
                        updated_at
                    )
                    VALUES (
                        ?,
                        NULL,
                        NULL,
                        NULL,
                        'FALLBACK',
                        1,
                        'AI_FAILURE_FALLBACK',
                        ?
                    );
                    """,
                    (
                        candidate["ticket_id"],
                        utc_now(),
                    ),
                )

                continue

            # -------------------------------------------------
            # Human-review routing
            # -------------------------------------------------

            review_reason = None

            if ai_row["category"] == "UNRESOLVED":
                review_reason = (
                    "AI_UNRESOLVED_CATEGORY"
                )

            elif (
                float(ai_row["confidence"])
                < CONFIDENCE_THRESHOLD
            ):
                review_reason = (
                    "LOW_AI_CONFIDENCE"
                )

            if review_reason is not None:
                human_review += 1

                connection.execute(
                    """
                    INSERT INTO human_review_queue (
                        ticket_id,
                        review_reason,
                        proposed_category,
                        confidence,
                        review_status,
                        created_at
                    )
                    VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        'PENDING',
                        ?
                    );
                    """,
                    (
                        candidate["ticket_id"],
                        review_reason,
                        ai_row["category"],
                        ai_row["confidence"],
                        utc_now(),
                    ),
                )

                connection.execute(
                    """
                    INSERT INTO final_ticket_classification (
                        ticket_id,
                        final_category,
                        sentiment,
                        summary,
                        classification_source,
                        ai_used,
                        final_handling,
                        updated_at
                    )
                    VALUES (
                        ?,
                        NULL,
                        ?,
                        ?,
                        'HUMAN_REVIEW',
                        1,
                        'HUMAN_REVIEW',
                        ?
                    );
                    """,
                    (
                        candidate["ticket_id"],
                        ai_row["sentiment"],
                        ai_row["summary"],
                        utc_now(),
                    ),
                )

                review_details.append(
                    {
                        "ticket_id": (
                            candidate["ticket_id"]
                        ),
                        "reason": review_reason,
                        "confidence": (
                            ai_row["confidence"]
                        ),
                        "evidence": (
                            candidate["evidence_text"]
                        ),
                    }
                )

                continue

            # -------------------------------------------------
            # Accepted AI result
            # -------------------------------------------------

            ai_accepted += 1

            connection.execute(
                """
                INSERT INTO final_ticket_classification (
                    ticket_id,
                    final_category,
                    sentiment,
                    summary,
                    classification_source,
                    ai_used,
                    final_handling,
                    updated_at
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    ?,
                    'AI',
                    1,
                    'AI_ACCEPTED',
                    ?
                );
                """,
                (
                    candidate["ticket_id"],
                    ai_row["category"],
                    ai_row["sentiment"],
                    ai_row["summary"],
                    utc_now(),
                ),
            )

        # -----------------------------------------------------
        # Final integrity validation
        # -----------------------------------------------------

        final_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM final_ticket_classification;
            """
        ).fetchone()[0]

        duplicate_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT ticket_id
                FROM final_ticket_classification
                GROUP BY ticket_id
                HAVING COUNT(*) > 1
            );
            """
        ).fetchone()[0]

        orphan_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM final_ticket_classification AS f
            LEFT JOIN ticket_clean AS tc
                ON f.ticket_id = tc.ticket_id
            WHERE tc.ticket_id IS NULL;
            """
        ).fetchone()[0]

        if final_count != clean_count:
            raise RuntimeError(
                "Final routing row-count failure: "
                f"clean={clean_count}, "
                f"final={final_count}."
            )

        if duplicate_count != 0:
            raise RuntimeError(
                "Final routing contains duplicate ticket IDs."
            )

        if orphan_count != 0:
            raise RuntimeError(
                "Final routing contains orphan records."
            )

        # -----------------------------------------------------
        # Observability
        # -----------------------------------------------------

        metrics = {
            "final_rows": final_count,
            "deterministic_rows": len(
                deterministic_rows
            ),
            "ai_accepted_rows": ai_accepted,
            "human_review_rows": human_review,
            "ai_failure_fallback_rows": (
                ai_failure_fallback
            ),
            "invalid_ai_output_rows": (
                invalid_ai_output
            ),
            "missing_ai_result_rows": (
                missing_ai_result
            ),
            "duplicate_final_rows": (
                duplicate_count
            ),
            "orphan_final_rows": orphan_count,
        }

        for metric_name, metric_value in metrics.items():
            record_metric(
                connection,
                run_id,
                metric_name,
                metric_value,
            )

        connection.execute(
            """
            UPDATE pipeline_run
            SET human_review_count = ?
            WHERE run_id = ?;
            """,
            (
                human_review,
                run_id,
            ),
        )

        connection.commit()

        return {
            "run_id": run_id,
            "clean_rows": clean_count,
            **metrics,
            "review_details": review_details,
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def print_summary(summary: dict) -> None:
    """Print final routing results."""

    print("FINAL ROUTING SUMMARY")
    print("---------------------")

    print(
        f"Clean tickets:        "
        f"{summary['clean_rows']}"
    )

    print(
        f"Final tickets:        "
        f"{summary['final_rows']}"
    )

    print(
        f"Deterministic:        "
        f"{summary['deterministic_rows']}"
    )

    print(
        f"AI accepted:          "
        f"{summary['ai_accepted_rows']}"
    )

    print(
        f"Human review:         "
        f"{summary['human_review_rows']}"
    )

    print(
        f"AI failure fallback:  "
        f"{summary['ai_failure_fallback_rows']}"
    )

    print(
        f"Invalid AI outputs:   "
        f"{summary['invalid_ai_output_rows']}"
    )

    print(
        f"Missing AI results:   "
        f"{summary['missing_ai_result_rows']}"
    )

    print()

    if summary["review_details"]:
        print("Human review queue:")

        for item in summary[
            "review_details"
        ]:
            print(
                f"  - Ticket "
                f"{item['ticket_id']}: "
                f"{item['reason']} "
                f"(confidence="
                f"{item['confidence']:.2f})"
            )

            print(
                f"    Evidence: "
                f"{item['evidence']}"
            )

        print()

    print("FINAL ROUTING VALIDATION: PASS")


if __name__ == "__main__":
    result = route_final_results()

    print_summary(result)
