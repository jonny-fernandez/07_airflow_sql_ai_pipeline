from datetime import datetime, timezone

from src.config import SQL_DIR
from src.database import (
    get_connection,
    initialize_database,
)


SUMMARY_SQL_PATH = (
    SQL_DIR / "build_daily_summary.sql"
)


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""

    return datetime.now(
        timezone.utc
    ).isoformat()


def get_latest_run_id(connection) -> str:
    """Return the latest pipeline execution ID."""

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
    """Persist summary-stage metrics."""

    connection.execute(
        """
        INSERT INTO task_run_metrics (
            run_id,
            task_name,
            metric_name,
            metric_value,
            recorded_at
        )
        VALUES (?, 'build_summaries', ?, ?, ?)

        ON CONFLICT (
            run_id,
            task_name,
            metric_name
        )
        DO UPDATE SET
            metric_value =
                excluded.metric_value,
            recorded_at =
                excluded.recorded_at;
        """,
        (
            run_id,
            metric_name,
            metric_value,
            utc_now(),
        ),
    )


def build_daily_summary(
    run_id: str | None = None,
) -> dict:
    """
    Build deterministic analytical summary tables.

    SQL performs all aggregation.
    AI is not used for counting or metrics.
    """

    initialize_database()

    if not SUMMARY_SQL_PATH.exists():
        raise FileNotFoundError(
            f"Summary SQL not found: "
            f"{SUMMARY_SQL_PATH}"
        )

    summary_sql = (
        SUMMARY_SQL_PATH.read_text(
            encoding="utf-8-sig"
        )
    )

    connection = get_connection()

    try:
        if run_id is None:
            run_id = get_latest_run_id(
                connection
            )

        final_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM final_ticket_classification;
            """
        ).fetchone()[0]

        if final_count == 0:
            raise RuntimeError(
                "No final classifications exist."
            )

        # -----------------------------------------------------
        # Rebuild analytical output
        # -----------------------------------------------------

        connection.execute(
            "DELETE FROM daily_ticket_summary;"
        )

        connection.execute(
            summary_sql,
            {
                "refreshed_at": utc_now(),
            },
        )

        # -----------------------------------------------------
        # Reconciliation
        # -----------------------------------------------------

        summarized_ticket_count = (
            connection.execute(
                """
                SELECT
                    COALESCE(
                        SUM(ticket_count),
                        0
                    )
                FROM daily_ticket_summary;
                """
            ).fetchone()[0]
        )

        deterministic_count = (
            connection.execute(
                """
                SELECT COALESCE(
                    SUM(deterministic_count),
                    0
                )
                FROM daily_ticket_summary;
                """
            ).fetchone()[0]
        )

        ai_enriched_count = (
            connection.execute(
                """
                SELECT COALESCE(
                    SUM(ai_enriched_count),
                    0
                )
                FROM daily_ticket_summary;
                """
            ).fetchone()[0]
        )

        human_review_count = (
            connection.execute(
                """
                SELECT COALESCE(
                    SUM(human_review_count),
                    0
                )
                FROM daily_ticket_summary;
                """
            ).fetchone()[0]
        )

        ai_failure_fallback_count = (
            connection.execute(
                """
                SELECT COALESCE(
                    SUM(ai_failure_fallback_count),
                    0
                )
                FROM daily_ticket_summary;
                """
            ).fetchone()[0]
        )

        summary_row_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM daily_ticket_summary;
                """
            ).fetchone()[0]
        )

        if summarized_ticket_count != final_count:
            raise RuntimeError(
                "Summary reconciliation failed: "
                f"final={final_count}, "
                f"summarized="
                f"{summarized_ticket_count}."
            )

        handling_total = (
            deterministic_count
            + ai_enriched_count
            + human_review_count
            + ai_failure_fallback_count
        )

        if handling_total != final_count:
            raise RuntimeError(
                "Handling reconciliation failed: "
                f"final={final_count}, "
                f"handling_total="
                f"{handling_total}."
            )

        # -----------------------------------------------------
        # Category-level analytical view
        # -----------------------------------------------------

        category_rows = connection.execute(
            """
            SELECT
                category,
                SUM(ticket_count)
                    AS ticket_count,

                SUM(deterministic_count)
                    AS deterministic_count,

                SUM(ai_enriched_count)
                    AS ai_enriched_count,

                SUM(human_review_count)
                    AS human_review_count,

                SUM(ai_failure_fallback_count)
                    AS ai_failure_fallback_count

            FROM daily_ticket_summary

            GROUP BY category
            ORDER BY category;
            """
        ).fetchall()

        metrics = {
            "summary_rows":
                summary_row_count,

            "summarized_tickets":
                summarized_ticket_count,

            "deterministic_tickets":
                deterministic_count,

            "ai_enriched_tickets":
                ai_enriched_count,

            "human_review_tickets":
                human_review_count,

            "ai_failure_fallback_tickets":
                ai_failure_fallback_count,

            "row_count_match":
                int(
                    summarized_ticket_count
                    == final_count
                ),
        }

        for metric_name, metric_value in (
            metrics.items()
        ):
            record_metric(
                connection,
                run_id,
                metric_name,
                metric_value,
            )

        connection.commit()

        return {
            "run_id": run_id,
            "final_rows": final_count,
            **metrics,
            "categories": [
                dict(row)
                for row in category_rows
            ],
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def print_summary(result: dict) -> None:
    """Print analytical reconciliation results."""

    print("ANALYTICAL SUMMARY")
    print("------------------")

    print(
        f"Final tickets:          "
        f"{result['final_rows']}"
    )

    print(
        f"Summarized tickets:     "
        f"{result['summarized_tickets']}"
    )

    print(
        f"Deterministic tickets:  "
        f"{result['deterministic_tickets']}"
    )

    print(
        f"AI-enriched tickets:    "
        f"{result['ai_enriched_tickets']}"
    )

    print(
        f"Human-review tickets:   "
        f"{result['human_review_tickets']}"
    )

    print(
        f"AI-fallback tickets:    "
        f"{result['ai_failure_fallback_tickets']}"
    )

    print(
        f"Summary table rows:      "
        f"{result['summary_rows']}"
    )

    print(
        f"Row-count match:         "
        f"{bool(result['row_count_match'])}"
    )

    print()
    print("Category totals:")

    for row in result["categories"]:
        print(
            f"  - {row['category']}: "
            f"{row['ticket_count']} total | "
            f"{row['deterministic_count']} deterministic | "
            f"{row['ai_enriched_count']} AI | "
            f"{row['human_review_count']} review | "
            f"{row['ai_failure_fallback_count']} fallback"
        )

    print()
    print("SUMMARY VALIDATION: PASS")


if __name__ == "__main__":
    result = build_daily_summary()

    print_summary(result)
