from datetime import datetime, timezone

from src.config import SQL_DIR
from src.database import get_connection, initialize_database


DETERMINISTIC_SQL_PATH = (
    SQL_DIR / "classify_deterministic.sql"
)

AI_CANDIDATE_SQL_PATH = (
    SQL_DIR / "identify_ai_candidates.sql"
)


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def get_latest_run_id(connection) -> str:
    """Return the most recent pipeline execution ID."""

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
            "No pipeline run exists. "
            "Run ingestion first."
        )

    return row["run_id"]


def record_metric(
    connection,
    run_id: str,
    metric_name: str,
    metric_value: float,
) -> None:
    """Persist routing metrics."""

    connection.execute(
        """
        INSERT INTO task_run_metrics (
            run_id,
            task_name,
            metric_name,
            metric_value,
            recorded_at
        )
        VALUES (?, 'route_tickets', ?, ?, ?)

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


def route_tickets(
    run_id: str | None = None,
) -> dict:
    """
    Apply deterministic classification first.

    Only unmatched tickets are routed to ai_candidate.

    AI is not called by this function.
    """

    initialize_database()

    if not DETERMINISTIC_SQL_PATH.exists():
        raise FileNotFoundError(
            f"Missing SQL file: "
            f"{DETERMINISTIC_SQL_PATH}"
        )

    if not AI_CANDIDATE_SQL_PATH.exists():
        raise FileNotFoundError(
            f"Missing SQL file: "
            f"{AI_CANDIDATE_SQL_PATH}"
        )

    deterministic_sql = (
        DETERMINISTIC_SQL_PATH.read_text(
            encoding="utf-8-sig"
        )
    )

    ai_candidate_sql = (
        AI_CANDIDATE_SQL_PATH.read_text(
            encoding="utf-8-sig"
        )
    )

    connection = get_connection()

    try:
        if run_id is None:
            run_id = get_latest_run_id(connection)

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
        # Rebuild derived routing tables
        #
        # The DELETE and INSERT operations are kept inside the
        # same transaction. If a later validation fails, the
        # transaction can be rolled back.
        # -----------------------------------------------------

        connection.execute(
            "DELETE FROM ai_candidate;"
        )

        connection.execute(
            "DELETE FROM deterministic_classification;"
        )

        # -----------------------------------------------------
        # Deterministic classification
        # -----------------------------------------------------

        connection.execute(
            deterministic_sql,
            {
                "classified_at": utc_now(),
            },
        )

        # -----------------------------------------------------
        # AI candidate identification
        # -----------------------------------------------------

        connection.execute(
            ai_candidate_sql,
            {
                "created_at": utc_now(),
            },
        )

        # -----------------------------------------------------
        # Validate routing integrity
        # -----------------------------------------------------

        deterministic_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM deterministic_classification;
            """
        ).fetchone()[0]

        ai_candidate_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM ai_candidate;
            """
        ).fetchone()[0]

        overlap_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM deterministic_classification AS dc
            INNER JOIN ai_candidate AS ac
                ON dc.ticket_id = ac.ticket_id;
            """
        ).fetchone()[0]

        routed_total = (
            deterministic_count
            + ai_candidate_count
        )

        unrouted_count = (
            clean_count - routed_total
        )

        if overlap_count != 0:
            raise RuntimeError(
                "Routing integrity failure: "
                f"{overlap_count} tickets appear in both "
                "deterministic and AI candidate tables."
            )

        if routed_total != clean_count:
            raise RuntimeError(
                "Routing integrity failure: "
                f"clean={clean_count}, "
                f"routed={routed_total}."
            )

        deterministic_pct = (
            deterministic_count
            / clean_count
            * 100
        )

        ai_candidate_pct = (
            ai_candidate_count
            / clean_count
            * 100
        )

        metrics = {
            "clean_rows": clean_count,
            "deterministic_rows": (
                deterministic_count
            ),
            "ai_candidate_rows": (
                ai_candidate_count
            ),
            "overlap_rows": overlap_count,
            "unrouted_rows": unrouted_count,
            "deterministic_pct": (
                deterministic_pct
            ),
            "ai_candidate_pct": (
                ai_candidate_pct
            ),
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
            SET
                deterministic_count = ?,
                ai_candidate_count = ?
            WHERE run_id = ?;
            """,
            (
                deterministic_count,
                ai_candidate_count,
                run_id,
            ),
        )

        connection.commit()

        # -----------------------------------------------------
        # Category breakdown
        # -----------------------------------------------------

        category_rows = connection.execute(
            """
            SELECT
                category,
                COUNT(*) AS ticket_count
            FROM deterministic_classification
            GROUP BY category
            ORDER BY category;
            """
        ).fetchall()

        candidate_rows = connection.execute(
            """
            SELECT
                ticket_id,
                evidence_text
            FROM ai_candidate
            ORDER BY ticket_id;
            """
        ).fetchall()

        return {
            "run_id": run_id,
            **metrics,
            "categories": [
                {
                    "category": row["category"],
                    "ticket_count": row["ticket_count"],
                }
                for row in category_rows
            ],
            "candidates": [
                {
                    "ticket_id": row["ticket_id"],
                    "evidence_text": row["evidence_text"],
                }
                for row in candidate_rows
            ],
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def print_routing_summary(
    summary: dict,
) -> None:
    """Print routing metrics and candidate evidence."""

    print(f"Run ID:                 {summary['run_id']}")
    print(
        f"Clean tickets:           "
        f"{summary['clean_rows']}"
    )
    print(
        f"Deterministic:           "
        f"{summary['deterministic_rows']} "
        f"({summary['deterministic_pct']:.1f}%)"
    )
    print(
        f"AI candidates:           "
        f"{summary['ai_candidate_rows']} "
        f"({summary['ai_candidate_pct']:.1f}%)"
    )
    print(
        f"Routing overlap:         "
        f"{summary['overlap_rows']}"
    )
    print(
        f"Unrouted tickets:        "
        f"{summary['unrouted_rows']}"
    )

    print()
    print("Deterministic category breakdown:")

    for item in summary["categories"]:
        print(
            f"  - {item['category']}: "
            f"{item['ticket_count']}"
        )

    print()
    print("AI candidates:")

    for item in summary["candidates"]:
        print(
            f"  - Ticket {item['ticket_id']}: "
            f"{item['evidence_text']}"
        )

    print()
    print("ROUTING VALIDATION: PASS")


if __name__ == "__main__":
    result = route_tickets()

    print_routing_summary(result)
