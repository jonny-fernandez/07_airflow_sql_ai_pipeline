from datetime import datetime, timezone

from src.config import SQL_DIR
from src.database import get_connection, initialize_database


TRANSFORM_SQL_PATH = (
    SQL_DIR / "transform_ticket_clean.sql"
)


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def get_latest_run_id(connection) -> str:
    """
    Return the most recent pipeline run.

    During later Airflow integration, the run_id will be passed
    explicitly between tasks. This fallback supports local testing.
    """
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
            "No pipeline_run record exists. "
            "Run ingestion before transformations."
        )

    return row["run_id"]


def record_metric(
    connection,
    run_id: str,
    metric_name: str,
    metric_value: float,
) -> None:
    """Persist a transformation metric idempotently."""

    connection.execute(
        """
        INSERT INTO task_run_metrics (
            run_id,
            task_name,
            metric_name,
            metric_value,
            recorded_at
        )
        VALUES (?, 'transform_tickets', ?, ?, ?)

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


def transform_tickets(
    run_id: str | None = None,
) -> dict:
    """
    Transform staging tickets into the clean business layer.

    SQL performs the transformation.
    Python performs orchestration and validation.
    AI is not involved.
    """

    initialize_database()

    if not TRANSFORM_SQL_PATH.exists():
        raise FileNotFoundError(
            f"Transformation SQL not found: "
            f"{TRANSFORM_SQL_PATH}"
        )

    transform_sql = TRANSFORM_SQL_PATH.read_text(
        encoding="utf-8-sig"
    )

    connection = get_connection()

    try:
        if run_id is None:
            run_id = get_latest_run_id(connection)

        # -----------------------------------------------------
        # Pre-transformation validation
        # -----------------------------------------------------

        staging_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM stg_raw_tickets;
            """
        ).fetchone()[0]

        if staging_count == 0:
            raise RuntimeError(
                "stg_raw_tickets contains no records."
            )

        # -----------------------------------------------------
        # Execute deterministic SQL transformation
        # -----------------------------------------------------

        connection.execute(
            transform_sql,
            {
                "cleaned_at": utc_now(),
            },
        )

        # -----------------------------------------------------
        # Validate transformed output
        # -----------------------------------------------------

        clean_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM ticket_clean;
            """
        ).fetchone()[0]

        duplicate_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT ticket_id
                FROM ticket_clean
                GROUP BY ticket_id
                HAVING COUNT(*) > 1
            );
            """
        ).fetchone()[0]

        null_normalized_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM ticket_clean
            WHERE normalized_text IS NULL
               OR TRIM(normalized_text) = '';
            """
        ).fetchone()[0]

        row_count_match = (
            staging_count == clean_count
        )

        # -----------------------------------------------------
        # Pipeline-integrity checks
        # -----------------------------------------------------

        if duplicate_count != 0:
            raise RuntimeError(
                "Transformation validation failed: "
                f"{duplicate_count} duplicate ticket IDs "
                "found in ticket_clean."
            )

        if null_normalized_count != 0:
            raise RuntimeError(
                "Transformation validation failed: "
                f"{null_normalized_count} blank normalized "
                "ticket records found."
            )

        if not row_count_match:
            raise RuntimeError(
                "Transformation row-count validation failed: "
                f"staging={staging_count}, "
                f"clean={clean_count}."
            )

        # -----------------------------------------------------
        # Observability metrics
        # -----------------------------------------------------

        metrics = {
            "staging_rows": staging_count,
            "clean_rows": clean_count,
            "duplicate_rows": duplicate_count,
            "blank_normalized_rows": (
                null_normalized_count
            ),
            "row_count_match": int(
                row_count_match
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
            SET clean_row_count = ?
            WHERE run_id = ?;
            """,
            (
                clean_count,
                run_id,
            ),
        )

        connection.commit()

        return {
            "run_id": run_id,
            **metrics,
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def print_transformation_summary(
    summary: dict,
) -> None:
    """Print transformation and integrity metrics."""

    print(f"Run ID:                 {summary['run_id']}")
    print(
        f"Staging rows:           "
        f"{summary['staging_rows']}"
    )
    print(
        f"Clean rows:             "
        f"{summary['clean_rows']}"
    )
    print(
        f"Duplicate rows:         "
        f"{summary['duplicate_rows']}"
    )
    print(
        f"Blank normalized rows:  "
        f"{summary['blank_normalized_rows']}"
    )
    print(
        f"Row-count match:        "
        f"{bool(summary['row_count_match'])}"
    )

    print()
    print("TRANSFORMATION VALIDATION: PASS")


if __name__ == "__main__":
    result = transform_tickets()

    print_transformation_summary(result)
