from datetime import datetime, timezone

from src.database import get_connection


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""

    return datetime.now(
        timezone.utc
    ).isoformat()


def finalize_pipeline_run(
    run_id: str,
) -> dict:
    """
    Mark a successfully completed pipeline execution.

    A run containing quarantined data-quality exceptions
    is successful operationally, but receives a distinct
    completion status.
    """

    connection = get_connection()

    try:
        row = connection.execute(
            """
            SELECT
                run_id,
                status,
                quarantined_row_count
            FROM pipeline_run
            WHERE run_id = ?;
            """,
            (run_id,),
        ).fetchone()

        if row is None:
            raise ValueError(
                f"Pipeline run not found: {run_id}"
            )

        if row["status"] == "FAILED":
            raise RuntimeError(
                "Cannot finalize a pipeline run "
                "already marked FAILED."
            )

        quarantined_rows = (
            row["quarantined_row_count"] or 0
        )

        final_status = (
            "COMPLETED_WITH_EXCEPTIONS"
            if quarantined_rows > 0
            else "SUCCESS"
        )

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
                final_status,
                run_id,
            ),
        )

        connection.commit()

        return {
            "run_id": run_id,
            "status": final_status,
            "quarantined_rows": quarantined_rows,
        }

    finally:
        connection.close()


def mark_pipeline_failed(
    run_id: str,
    task_name: str,
    error_type: str,
    error_message: str,
) -> dict:
    """
    Mark the custom pipeline execution FAILED when an
    Airflow task reaches a terminal failure state.

    The error is also retained in pipeline_errors.
    """

    connection = get_connection()

    try:
        row = connection.execute(
            """
            SELECT run_id
            FROM pipeline_run
            WHERE run_id = ?;
            """,
            (run_id,),
        ).fetchone()

        # A failure can theoretically occur before ingestion
        # has created the custom pipeline-run record.
        if row is None:
            return {
                "run_id": run_id,
                "updated": False,
            }

        existing_error = connection.execute(
            """
            SELECT error_id
            FROM pipeline_errors
            WHERE run_id = ?
              AND task_name = ?
              AND error_type = ?
              AND error_message = ?
            LIMIT 1;
            """,
            (
                run_id,
                task_name,
                error_type,
                error_message,
            ),
        ).fetchone()

        if existing_error is None:
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
                    task_name,
                    error_type,
                    error_message,
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

        return {
            "run_id": run_id,
            "updated": True,
        }

    finally:
        connection.close()