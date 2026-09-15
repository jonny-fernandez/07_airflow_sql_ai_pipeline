import hashlib
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import INCOMING_DIR
from src.database import get_connection, initialize_database
from src.source_validation import validate_source_file


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def generate_run_id() -> str:
    """Create a unique pipeline execution ID."""
    return f"RUN-{uuid.uuid4().hex[:12].upper()}"


def calculate_source_hash(
    ticket_id: str,
    created_date: str,
    ticket_text: str,
) -> str:
    """
    Create a deterministic fingerprint for a source record.

    If the same ticket arrives again with identical content,
    the hash will be identical.
    """
    canonical_value = "|".join(
        [
            str(ticket_id),
            str(created_date),
            str(ticket_text),
        ]
    )

    return hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()


def create_pipeline_run(
    connection: sqlite3.Connection,
    run_id: str,
    source_file: Path,
) -> None:
    """
    Register the start of a pipeline execution.

    Reusing the same run_id during an Airflow retry does
    not create a second pipeline-run record.
    """

    connection.execute(
        """
        INSERT INTO pipeline_run (
            run_id,
            source_file,
            started_at,
            status
        )
        VALUES (?, ?, ?, 'RUNNING')

        ON CONFLICT(run_id)
        DO UPDATE SET
            source_file = excluded.source_file,
            completed_at = NULL,
            status = 'RUNNING',
            source_row_count = 0,
            clean_row_count = 0,
            quarantined_row_count = 0,
            deterministic_count = 0,
            ai_candidate_count = 0,
            ai_success_count = 0,
            ai_failure_count = 0,
            human_review_count = 0;
        """,
        (
            run_id,
            source_file.name,
            utc_now(),
        ),
    )

    connection.commit()

def record_metric(
    connection: sqlite3.Connection,
    run_id: str,
    metric_name: str,
    metric_value: float,
) -> None:
    """Write one ingestion metric idempotently."""

    connection.execute(
        """
        INSERT INTO task_run_metrics (
            run_id,
            task_name,
            metric_name,
            metric_value,
            recorded_at
        )
        VALUES (?, 'ingest_source', ?, ?, ?)
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


def log_pipeline_failure(
    connection: sqlite3.Connection,
    run_id: str,
    error: Exception,
) -> None:
    """Persist a true pipeline failure."""

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
            "ingest_source",
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


def normalize_issue_ticket_id(value):
    """
    Convert a quality-issue ticket ID to integer when possible.

    Invalid or blank IDs remain NULL in the issue table.
    """
    text = str(value).strip()

    if text.isdigit():
        return int(text)

    return None


def ingest_ticket_file(
    source_file: Path,
    run_id: str | None = None,
) -> dict:
    """
    Validate and idempotently load a ticket CSV into SQLite.

    Business records are not duplicated across reruns.
    Pipeline execution history is retained separately.
    """

    initialize_database()

    if run_id is None:
        run_id = generate_run_id()
    connection = get_connection()

    create_pipeline_run(
        connection,
        run_id,
        source_file,
    )

    try:
        # -----------------------------------------------------
        # Validate source before loading
        # -----------------------------------------------------

        validation = validate_source_file(source_file)

        # Read source as text after validation.
        df = pd.read_csv(
            source_file,
            dtype=str,
            keep_default_na=False,
        )

        df["source_row_number"] = range(
            2,
            len(df) + 2,
        )

        # -----------------------------------------------------
        # Persist row-level quality exceptions
        # -----------------------------------------------------

        invalid_row_numbers = set()

        if not validation.issues.empty:
            invalid_row_numbers = set(
                validation.issues[
                    "source_row_number"
                ].tolist()
            )

            for _, issue in validation.issues.iterrows():
                connection.execute(
                    """
                    INSERT INTO ticket_quality_issues (
                        run_id,
                        ticket_id,
                        issue_code,
                        issue_detail,
                        source_file,
                        source_row_number,
                        detected_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        run_id,
                        normalize_issue_ticket_id(
                            issue["ticket_id"]
                        ),
                        issue["issue_code"],
                        issue["issue_detail"],
                        source_file.name,
                        int(issue["source_row_number"]),
                        utc_now(),
                    ),
                )

        # -----------------------------------------------------
        # Keep only records that passed row-level validation
        # -----------------------------------------------------

        valid_df = df[
            ~df["source_row_number"].isin(
                invalid_row_numbers
            )
        ].copy()

        # -----------------------------------------------------
        # Compare incoming records with existing staging data
        # -----------------------------------------------------

        existing_rows = connection.execute(
            """
            SELECT
                ticket_id,
                source_hash
            FROM stg_raw_tickets;
            """
        ).fetchall()

        existing_hashes = {
            row["ticket_id"]: row["source_hash"]
            for row in existing_rows
        }

        inserted_rows = 0
        updated_rows = 0
        unchanged_rows = 0

        for _, row in valid_df.iterrows():
            ticket_id = int(
                row["ticket_id"].strip()
            )

            created_date = (
                row["created_date"].strip()
            )

            ticket_text = (
                row["ticket_text"].strip()
            )

            source_hash = calculate_source_hash(
                row["ticket_id"],
                row["created_date"],
                row["ticket_text"],
            )

            existing_hash = existing_hashes.get(
                ticket_id
            )

            if existing_hash is None:
                inserted_rows += 1

            elif existing_hash == source_hash:
                unchanged_rows += 1
                continue

            else:
                updated_rows += 1

            connection.execute(
                """
                INSERT INTO stg_raw_tickets (
                    ticket_id,
                    created_date,
                    ticket_text,
                    source_file,
                    source_row_number,
                    source_hash,
                    loaded_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)

                ON CONFLICT(ticket_id)
                DO UPDATE SET
                    created_date =
                        excluded.created_date,
                    ticket_text =
                        excluded.ticket_text,
                    source_file =
                        excluded.source_file,
                    source_row_number =
                        excluded.source_row_number,
                    source_hash =
                        excluded.source_hash,
                    loaded_at =
                        excluded.loaded_at;
                """,
                (
                    ticket_id,
                    created_date,
                    ticket_text,
                    source_file.name,
                    int(row["source_row_number"]),
                    source_hash,
                    utc_now(),
                ),
            )

        # -----------------------------------------------------
        # Record metrics
        # -----------------------------------------------------

        metrics = {
            "source_rows": validation.total_rows,
            "valid_rows": validation.valid_rows,
            "invalid_rows": validation.invalid_rows,
            "inserted_rows": inserted_rows,
            "updated_rows": updated_rows,
            "unchanged_rows": unchanged_rows,
        }

        for metric_name, metric_value in metrics.items():
            record_metric(
                connection,
                run_id,
                metric_name,
                metric_value,
            )

        # -----------------------------------------------------
        # Complete this ingestion execution
        # -----------------------------------------------------

        # Ingestion is only one stage of the full pipeline.
        # The overall pipeline_run remains RUNNING until the
        # end-to-end orchestrator marks it complete.

        connection.execute(
            """
            UPDATE pipeline_run
            SET
                source_row_count = ?,
                quarantined_row_count = ?
            WHERE run_id = ?;
            """,
            (
                validation.total_rows,
                validation.invalid_rows,
                run_id,
            ),
        )

        status = "RUNNING"



        connection.commit()

        staging_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM stg_raw_tickets;
            """
        ).fetchone()[0]

        return {
            "run_id": run_id,
            "status": status,
            **metrics,
            "staging_total": staging_count,
        }

    except Exception as error:
        log_pipeline_failure(
            connection,
            run_id,
            error,
        )
        raise

    finally:
        connection.close()


def print_ingestion_summary(summary: dict) -> None:
    """Print metrics suitable for local testing and Airflow logs."""

    print(f"Run ID:          {summary['run_id']}")
    print(f"Status:          {summary['status']}")
    print(f"Source rows:     {summary['source_rows']}")
    print(f"Valid rows:      {summary['valid_rows']}")
    print(f"Invalid rows:    {summary['invalid_rows']}")
    print(f"Inserted rows:   {summary['inserted_rows']}")
    print(f"Updated rows:    {summary['updated_rows']}")
    print(f"Unchanged rows:  {summary['unchanged_rows']}")
    print(f"Staging total:   {summary['staging_total']}")


if __name__ == "__main__":
    source_file = (
        INCOMING_DIR
        / "tickets_batch_001.csv"
    )

    result = ingest_ticket_file(source_file)

    print_ingestion_summary(result)
