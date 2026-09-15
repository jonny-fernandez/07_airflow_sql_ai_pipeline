from src.ai_enrichment import (
    enrich_one_ticket,
)
from src.database import (
    get_connection,
    initialize_database,
)
from src.ingestion import utc_now


TASK_NAME = "ai_enrichment"


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


def get_candidate_ids() -> list[int]:
    """Return only tickets selected for AI enrichment."""

    connection = get_connection()

    try:
        rows = connection.execute(
            """
            SELECT ticket_id
            FROM ai_candidate
            ORDER BY ticket_id;
            """
        ).fetchall()

        return [
            row["ticket_id"]
            for row in rows
        ]

    finally:
        connection.close()


def record_metric(
    connection,
    run_id: str,
    metric_name: str,
    metric_value: float,
) -> None:
    """Persist AI batch observability metrics."""

    connection.execute(
        """
        INSERT INTO task_run_metrics (
            run_id,
            task_name,
            metric_name,
            metric_value,
            recorded_at
        )
        VALUES (?, ?, ?, ?, ?)

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
            TASK_NAME,
            metric_name,
            metric_value,
            utc_now(),
        ),
    )


def enrich_candidate_batch(
    run_id: str | None = None,
) -> dict:
    """
    Process every ticket already routed to ai_candidate.

    Individual AI failures are captured per record and do not
    automatically fail deterministic processing for other records.
    """

    initialize_database()

    candidate_ids = get_candidate_ids()

    if not candidate_ids:
        raise RuntimeError(
            "No AI candidates were found."
        )

    connection = get_connection()

    try:
        if run_id is None:
            run_id = get_latest_run_id(
                connection
            )

    finally:
        connection.close()

    api_calls = 0
    reused = 0
    succeeded = 0
    failed = 0

    results = []
    failures = []

    for ticket_id in candidate_ids:
        try:
            result = enrich_one_ticket(
                ticket_id
            )

            execution = result[
                "execution"
            ]

            if execution == "API_CALL":
                api_calls += 1

            elif execution == "REUSED":
                reused += 1

            succeeded += 1

            results.append(
                {
                    "ticket_id": ticket_id,
                    "execution": execution,
                    "category": result[
                        "category"
                    ],
                    "confidence": result[
                        "confidence"
                    ],
                }
            )

            print(
                f"[{execution}] "
                f"Ticket {ticket_id}: "
                f"{result['category']} "
                f"({result['confidence']:.2f})"
            )

        except Exception as error:
            failed += 1

            failures.append(
                {
                    "ticket_id": ticket_id,
                    "error_type": (
                        type(error).__name__
                    ),
                    "error_message": str(error),
                }
            )

            print(
                f"[FAILED] "
                f"Ticket {ticket_id}: "
                f"{type(error).__name__}: "
                f"{error}"
            )

    # ---------------------------------------------------------
    # Record batch-level observability
    # ---------------------------------------------------------

    connection = get_connection()

    try:
        metrics = {
            "candidate_rows": len(
                candidate_ids
            ),
            "successful_rows": succeeded,
            "failed_rows": failed,
            "api_calls": api_calls,
            "reused_results": reused,
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

        connection.execute(
            """
            UPDATE pipeline_run
            SET
                ai_success_count = ?,
                ai_failure_count = ?
            WHERE run_id = ?;
            """,
            (
                succeeded,
                failed,
                run_id,
            ),
        )

        connection.commit()

    finally:
        connection.close()

    return {
        "run_id": run_id,
        "candidate_rows": len(
            candidate_ids
        ),
        "successful_rows": succeeded,
        "failed_rows": failed,
        "api_calls": api_calls,
        "reused_results": reused,
        "results": results,
        "failures": failures,
    }


def print_batch_summary(
    summary: dict,
) -> None:
    """Print compact batch metrics."""

    print()
    print("AI ENRICHMENT BATCH SUMMARY")
    print("---------------------------")

    print(
        f"Run ID:           "
        f"{summary['run_id']}"
    )

    print(
        f"AI candidates:    "
        f"{summary['candidate_rows']}"
    )

    print(
        f"Successful:       "
        f"{summary['successful_rows']}"
    )

    print(
        f"Failed:           "
        f"{summary['failed_rows']}"
    )

    print(
        f"API calls:        "
        f"{summary['api_calls']}"
    )

    print(
        f"Reused results:   "
        f"{summary['reused_results']}"
    )


if __name__ == "__main__":
    result = enrich_candidate_batch()

    print_batch_summary(result)
