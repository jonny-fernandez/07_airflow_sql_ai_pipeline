from src.database import get_connection


def inspect_latest_run() -> None:
    """
    Display the latest pipeline execution and all task-level
    metrics recorded for that execution.
    """

    connection = get_connection()

    try:
        run = connection.execute(
            """
            SELECT *
            FROM pipeline_run
            ORDER BY started_at DESC
            LIMIT 1;
            """
        ).fetchone()

        if run is None:
            raise RuntimeError(
                "No pipeline executions were found."
            )

        run_id = run["run_id"]

        print("LATEST PIPELINE RUN")
        print("-------------------")

        print(f"Run ID:              {run_id}")
        print(f"Status:              {run['status']}")
        print(f"Started:             {run['started_at']}")
        print(f"Completed:           {run['completed_at']}")
        print(f"Source rows:         {run['source_row_count']}")
        print(f"Clean rows:          {run['clean_row_count']}")
        print(f"Quarantined rows:    {run['quarantined_row_count']}")
        print(f"Deterministic:       {run['deterministic_count']}")
        print(f"AI candidates:       {run['ai_candidate_count']}")
        print(f"AI successes:        {run['ai_success_count']}")
        print(f"AI failures:         {run['ai_failure_count']}")
        print(f"Human review:        {run['human_review_count']}")

        print()
        print("TASK METRICS")
        print("------------")

        metrics = connection.execute(
            """
            SELECT
                task_name,
                metric_name,
                metric_value
            FROM task_run_metrics
            WHERE run_id = ?
            ORDER BY
                task_name,
                metric_name;
            """,
            (run_id,),
        ).fetchall()

        current_task = None

        for row in metrics:
            if row["task_name"] != current_task:
                current_task = row["task_name"]

                print()
                print(f"[{current_task}]")

            print(
                f"  {row['metric_name']}: "
                f"{row['metric_value']}"
            )

        print()
        print(
            f"Metric rows: {len(metrics)}"
        )

    finally:
        connection.close()


if __name__ == "__main__":
    inspect_latest_run()
