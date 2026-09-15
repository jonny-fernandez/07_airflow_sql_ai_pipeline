from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from airflow.sdk import (
    dag,
    get_current_context,
    task,
)

from src.batch_ai_enrichment import enrich_candidate_batch
from src.config import INCOMING_DIR
from src.evaluation import evaluate_pipeline
from src.final_routing import route_final_results
from src.ingestion import ingest_ticket_file
from src.routing import route_tickets
from src.run_status import (
    finalize_pipeline_run,
    mark_pipeline_failed,
)
from src.summaries import build_daily_summary
from src.transformations import transform_tickets


SOURCE_FILE = (
    INCOMING_DIR
    / "tickets_batch_001.csv"
)


def handle_task_failure(context) -> None:
    """
    Synchronize a terminal Airflow task failure with the
    project's own pipeline execution metadata.
    """

    task_instance = context["ti"]

    pipeline_run_id = task_instance.run_id
    task_name = task_instance.task_id

    exception = context.get(
        "exception"
    )

    if exception is None:
        error_type = "AirflowTaskFailure"
        error_message = (
            "Airflow task failed without an exception "
            "object in callback context."
        )
    else:
        error_type = type(
            exception
        ).__name__

        error_message = str(
            exception
        )

    result = mark_pipeline_failed(
        run_id=pipeline_run_id,
        task_name=task_name,
        error_type=error_type,
        error_message=error_message,
    )

    print(
        f"failure_pipeline_run_id="
        f"{pipeline_run_id}"
    )

    print(
        f"failure_task="
        f"{task_name}"
    )

    print(
        f"pipeline_failure_recorded="
        f"{result['updated']}"
    )


@dag(
    dag_id="sql_ai_ticket_pipeline",
    description=(
        "Deterministic-first SQL and AI ticket pipeline "
        "with selective semantic enrichment."
    ),
    start_date=datetime(
        2026,
        1,
        1,
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,

    default_args={
        "on_failure_callback":
            handle_task_failure,
    },

    tags=[
        "portfolio",
        "sql",
        "ai",
        "data-quality",
        "idempotent",
    ],
)
def sql_ai_ticket_pipeline():

    @task(
        task_id="ingest_source",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def ingest_source() -> str:
        """
        Validate and idempotently ingest the source data.
        """

        context = get_current_context()

        airflow_run_id = (
            context["ti"].run_id
        )

        result = ingest_ticket_file(
            SOURCE_FILE,
            run_id=airflow_run_id,
        )

        print(
            f"run_id={result['run_id']}"
        )
        print(
            f"source_rows={result['source_rows']}"
        )
        print(
            f"inserted_rows={result['inserted_rows']}"
        )
        print(
            f"updated_rows={result['updated_rows']}"
        )
        print(
            f"unchanged_rows={result['unchanged_rows']}"
        )
        print(
            f"invalid_rows={result['invalid_rows']}"
        )

        return result["run_id"]


    @task(
        task_id="transform_tickets",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def transform(
        pipeline_run_id: str,
    ) -> str:

        result = transform_tickets(
            run_id=pipeline_run_id
        )

        print(
            f"clean_rows="
            f"{result['clean_rows']}"
        )

        print(
            f"row_count_match="
            f"{bool(result['row_count_match'])}"
        )

        return pipeline_run_id


    @task(
        task_id="route_deterministically",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def deterministic_routing(
        pipeline_run_id: str,
    ) -> str:

        result = route_tickets(
            run_id=pipeline_run_id
        )

        print(
            f"deterministic_rows="
            f"{result['deterministic_rows']}"
        )

        print(
            f"ai_candidate_rows="
            f"{result['ai_candidate_rows']}"
        )

        print(
            f"overlap_rows="
            f"{result['overlap_rows']}"
        )

        return pipeline_run_id


    @task(
        task_id="enrich_ai_candidates",
        retries=2,
        retry_delay=timedelta(
            minutes=1
        ),
    )
    def ai_enrichment(
        pipeline_run_id: str,
    ) -> str:

        result = enrich_candidate_batch(
            run_id=pipeline_run_id
        )

        print(
            f"candidate_rows="
            f"{result['candidate_rows']}"
        )

        print(
            f"successful_rows="
            f"{result['successful_rows']}"
        )

        print(
            f"failed_rows="
            f"{result['failed_rows']}"
        )

        print(
            f"api_calls="
            f"{result['api_calls']}"
        )

        print(
            f"reused_results="
            f"{result['reused_results']}"
        )

        return pipeline_run_id


    @task(
        task_id="validate_and_route_results",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def validate_and_route(
        pipeline_run_id: str,
    ) -> str:

        result = route_final_results(
            run_id=pipeline_run_id
        )

        print(
            f"final_rows="
            f"{result['final_rows']}"
        )

        print(
            f"deterministic_rows="
            f"{result['deterministic_rows']}"
        )

        print(
            f"ai_accepted_rows="
            f"{result['ai_accepted_rows']}"
        )

        print(
            f"human_review_rows="
            f"{result['human_review_rows']}"
        )

        print(
            f"ai_failure_fallback_rows="
            f"{result['ai_failure_fallback_rows']}"
        )

        return pipeline_run_id


    @task(
        task_id="build_summary_tables",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def summaries(
        pipeline_run_id: str,
    ) -> str:

        result = build_daily_summary(
            run_id=pipeline_run_id
        )

        print(
            f"summarized_tickets="
            f"{result['summarized_tickets']}"
        )

        print(
            f"summary_rows="
            f"{result['summary_rows']}"
        )

        print(
            f"row_count_match="
            f"{bool(result['row_count_match'])}"
        )

        return pipeline_run_id


    @task(
        task_id="evaluate_pipeline",
        retries=0,
    )
    def evaluation(
        pipeline_run_id: str,
    ) -> str:

        result = evaluate_pipeline()

        print(
            f"evaluated_rows="
            f"{result['evaluated_rows']}"
        )

        print(
            f"final_handling_accuracy="
            f"{result['final_handling_accuracy_pct']:.1f}%"
        )

        print(
            f"overall_category_accuracy="
            f"{result['overall_category_accuracy_pct']:.1f}%"
        )

        print(
            f"mismatch_rows="
            f"{result['mismatch_rows']}"
        )

        if result[
            "mismatch_rows"
        ] != 0:
            raise RuntimeError(
                "Pipeline evaluation "
                "contains mismatches."
            )

        return pipeline_run_id


    @task(
        task_id="finalize_pipeline_run",
        retries=0,
    )
    def finalize(
        pipeline_run_id: str,
    ) -> str:

        result = finalize_pipeline_run(
            pipeline_run_id
        )

        print(
            f"pipeline_run_id="
            f"{result['run_id']}"
        )

        print(
            f"pipeline_status="
            f"{result['status']}"
        )

        print(
            f"quarantined_rows="
            f"{result['quarantined_rows']}"
        )

        return pipeline_run_id


    pipeline_run_id = (
        ingest_source()
    )

    pipeline_run_id = transform(
        pipeline_run_id
    )

    pipeline_run_id = (
        deterministic_routing(
            pipeline_run_id
        )
    )

    pipeline_run_id = (
        ai_enrichment(
            pipeline_run_id
        )
    )

    pipeline_run_id = (
        validate_and_route(
            pipeline_run_id
        )
    )

    pipeline_run_id = summaries(
        pipeline_run_id
    )

    pipeline_run_id = evaluation(
        pipeline_run_id
    )

    finalize(
        pipeline_run_id
    )


sql_ai_ticket_pipeline()