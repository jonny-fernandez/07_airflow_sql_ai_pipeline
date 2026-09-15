from pathlib import Path

from airflow.models.dagbag import DagBag


PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)

DAG_FOLDER = (
    PROJECT_ROOT
    / "dags"
)

DAG_ID = "sql_ai_ticket_pipeline"


def load_dagbag() -> DagBag:
    """
    Load only the Project 7 DAG folder.

    Because dag_folder points directly to this project's
    dags directory, Airflow example DAGs are not included.
    """

    return DagBag(
        dag_folder=str(DAG_FOLDER),
    )


def test_dag_parses_without_import_errors():
    """
    Airflow must be able to discover and parse
    the DAG without import failures.
    """

    dagbag = load_dagbag()

    assert dagbag.import_errors == {}

    assert DAG_ID in dagbag.dags


def test_dag_contains_expected_tasks():
    """
    Verify the production workflow contains exactly
    the eight intended orchestration boundaries.
    """

    dagbag = load_dagbag()

    dag = dagbag.get_dag(
        DAG_ID
    )

    expected_tasks = {
        "ingest_source",
        "transform_tickets",
        "route_deterministically",
        "enrich_ai_candidates",
        "validate_and_route_results",
        "build_summary_tables",
        "evaluate_pipeline",
        "finalize_pipeline_run",
    }

    actual_tasks = {
        task.task_id
        for task in dag.tasks
    }

    assert actual_tasks == expected_tasks

    assert len(dag.tasks) == 8


def test_dag_dependency_chain():
    """
    The DAG must preserve the deterministic
    processing order.
    """

    dagbag = load_dagbag()

    dag = dagbag.get_dag(
        DAG_ID
    )

    expected_chain = [
        (
            "ingest_source",
            "transform_tickets",
        ),
        (
            "transform_tickets",
            "route_deterministically",
        ),
        (
            "route_deterministically",
            "enrich_ai_candidates",
        ),
        (
            "enrich_ai_candidates",
            "validate_and_route_results",
        ),
        (
            "validate_and_route_results",
            "build_summary_tables",
        ),
        (
            "build_summary_tables",
            "evaluate_pipeline",
        ),
        (
            "evaluate_pipeline",
            "finalize_pipeline_run",
        ),
    ]

    for upstream_id, downstream_id in expected_chain:
        upstream = dag.get_task(
            upstream_id
        )

        assert (
            downstream_id
            in upstream.downstream_task_ids
        )


def test_dag_operational_configuration():
    """
    Verify important orchestration controls remain
    configured as designed.
    """

    dagbag = load_dagbag()

    dag = dagbag.get_dag(
        DAG_ID
    )

    assert dag.catchup is False

    assert dag.max_active_runs == 1

    assert (
        dag.get_task(
            "ingest_source"
        ).retries
        == 1
    )

    assert (
        dag.get_task(
            "transform_tickets"
        ).retries
        == 1
    )

    assert (
        dag.get_task(
            "enrich_ai_candidates"
        ).retries
        == 2
    )

    assert (
        dag.get_task(
            "evaluate_pipeline"
        ).retries
        == 0
    )

    assert (
        dag.get_task(
            "finalize_pipeline_run"
        ).retries
        == 0
    )
