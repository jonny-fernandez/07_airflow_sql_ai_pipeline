from pathlib import Path

import pandas as pd
import pytest

import src.database as database

from src.ingestion import ingest_ticket_file
from src.routing import route_tickets
from src.transformations import transform_tickets


@pytest.fixture
def isolated_database(
    tmp_path: Path,
    monkeypatch,
):
    """
    Redirect all project database operations to a temporary
    SQLite database.

    The real data/processed/ticket_pipeline.db is untouched.
    """

    test_db = (
        tmp_path
        / "ticket_pipeline_test.db"
    )

    monkeypatch.setattr(
        database,
        "DB_PATH",
        test_db,
    )

    database.initialize_database()

    return test_db


def create_valid_test_source(
    tmp_path: Path,
) -> Path:
    """
    Create a small synthetic batch containing both
    deterministic and AI-routed cases.
    """

    source_file = (
        tmp_path
        / "tickets_test.csv"
    )

    pd.DataFrame(
        [
            {
                "ticket_id": "1",
                "created_date": "2026-01-01",
                "ticket_text": (
                    "My paycheck amount looks incorrect."
                ),
            },
            {
                "ticket_id": "2",
                "created_date": "2026-01-02",
                "ticket_text": (
                    "I cannot log into the employee portal."
                ),
            },
            {
                "ticket_id": "3",
                "created_date": "2026-01-03",
                "ticket_text": (
                    "Where can I find the required training?"
                ),
            },
            {
                "ticket_id": "4",
                "created_date": "2026-01-04",
                "ticket_text": (
                    "The application crashes "
                    "when I upload a file."
                ),
            },
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    return source_file


def test_ingestion_rerun_is_idempotent(
    tmp_path: Path,
    isolated_database,
):
    """
    Reprocessing identical source data must not create
    duplicate staging records.
    """

    source_file = (
        create_valid_test_source(
            tmp_path
        )
    )

    first = ingest_ticket_file(
        source_file,
        run_id="TEST-RUN-001",
    )

    second = ingest_ticket_file(
        source_file,
        run_id="TEST-RUN-002",
    )

    assert first["inserted_rows"] == 4
    assert first["unchanged_rows"] == 0
    assert first["staging_total"] == 4

    assert second["inserted_rows"] == 0
    assert second["updated_rows"] == 0
    assert second["unchanged_rows"] == 4
    assert second["staging_total"] == 4

    connection = (
        database.get_connection()
    )

    try:
        staging_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM stg_raw_tickets;
                """
            ).fetchone()[0]
        )

        run_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM pipeline_run;
                """
            ).fetchone()[0]
        )

    finally:
        connection.close()

    assert staging_count == 4

    # Execution history accumulates even though business
    # records remain idempotent.
    assert run_count == 2


def test_transform_and_routing_integrity(
    tmp_path: Path,
    isolated_database,
):
    """
    Validate staging -> clean -> deterministic/AI routing
    using a real temporary SQLite database.
    """

    source_file = (
        create_valid_test_source(
            tmp_path
        )
    )

    ingestion = ingest_ticket_file(
        source_file,
        run_id="TEST-ROUTING-001",
    )

    transformation = (
        transform_tickets(
            run_id=ingestion["run_id"]
        )
    )

    routing = route_tickets(
        run_id=ingestion["run_id"]
    )

    assert (
        transformation["staging_rows"]
        == 4
    )

    assert (
        transformation["clean_rows"]
        == 4
    )

    assert (
        transformation["duplicate_rows"]
        == 0
    )

    assert (
        transformation["row_count_match"]
        == 1
    )

    # Three messages match explicit deterministic rules.
    assert (
        routing["deterministic_rows"]
        == 3
    )

    # Application-crash message should require AI.
    assert (
        routing["ai_candidate_rows"]
        == 1
    )

    assert routing["overlap_rows"] == 0
    assert routing["unrouted_rows"] == 0

    connection = (
        database.get_connection()
    )

    try:
        candidate = (
            connection.execute(
                """
                SELECT
                    ticket_id,
                    evidence_text
                FROM ai_candidate;
                """
            ).fetchone()
        )

        categories = (
            connection.execute(
                """
                SELECT
                    ticket_id,
                    category
                FROM deterministic_classification
                ORDER BY ticket_id;
                """
            ).fetchall()
        )

    finally:
        connection.close()

    assert candidate["ticket_id"] == 4

    assert (
        "application crashes"
        in candidate[
            "evidence_text"
        ].lower()
    )

    category_map = {
        row["ticket_id"]:
            row["category"]
        for row in categories
    }

    assert (
        category_map[1]
        == "PAYROLL"
    )

    assert (
        category_map[2]
        == "IT_SUPPORT"
    )

    assert (
        category_map[3]
        == "TRAINING"
    )


def test_data_quality_exception_does_not_block_valid_rows(
    tmp_path: Path,
    isolated_database,
):
    """
    One invalid source row should be captured as a
    data-quality exception while valid records continue.
    """

    source_file = (
        tmp_path
        / "tickets_with_bad_row.csv"
    )

    pd.DataFrame(
        [
            {
                "ticket_id": "1",
                "created_date": "2026-01-01",
                "ticket_text": (
                    "My paycheck amount looks incorrect."
                ),
            },
            {
                "ticket_id": "9001",
                "created_date": "",
                "ticket_text": (
                    "Synthetic invalid-date test."
                ),
            },
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    result = ingest_ticket_file(
        source_file,
        run_id="TEST-DQ-001",
    )

    assert result["source_rows"] == 2
    assert result["valid_rows"] == 1
    assert result["invalid_rows"] == 1

    assert result["inserted_rows"] == 1
    assert result["staging_total"] == 1

    connection = (
        database.get_connection()
    )

    try:
        issue = (
            connection.execute(
                """
                SELECT
                    ticket_id,
                    issue_code,
                    issue_detail
                FROM ticket_quality_issues
                WHERE run_id = ?
                LIMIT 1;
                """,
                (
                    "TEST-DQ-001",
                ),
            ).fetchone()
        )

        staging_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM stg_raw_tickets;
                """
            ).fetchone()[0]
        )

    finally:
        connection.close()

    assert issue is not None

    assert (
        issue["ticket_id"]
        == 9001
    )

    assert (
        issue["issue_code"]
        == "NULL_CREATED_DATE"
    )

    assert staging_count == 1
