from pathlib import Path

import pandas as pd
import pytest

from src.ai_enrichment import validate_ai_payload
from src.ingestion import calculate_source_hash
from src.source_validation import validate_source_file


def test_clean_source_file_passes(tmp_path: Path):
    source_file = tmp_path / "tickets.csv"

    pd.DataFrame(
        [
            {
                "ticket_id": "1",
                "created_date": "2026-01-01",
                "ticket_text": "My paycheck is incorrect.",
            },
            {
                "ticket_id": "2",
                "created_date": "2026-01-02",
                "ticket_text": "I cannot log into the portal.",
            },
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    result = validate_source_file(source_file)

    assert result.schema_valid is True
    assert result.total_rows == 2
    assert result.valid_rows == 2
    assert result.invalid_rows == 0
    assert result.issues.empty


def test_missing_required_column_fails(tmp_path: Path):
    source_file = tmp_path / "tickets.csv"

    pd.DataFrame(
        [
            {
                "ticket_id": "1",
                "created_date": "2026-01-01",
            }
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        validate_source_file(source_file)


def test_duplicate_ticket_ids_are_detected(tmp_path: Path):
    source_file = tmp_path / "tickets.csv"

    pd.DataFrame(
        [
            {
                "ticket_id": "100",
                "created_date": "2026-01-01",
                "ticket_text": "First ticket.",
            },
            {
                "ticket_id": "100",
                "created_date": "2026-01-02",
                "ticket_text": "Duplicate ticket.",
            },
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    result = validate_source_file(source_file)

    assert result.total_rows == 2
    assert result.valid_rows == 0
    assert result.invalid_rows == 2

    assert (
        result.issues["issue_code"]
        == "DUPLICATE_TICKET_ID"
    ).sum() == 2


def test_invalid_date_is_quarantinable(tmp_path: Path):
    source_file = tmp_path / "tickets.csv"

    pd.DataFrame(
        [
            {
                "ticket_id": "1",
                "created_date": "",
                "ticket_text": "Valid text.",
            },
            {
                "ticket_id": "2",
                "created_date": "2026-01-02",
                "ticket_text": "Another ticket.",
            },
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    result = validate_source_file(source_file)

    assert result.total_rows == 2
    assert result.valid_rows == 1
    assert result.invalid_rows == 1

    assert (
        "NULL_CREATED_DATE"
        in result.issues["issue_code"].tolist()
    )


def test_blank_ticket_text_is_detected(tmp_path: Path):
    source_file = tmp_path / "tickets.csv"

    pd.DataFrame(
        [
            {
                "ticket_id": "1",
                "created_date": "2026-01-01",
                "ticket_text": "   ",
            }
        ]
    ).to_csv(
        source_file,
        index=False,
    )

    result = validate_source_file(source_file)

    assert result.invalid_rows == 1

    assert (
        "NULL_TICKET_TEXT"
        in result.issues["issue_code"].tolist()
    )


def test_source_hash_is_deterministic():
    first = calculate_source_hash(
        "1",
        "2026-01-01",
        "Example ticket",
    )

    second = calculate_source_hash(
        "1",
        "2026-01-01",
        "Example ticket",
    )

    assert first == second


def test_source_hash_changes_when_record_changes():
    original = calculate_source_hash(
        "1",
        "2026-01-01",
        "Example ticket",
    )

    changed = calculate_source_hash(
        "1",
        "2026-01-01",
        "Changed ticket",
    )

    assert original != changed


def test_valid_ai_payload_passes():
    payload = {
        "category": "PAYROLL",
        "sentiment": "NEGATIVE",
        "summary": "Employee reports an incorrect pay rate.",
        "confidence": 0.95,
    }

    result = validate_ai_payload(payload)

    assert result["category"] == "PAYROLL"
    assert result["sentiment"] == "NEGATIVE"
    assert result["confidence"] == 0.95


def test_invalid_ai_category_fails():
    payload = {
        "category": "RANDOM_CATEGORY",
        "sentiment": "NEUTRAL",
        "summary": "Test.",
        "confidence": 0.90,
    }

    with pytest.raises(
        ValueError,
        match="Invalid AI category",
    ):
        validate_ai_payload(payload)


def test_invalid_ai_confidence_fails():
    payload = {
        "category": "PAYROLL",
        "sentiment": "NEGATIVE",
        "summary": "Test.",
        "confidence": 1.5,
    }

    with pytest.raises(
        ValueError,
        match="between 0 and 1",
    ):
        validate_ai_payload(payload)
