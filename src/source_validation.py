from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.config import INCOMING_DIR


EXPECTED_COLUMNS = [
    "ticket_id",
    "created_date",
    "ticket_text",
]


@dataclass
class ValidationResult:
    file_path: Path
    schema_valid: bool
    total_rows: int
    valid_rows: int
    invalid_rows: int
    issues: pd.DataFrame


def validate_source_file(file_path: Path) -> ValidationResult:
    """
    Validate one incoming ticket CSV without changing source data.

    This function performs deterministic file, schema, uniqueness,
    null/blank, and date-format checks. It does not use AI.
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"Required input file not found: {file_path}"
        )

    if file_path.stat().st_size == 0:
        raise ValueError(
            f"Input file is empty: {file_path}"
        )

    # Read everything as text so validation occurs before type coercion.
    df = pd.read_csv(
        file_path,
        dtype=str,
        keep_default_na=False,
    )

    actual_columns = list(df.columns)

    missing_columns = [
        column
        for column in EXPECTED_COLUMNS
        if column not in actual_columns
    ]

    unexpected_columns = [
        column
        for column in actual_columns
        if column not in EXPECTED_COLUMNS
    ]

    if missing_columns:
        raise ValueError(
            "Source schema validation failed. "
            f"Missing required columns: {missing_columns}"
        )

    if unexpected_columns:
        raise ValueError(
            "Source schema validation failed. "
            f"Unexpected columns: {unexpected_columns}"
        )

    if df.empty:
        raise ValueError(
            f"Input file contains headers but no records: {file_path}"
        )

    # Preserve the physical source row number.
    # CSV row 1 is the header, so data begins on row 2.
    df["source_row_number"] = range(2, len(df) + 2)

    issues = []

    # ---------------------------------------------------------
    # Normalize temporary validation values
    # ---------------------------------------------------------

    ticket_ids = df["ticket_id"].str.strip()
    created_dates = df["created_date"].str.strip()
    ticket_text = df["ticket_text"].str.strip()

    # ---------------------------------------------------------
    # ticket_id checks
    # ---------------------------------------------------------

    blank_ticket_id = ticket_ids.eq("")

    for index in df.index[blank_ticket_id]:
        issues.append(
            {
                "source_row_number": int(
                    df.loc[index, "source_row_number"]
                ),
                "ticket_id": df.loc[index, "ticket_id"],
                "issue_code": "NULL_TICKET_ID",
                "issue_detail": "ticket_id is blank.",
            }
        )

    numeric_ticket_id = ticket_ids.str.fullmatch(r"\d+")

    for index in df.index[
        (~blank_ticket_id) & (~numeric_ticket_id)
    ]:
        issues.append(
            {
                "source_row_number": int(
                    df.loc[index, "source_row_number"]
                ),
                "ticket_id": df.loc[index, "ticket_id"],
                "issue_code": "INVALID_TICKET_ID",
                "issue_detail": (
                    "ticket_id must contain only integer digits."
                ),
            }
        )

    duplicate_ticket_id = (
        ticket_ids.ne("")
        & ticket_ids.duplicated(keep=False)
    )

    for index in df.index[duplicate_ticket_id]:
        issues.append(
            {
                "source_row_number": int(
                    df.loc[index, "source_row_number"]
                ),
                "ticket_id": df.loc[index, "ticket_id"],
                "issue_code": "DUPLICATE_TICKET_ID",
                "issue_detail": (
                    "ticket_id appears more than once in the source file."
                ),
            }
        )

    # ---------------------------------------------------------
    # created_date checks
    # ---------------------------------------------------------

    blank_created_date = created_dates.eq("")

    for index in df.index[blank_created_date]:
        issues.append(
            {
                "source_row_number": int(
                    df.loc[index, "source_row_number"]
                ),
                "ticket_id": df.loc[index, "ticket_id"],
                "issue_code": "NULL_CREATED_DATE",
                "issue_detail": "created_date is blank.",
            }
        )

    parsed_dates = pd.to_datetime(
        created_dates,
        format="%Y-%m-%d",
        errors="coerce",
    )

    invalid_created_date = (
        (~blank_created_date)
        & parsed_dates.isna()
    )

    for index in df.index[invalid_created_date]:
        issues.append(
            {
                "source_row_number": int(
                    df.loc[index, "source_row_number"]
                ),
                "ticket_id": df.loc[index, "ticket_id"],
                "issue_code": "INVALID_CREATED_DATE",
                "issue_detail": (
                    "created_date must be a valid YYYY-MM-DD date."
                ),
            }
        )

    # ---------------------------------------------------------
    # ticket_text checks
    # ---------------------------------------------------------

    blank_ticket_text = ticket_text.eq("")

    for index in df.index[blank_ticket_text]:
        issues.append(
            {
                "source_row_number": int(
                    df.loc[index, "source_row_number"]
                ),
                "ticket_id": df.loc[index, "ticket_id"],
                "issue_code": "NULL_TICKET_TEXT",
                "issue_detail": (
                    "ticket_text is blank or whitespace only."
                ),
            }
        )

    # ---------------------------------------------------------
    # Build validation result
    # ---------------------------------------------------------

    issues_df = pd.DataFrame(
        issues,
        columns=[
            "source_row_number",
            "ticket_id",
            "issue_code",
            "issue_detail",
        ],
    )

    invalid_source_rows = (
        set(issues_df["source_row_number"].tolist())
        if not issues_df.empty
        else set()
    )

    invalid_rows = len(invalid_source_rows)
    valid_rows = len(df) - invalid_rows

    return ValidationResult(
        file_path=file_path,
        schema_valid=True,
        total_rows=len(df),
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        issues=issues_df,
    )


def print_validation_result(result: ValidationResult) -> None:
    """
    Print a compact validation report for local testing and logs.
    """

    print(f"Source file:   {result.file_path}")
    print(f"Schema valid: {result.schema_valid}")
    print(f"Rows read:    {result.total_rows}")
    print(f"Valid rows:   {result.valid_rows}")
    print(f"Invalid rows: {result.invalid_rows}")
    print(f"Issues found: {len(result.issues)}")

    if result.issues.empty:
        print()
        print("SOURCE VALIDATION: PASS")
    else:
        print()
        print("SOURCE VALIDATION: EXCEPTIONS FOUND")
        print()
        print(result.issues.to_string(index=False))


if __name__ == "__main__":
    source_file = INCOMING_DIR / "tickets_batch_001.csv"

    validation_result = validate_source_file(source_file)

    print_validation_result(validation_result)
