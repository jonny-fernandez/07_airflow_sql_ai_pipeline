import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from openai import OpenAI

from src.config import PROJECT_ROOT
from src.database import (
    get_connection,
    initialize_database,
)


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv(PROJECT_ROOT / ".env")

MODEL = os.getenv(
    "OPENAI_MODEL",
    "gpt-5.6-luna",
)

PROMPT_VERSION = "ticket-classifier-v1"


ALLOWED_CATEGORIES = {
    "PAYROLL",
    "IT_SUPPORT",
    "TRAINING",
    "ONBOARDING",
    "UNRESOLVED",
}

ALLOWED_SENTIMENTS = {
    "NEGATIVE",
    "NEUTRAL",
    "POSITIVE",
}


TICKET_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {
            "type": "string",
            "enum": [
                "PAYROLL",
                "IT_SUPPORT",
                "TRAINING",
                "ONBOARDING",
                "UNRESOLVED",
            ],
        },
        "sentiment": {
            "type": "string",
            "enum": [
                "NEGATIVE",
                "NEUTRAL",
                "POSITIVE",
            ],
        },
        "summary": {
            "type": "string",
        },
        "confidence": {
            "type": "number",
        },
    },
    "required": [
        "category",
        "sentiment",
        "summary",
        "confidence",
    ],
    "additionalProperties": False,
}


SYSTEM_INSTRUCTIONS = """
You classify synthetic employee-support tickets.

Use only the supplied ticket text as evidence.

Allowed categories:

PAYROLL
Compensation, pay rate, paycheck, direct deposit,
or other payroll-related problems.

IT_SUPPORT
Login, password, application, software, portal,
upload, system, or technical problems.

TRAINING
Training requirements, courses, completion status,
or training certificates.

ONBOARDING
Onboarding forms, documents, or onboarding process.

UNRESOLVED
Use this when the supplied text does not contain
enough information to confidently select one of
the other categories without guessing.

Do not invent missing context.

The summary must be a short factual description
grounded only in the supplied text.

Confidence must be between 0 and 1.
"""


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""

    return datetime.now(
        timezone.utc
    ).isoformat()


def get_candidate(ticket_id: int) -> dict:
    """
    Retrieve one record already selected by the
    deterministic routing stage.
    """

    connection = get_connection()

    try:
        row = connection.execute(
            """
            SELECT
                ticket_id,
                evidence_text,
                candidate_reason,
                input_hash
            FROM ai_candidate
            WHERE ticket_id = ?;
            """,
            (ticket_id,),
        ).fetchone()

        if row is None:
            raise ValueError(
                f"Ticket {ticket_id} is not an AI candidate."
            )

        return dict(row)

    finally:
        connection.close()


def get_existing_success(
    ticket_id: int,
    input_hash: str,
) -> dict | None:
    """
    Reuse an already successful AI result for the
    same ticket/input/model/prompt combination.

    This prevents unnecessary duplicate API calls
    during safe reruns.
    """

    connection = get_connection()

    try:
        row = connection.execute(
            """
            SELECT
                ticket_id,
                category,
                sentiment,
                summary,
                confidence,
                model,
                prompt_version,
                processed_at
            FROM ai_classification
            WHERE ticket_id = ?
              AND input_hash = ?
              AND prompt_version = ?
              AND model = ?
              AND result_status = 'SUCCESS'
            LIMIT 1;
            """,
            (
                ticket_id,
                input_hash,
                PROMPT_VERSION,
                MODEL,
            ),
        ).fetchone()

        if row is None:
            return None

        return dict(row)

    finally:
        connection.close()


def validate_ai_payload(payload: dict) -> dict:
    """
    Perform deterministic validation even though
    Structured Outputs already constrains shape.

    The model never decides whether its own output
    is accepted by the pipeline.
    """

    category = payload.get("category")
    sentiment = payload.get("sentiment")
    summary = payload.get("summary")
    confidence = payload.get("confidence")

    if category not in ALLOWED_CATEGORIES:
        raise ValueError(
            f"Invalid AI category: {category}"
        )

    if sentiment not in ALLOWED_SENTIMENTS:
        raise ValueError(
            f"Invalid AI sentiment: {sentiment}"
        )

    if not isinstance(summary, str):
        raise ValueError(
            "AI summary must be a string."
        )

    if not summary.strip():
        raise ValueError(
            "AI summary cannot be blank."
        )

    if not isinstance(
        confidence,
        (int, float),
    ):
        raise ValueError(
            "AI confidence must be numeric."
        )

    if not 0.0 <= float(confidence) <= 1.0:
        raise ValueError(
            "AI confidence must be between 0 and 1."
        )

    return {
        "category": category,
        "sentiment": sentiment,
        "summary": summary.strip(),
        "confidence": float(confidence),
    }


def call_ai(evidence_text: str) -> dict:
    """
    Call the model using strict JSON Structured Outputs.
    """

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError(
            "OPENAI_API_KEY was not found in .env."
        )

    client = OpenAI()

    response = client.responses.create(
        model=MODEL,

        reasoning={
            "effort": "none",
        },

        instructions=SYSTEM_INSTRUCTIONS,

        input=(
            "Classify this synthetic support ticket:\n\n"
            f"{evidence_text}"
        ),

        text={
            "format": {
                "type": "json_schema",
                "name": "ticket_enrichment",
                "strict": True,
                "schema": TICKET_SCHEMA,
            },
            "verbosity": "low",
        },

        max_output_tokens=300,

        # Synthetic data, but no need to persist the
        # API response remotely for this portfolio test.
        store=False,
    )

    if not response.output_text:
        raise RuntimeError(
            "OpenAI returned no structured output."
        )

    payload = json.loads(
        response.output_text
    )

    return validate_ai_payload(payload)


def persist_success(
    candidate: dict,
    result: dict,
) -> None:
    """Persist a successful structured AI result."""

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO ai_classification (
                ticket_id,
                evidence_text,
                input_hash,
                prompt_version,
                model,
                category,
                sentiment,
                summary,
                confidence,
                ai_used,
                result_status,
                error_type,
                error_message,
                processed_at
            )
            VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                1,
                'SUCCESS',
                NULL,
                NULL,
                ?
            )

            ON CONFLICT (
                ticket_id,
                input_hash,
                prompt_version,
                model
            )
            DO UPDATE SET
                evidence_text =
                    excluded.evidence_text,
                category =
                    excluded.category,
                sentiment =
                    excluded.sentiment,
                summary =
                    excluded.summary,
                confidence =
                    excluded.confidence,
                ai_used = 1,
                result_status = 'SUCCESS',
                error_type = NULL,
                error_message = NULL,
                processed_at =
                    excluded.processed_at;
            """,
            (
                candidate["ticket_id"],
                candidate["evidence_text"],
                candidate["input_hash"],
                PROMPT_VERSION,
                MODEL,
                result["category"],
                result["sentiment"],
                result["summary"],
                result["confidence"],
                utc_now(),
            ),
        )

        connection.commit()

    finally:
        connection.close()


def persist_failure(
    candidate: dict,
    error: Exception,
) -> None:
    """
    Persist an AI-specific failure separately from
    deterministic pipeline processing.
    """

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO ai_classification (
                ticket_id,
                evidence_text,
                input_hash,
                prompt_version,
                model,
                category,
                sentiment,
                summary,
                confidence,
                ai_used,
                result_status,
                error_type,
                error_message,
                processed_at
            )
            VALUES (
                ?, ?, ?, ?, ?,
                NULL,
                NULL,
                NULL,
                NULL,
                1,
                'FAILED',
                ?,
                ?,
                ?
            )

            ON CONFLICT (
                ticket_id,
                input_hash,
                prompt_version,
                model
            )
            DO UPDATE SET
                result_status = 'FAILED',
                error_type =
                    excluded.error_type,
                error_message =
                    excluded.error_message,
                processed_at =
                    excluded.processed_at;
            """,
            (
                candidate["ticket_id"],
                candidate["evidence_text"],
                candidate["input_hash"],
                PROMPT_VERSION,
                MODEL,
                type(error).__name__,
                str(error),
                utc_now(),
            ),
        )

        connection.commit()

    finally:
        connection.close()


def enrich_one_ticket(
    ticket_id: int,
) -> dict:
    """
    Enrich exactly one AI candidate.

    Successful identical results are reused instead
    of generating another API call.
    """

    initialize_database()

    candidate = get_candidate(ticket_id)

    existing = get_existing_success(
        ticket_id=candidate["ticket_id"],
        input_hash=candidate["input_hash"],
    )

    if existing is not None:
        return {
            "execution": "REUSED",
            **existing,
        }

    try:
        result = call_ai(
            candidate["evidence_text"]
        )

        persist_success(
            candidate,
            result,
        )

        return {
            "execution": "API_CALL",
            "ticket_id": candidate["ticket_id"],
            "evidence_text": (
                candidate["evidence_text"]
            ),
            "model": MODEL,
            "prompt_version": PROMPT_VERSION,
            **result,
        }

    except Exception as error:
        persist_failure(
            candidate,
            error,
        )

        raise


def print_result(result: dict) -> None:
    """Print the structured result."""

    print(
        f"Execution:      "
        f"{result['execution']}"
    )

    print(
        f"Ticket ID:      "
        f"{result['ticket_id']}"
    )

    if "evidence_text" in result:
        print(
            f"Evidence:       "
            f"{result['evidence_text']}"
        )

    print(
        f"Category:       "
        f"{result['category']}"
    )

    print(
        f"Sentiment:      "
        f"{result['sentiment']}"
    )

    print(
        f"Confidence:     "
        f"{result['confidence']}"
    )

    print(
        f"Summary:        "
        f"{result['summary']}"
    )

    print(
        f"Model:          "
        f"{result['model']}"
    )

    print(
        f"Prompt version: "
        f"{result['prompt_version']}"
    )


if __name__ == "__main__":
    # Ticket 4 is an intentionally AI-routed
    # technical-support case.
    result = enrich_one_ticket(
        ticket_id=4
    )

    print_result(result)
