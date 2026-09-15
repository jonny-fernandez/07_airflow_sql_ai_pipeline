import pandas as pd

from src.config import (
    INCOMING_DIR,
    GROUND_TRUTH_DIR,
)


SOURCE_FILE = (
    INCOMING_DIR
    / "tickets_batch_001.csv"
)

OUTPUT_FILE = (
    GROUND_TRUTH_DIR
    / "ticket_ground_truth.csv"
)


GROUND_TRUTH_MAP = {
    "I cannot log into the employee portal.": {
        "expected_category": "IT_SUPPORT",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "My password reset is not working.": {
        "expected_category": "IT_SUPPORT",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "The application crashes when I upload a file.": {
        "expected_category": "IT_SUPPORT",
        "expected_route": "AI",
        "expected_ai_needed": 1,
        "expected_human_review": 0,
    },

    "My paycheck amount looks incorrect.": {
        "expected_category": "PAYROLL",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "My direct deposit did not arrive.": {
        "expected_category": "PAYROLL",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "The pay rate in my profile is wrong.": {
        "expected_category": "PAYROLL",
        "expected_route": "AI",
        "expected_ai_needed": 1,
        "expected_human_review": 0,
    },

    "The course is not showing as complete.": {
        "expected_category": "TRAINING",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "My training certificate is expiring soon.": {
        "expected_category": "TRAINING",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "Where can I find the required training?": {
        "expected_category": "TRAINING",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "Where do I upload onboarding documents?": {
        "expected_category": "ONBOARDING",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "I do not know which onboarding forms are left.": {
        "expected_category": "ONBOARDING",
        "expected_route": "DETERMINISTIC",
        "expected_ai_needed": 0,
        "expected_human_review": 0,
    },

    "My checklist still says incomplete.": {
        "expected_category": "",
        "expected_route": "HUMAN_REVIEW",
        "expected_ai_needed": 1,
        "expected_human_review": 1,
    },
}


def build_ground_truth():
    """
    Build evaluation-only expected results for the synthetic batch.

    This file must never be used by production classification
    or supplied to the AI prompt.
    """

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source file not found: {SOURCE_FILE}"
        )

    df = pd.read_csv(SOURCE_FILE)

    records = []

    for _, row in df.iterrows():
        ticket_text = row["ticket_text"]

        expected = GROUND_TRUTH_MAP.get(
            ticket_text
        )

        if expected is None:
            raise ValueError(
                "No ground truth mapping exists for: "
                f"{ticket_text}"
            )

        records.append(
            {
                "ticket_id": row["ticket_id"],
                "expected_category": (
                    expected["expected_category"]
                ),
                "expected_route": (
                    expected["expected_route"]
                ),
                "expected_ai_needed": (
                    expected["expected_ai_needed"]
                ),
                "expected_human_review": (
                    expected[
                        "expected_human_review"
                    ]
                ),
            }
        )

    ground_truth = pd.DataFrame(records)

    GROUND_TRUTH_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    ground_truth.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"Ground truth written: {OUTPUT_FILE}"
    )
    print(
        f"Rows:               {len(ground_truth)}"
    )

    print()
    print("Expected routing:")

    print(
        ground_truth[
            "expected_route"
        ].value_counts().to_string()
    )

    print()
    print("Expected categories:")

    print(
        ground_truth[
            "expected_category"
        ]
        .replace("", "UNRESOLVED")
        .value_counts()
        .to_string()
    )


if __name__ == "__main__":
    build_ground_truth()
