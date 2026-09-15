from pathlib import Path

import pandas as pd

from src.config import (
    GROUND_TRUTH_DIR,
    PROJECT_ROOT,
)
from src.database import (
    get_connection,
    initialize_database,
)


GROUND_TRUTH_PATH = (
    GROUND_TRUTH_DIR
    / "ticket_ground_truth.csv"
)

OUTPUTS_DIR = (
    PROJECT_ROOT
    / "outputs"
)

EVALUATION_OUTPUT = (
    OUTPUTS_DIR
    / "pipeline_evaluation.csv"
)

MISMATCH_OUTPUT = (
    OUTPUTS_DIR
    / "evaluation_mismatches.csv"
)


def safe_rate(
    numerator: int,
    denominator: int,
) -> float:
    """Return a percentage safely."""
    if denominator == 0:
        return 0.0

    return (
        numerator
        / denominator
        * 100
    )


def load_actual_results() -> pd.DataFrame:
    """
    Load pipeline outputs needed for evaluation.
    """

    connection = get_connection()

    try:
        query = """
        SELECT
            tc.ticket_id,

            dc.category
                AS deterministic_category,

            CASE
                WHEN ac.ticket_id IS NOT NULL
                THEN 1
                ELSE 0
            END AS actual_ai_candidate,

            ai.category
                AS ai_category,

            ai.confidence
                AS ai_confidence,

            ai.result_status
                AS ai_result_status,

            ftc.final_category,

            ftc.final_handling,

            ftc.ai_used

        FROM ticket_clean AS tc

        LEFT JOIN deterministic_classification AS dc
            ON tc.ticket_id = dc.ticket_id

        LEFT JOIN ai_candidate AS ac
            ON tc.ticket_id = ac.ticket_id

        LEFT JOIN ai_classification AS ai
            ON tc.ticket_id = ai.ticket_id
           AND ai.result_status = 'SUCCESS'

        LEFT JOIN final_ticket_classification AS ftc
            ON tc.ticket_id = ftc.ticket_id

        ORDER BY tc.ticket_id;
        """

        return pd.read_sql_query(
            query,
            connection,
        )

    finally:
        connection.close()


def evaluate_pipeline() -> dict:
    """
    Compare actual pipeline behavior with synthetic
    ground truth that was locked before AI execution.
    """

    initialize_database()

    if not GROUND_TRUTH_PATH.exists():
        raise FileNotFoundError(
            f"Ground truth not found: "
            f"{GROUND_TRUTH_PATH}"
        )

    ground_truth = pd.read_csv(
        GROUND_TRUTH_PATH,
        keep_default_na=False,
    )

    actual = load_actual_results()

    evaluation = ground_truth.merge(
        actual,
        on="ticket_id",
        how="left",
        validate="one_to_one",
    )

    if len(evaluation) != len(ground_truth):
        raise RuntimeError(
            "Evaluation merge changed row count."
        )

    # ---------------------------------------------------------
    # Expected AI-candidate selection
    # ---------------------------------------------------------

    evaluation[
        "ai_candidate_correct"
    ] = (
        evaluation["expected_ai_needed"]
        == evaluation["actual_ai_candidate"]
    )

    expected_ai = (
        evaluation["expected_ai_needed"] == 1
    )

    actual_ai = (
        evaluation["actual_ai_candidate"] == 1
    )

    true_positive_ai = int(
        (expected_ai & actual_ai).sum()
    )

    false_positive_ai = int(
        ((~expected_ai) & actual_ai).sum()
    )

    false_negative_ai = int(
        (expected_ai & (~actual_ai)).sum()
    )

    ai_candidate_precision = safe_rate(
        true_positive_ai,
        true_positive_ai
        + false_positive_ai,
    )

    ai_candidate_recall = safe_rate(
        true_positive_ai,
        true_positive_ai
        + false_negative_ai,
    )

    # ---------------------------------------------------------
    # Deterministic classification accuracy
    # ---------------------------------------------------------

    deterministic_mask = (
        evaluation["expected_route"]
        == "DETERMINISTIC"
    )

    evaluation[
        "deterministic_category_correct"
    ] = False

    evaluation.loc[
        deterministic_mask,
        "deterministic_category_correct",
    ] = (
        evaluation.loc[
            deterministic_mask,
            "deterministic_category",
        ]
        ==
        evaluation.loc[
            deterministic_mask,
            "expected_category",
        ]
    )

    deterministic_correct = int(
        evaluation.loc[
            deterministic_mask,
            "deterministic_category_correct",
        ].sum()
    )

    deterministic_total = int(
        deterministic_mask.sum()
    )

    deterministic_accuracy = safe_rate(
        deterministic_correct,
        deterministic_total,
    )

    # ---------------------------------------------------------
    # AI category accuracy
    #
    # Only records whose expected final route is AI.
    # Human-review ground truth is evaluated separately.
    # ---------------------------------------------------------

    expected_ai_final_mask = (
        evaluation["expected_route"]
        == "AI"
    )

    evaluation[
        "ai_category_correct"
    ] = False

    evaluation.loc[
        expected_ai_final_mask,
        "ai_category_correct",
    ] = (
        evaluation.loc[
            expected_ai_final_mask,
            "ai_category",
        ]
        ==
        evaluation.loc[
            expected_ai_final_mask,
            "expected_category",
        ]
    )

    ai_category_correct = int(
        evaluation.loc[
            expected_ai_final_mask,
            "ai_category_correct",
        ].sum()
    )

    ai_category_total = int(
        expected_ai_final_mask.sum()
    )

    ai_category_accuracy = safe_rate(
        ai_category_correct,
        ai_category_total,
    )

    # ---------------------------------------------------------
    # Human-review routing
    # ---------------------------------------------------------

    expected_review_mask = (
        evaluation[
            "expected_human_review"
        ]
        == 1
    )

    actual_review_mask = (
        evaluation["final_handling"]
        == "HUMAN_REVIEW"
    )

    true_positive_review = int(
        (
            expected_review_mask
            & actual_review_mask
        ).sum()
    )

    review_total = int(
        expected_review_mask.sum()
    )

    human_review_recall = safe_rate(
        true_positive_review,
        review_total,
    )

    # ---------------------------------------------------------
    # Expected final handling
    # ---------------------------------------------------------

    expected_final_handling = (
        evaluation[
            "expected_route"
        ].map(
            {
                "DETERMINISTIC":
                    "DETERMINISTIC",
                "AI":
                    "AI_ACCEPTED",
                "HUMAN_REVIEW":
                    "HUMAN_REVIEW",
            }
        )
    )

    evaluation[
        "expected_final_handling"
    ] = expected_final_handling

    evaluation[
        "final_handling_correct"
    ] = (
        evaluation[
            "expected_final_handling"
        ]
        ==
        evaluation[
            "final_handling"
        ]
    )

    final_handling_correct = int(
        evaluation[
            "final_handling_correct"
        ].sum()
    )

    final_handling_accuracy = safe_rate(
        final_handling_correct,
        len(evaluation),
    )

    # ---------------------------------------------------------
    # Overall category correctness
    #
    # Human-review cases intentionally have no expected
    # category and are excluded here.
    # ---------------------------------------------------------

    category_expected_mask = (
        evaluation[
            "expected_category"
        ].str.strip()
        != ""
    )

    evaluation[
        "final_category_correct"
    ] = False

    evaluation.loc[
        category_expected_mask,
        "final_category_correct",
    ] = (
        evaluation.loc[
            category_expected_mask,
            "final_category",
        ]
        ==
        evaluation.loc[
            category_expected_mask,
            "expected_category",
        ]
    )

    overall_category_correct = int(
        evaluation.loc[
            category_expected_mask,
            "final_category_correct",
        ].sum()
    )

    overall_category_total = int(
        category_expected_mask.sum()
    )

    overall_category_accuracy = safe_rate(
        overall_category_correct,
        overall_category_total,
    )

    # ---------------------------------------------------------
    # Write detailed evidence
    # ---------------------------------------------------------

    OUTPUTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    evaluation.to_csv(
        EVALUATION_OUTPUT,
        index=False,
    )

    mismatch_mask = (
        (~evaluation[
            "ai_candidate_correct"
        ])
        |
        (
            deterministic_mask
            &
            (~evaluation[
                "deterministic_category_correct"
            ])
        )
        |
        (
            expected_ai_final_mask
            &
            (~evaluation[
                "ai_category_correct"
            ])
        )
        |
        (
            ~evaluation[
                "final_handling_correct"
            ]
        )
        |
        (
            category_expected_mask
            &
            (~evaluation[
                "final_category_correct"
            ])
        )
    )

    mismatches = evaluation[
        mismatch_mask
    ].copy()

    mismatches.to_csv(
        MISMATCH_OUTPUT,
        index=False,
    )

    metrics = {
        "evaluated_rows":
            len(evaluation),

        "deterministic_accuracy_pct":
            deterministic_accuracy,

        "ai_candidate_precision_pct":
            ai_candidate_precision,

        "ai_candidate_recall_pct":
            ai_candidate_recall,

        "ai_category_accuracy_pct":
            ai_category_accuracy,

        "human_review_recall_pct":
            human_review_recall,

        "final_handling_accuracy_pct":
            final_handling_accuracy,

        "overall_category_accuracy_pct":
            overall_category_accuracy,

        "false_positive_ai_escalations":
            false_positive_ai,

        "false_negative_ai_escalations":
            false_negative_ai,

        "mismatch_rows":
            len(mismatches),
    }

    metrics_df = pd.DataFrame(
        [
            {
                "metric": key,
                "value": value,
            }
            for key, value
            in metrics.items()
        ]
    )

    metrics_df.to_csv(
        OUTPUTS_DIR
        / "evaluation_metrics.csv",
        index=False,
    )

    return {
        **metrics,
        "evaluation_output":
            EVALUATION_OUTPUT,
        "mismatch_output":
            MISMATCH_OUTPUT,
    }


def print_evaluation(
    result: dict,
) -> None:
    """Print evaluation metrics."""

    print("PIPELINE EVALUATION")
    print("-------------------")

    print(
        f"Evaluated rows:              "
        f"{result['evaluated_rows']}"
    )

    print(
        f"Deterministic accuracy:      "
        f"{result['deterministic_accuracy_pct']:.1f}%"
    )

    print(
        f"AI candidate precision:      "
        f"{result['ai_candidate_precision_pct']:.1f}%"
    )

    print(
        f"AI candidate recall:         "
        f"{result['ai_candidate_recall_pct']:.1f}%"
    )

    print(
        f"AI category accuracy:        "
        f"{result['ai_category_accuracy_pct']:.1f}%"
    )

    print(
        f"Human-review recall:         "
        f"{result['human_review_recall_pct']:.1f}%"
    )

    print(
        f"Final-handling accuracy:     "
        f"{result['final_handling_accuracy_pct']:.1f}%"
    )

    print(
        f"Overall category accuracy:   "
        f"{result['overall_category_accuracy_pct']:.1f}%"
    )

    print(
        f"False-positive AI routes:    "
        f"{result['false_positive_ai_escalations']}"
    )

    print(
        f"False-negative AI routes:    "
        f"{result['false_negative_ai_escalations']}"
    )

    print(
        f"Mismatch rows:               "
        f"{result['mismatch_rows']}"
    )

    print()

    print(
        f"Detailed evaluation: "
        f"{result['evaluation_output']}"
    )

    print(
        f"Mismatch report:     "
        f"{result['mismatch_output']}"
    )

    print()
    print("EVALUATION COMPLETE")


if __name__ == "__main__":
    result = evaluate_pipeline()

    print_evaluation(result)
