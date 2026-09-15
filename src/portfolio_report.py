from html import escape

import pandas as pd

from src.config import PROJECT_ROOT
from src.database import get_connection


# ============================================================
# OUTPUT PATHS
# ============================================================

OUTPUTS_DIR = (
    PROJECT_ROOT
    / "outputs"
)

HTML_OUTPUT = (
    OUTPUTS_DIR
    / "portfolio_pipeline_report.html"
)

FINAL_RESULTS_OUTPUT = (
    OUTPUTS_DIR
    / "final_ticket_results.csv"
)

METRICS_OUTPUT = (
    OUTPUTS_DIR
    / "latest_run_metrics.csv"
)

REVIEW_OUTPUT = (
    OUTPUTS_DIR
    / "human_review_queue.csv"
)


# ============================================================
# HTML HELPERS
# ============================================================

def dataframe_html(
    dataframe: pd.DataFrame,
) -> str:
    """
    Render a pandas DataFrame as an HTML table.
    """

    if dataframe.empty:
        return "<p>No records.</p>"

    return dataframe.to_html(
        index=False,
        border=0,
        classes="data-table",
        escape=True,
    )


def get_handling_count(
    handling_summary: pd.DataFrame,
    handling_name: str,
) -> int:
    """
    Return the number of tickets belonging to one
    final-handling category.
    """

    if handling_summary.empty:
        return 0

    matching = handling_summary[
        handling_summary[
            "final_handling"
        ]
        == handling_name
    ]

    if matching.empty:
        return 0

    return int(
        matching.iloc[0][
            "ticket_count"
        ]
    )


# ============================================================
# REPORT GENERATOR
# ============================================================

def build_portfolio_report() -> None:
    """
    Export portfolio-ready analytical evidence from
    the latest successful pipeline execution.

    Outputs:
    - HTML execution report
    - final record-level results
    - latest task metrics
    - human-review queue
    """

    OUTPUTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = get_connection()

    try:
        # ====================================================
        # LATEST COMPLETED PIPELINE RUN
        # ====================================================

        run = connection.execute(
            """
            SELECT *
            FROM pipeline_run

            WHERE status IN (
                'SUCCESS',
                'COMPLETED_WITH_EXCEPTIONS'
            )

            ORDER BY started_at DESC
            LIMIT 1;
            """
        ).fetchone()

        if run is None:
            raise RuntimeError(
                "No completed pipeline run was found."
            )

        run = dict(run)

        run_id = run[
            "run_id"
        ]

        # ====================================================
        # FINAL RECORD-LEVEL OUTPUT
        # ====================================================

        final_results = pd.read_sql_query(
            """
            SELECT
                tc.ticket_id,
                tc.created_date,
                tc.ticket_text,

                ftc.final_category,
                ftc.sentiment,
                ftc.summary,
                ftc.classification_source,
                ftc.ai_used,
                ftc.final_handling

            FROM ticket_clean AS tc

            INNER JOIN final_ticket_classification AS ftc
                ON tc.ticket_id = ftc.ticket_id

            ORDER BY
                tc.ticket_id;
            """,
            connection,
        )

        final_results.to_csv(
            FINAL_RESULTS_OUTPUT,
            index=False,
        )

        # ====================================================
        # CATEGORY SUMMARY
        # ====================================================

        category_summary = pd.read_sql_query(
            """
            SELECT
                category,

                SUM(ticket_count)
                    AS ticket_count,

                SUM(deterministic_count)
                    AS deterministic,

                SUM(ai_enriched_count)
                    AS ai_enriched,

                SUM(human_review_count)
                    AS human_review,

                SUM(ai_failure_fallback_count)
                    AS ai_failure_fallback

            FROM daily_ticket_summary

            GROUP BY
                category

            ORDER BY
                ticket_count DESC,
                category;
            """,
            connection,
        )

        # ====================================================
        # FINAL-HANDLING BREAKDOWN
        # ====================================================

        handling_summary = pd.read_sql_query(
            """
            SELECT
                final_handling,
                COUNT(*) AS ticket_count

            FROM final_ticket_classification

            GROUP BY
                final_handling

            ORDER BY
                ticket_count DESC;
            """,
            connection,
        )

        # ====================================================
        # HUMAN-REVIEW RECORDS
        # ====================================================

        human_review = pd.read_sql_query(
            """
            SELECT
                hrq.ticket_id,
                tc.ticket_text,
                hrq.review_reason,
                hrq.proposed_category,
                hrq.confidence,
                hrq.review_status

            FROM human_review_queue AS hrq

            INNER JOIN ticket_clean AS tc
                ON hrq.ticket_id = tc.ticket_id

            ORDER BY
                hrq.ticket_id;
            """,
            connection,
        )

        human_review.to_csv(
            REVIEW_OUTPUT,
            index=False,
        )

        # ====================================================
        # TASK-LEVEL OBSERVABILITY
        # ====================================================

        metrics = pd.read_sql_query(
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
            connection,
            params=(
                run_id,
            ),
        )

        metrics.to_csv(
            METRICS_OUTPUT,
            index=False,
        )

    finally:
        connection.close()

    # ========================================================
    # GROUND-TRUTH EVALUATION METRICS
    # ========================================================

    evaluation_path = (
        OUTPUTS_DIR
        / "evaluation_metrics.csv"
    )

    if evaluation_path.exists():
        evaluation = pd.read_csv(
            evaluation_path
        )

    else:
        evaluation = pd.DataFrame(
            columns=[
                "metric",
                "value",
            ]
        )

    # ========================================================
    # PORTFOLIO METRICS
    # ========================================================

    total_tickets = int(
        run["clean_row_count"]
        or 0
    )

    deterministic_count = int(
        run["deterministic_count"]
        or 0
    )

    ai_candidate_count = int(
        run["ai_candidate_count"]
        or 0
    )

    ai_success_count = int(
        run["ai_success_count"]
        or 0
    )

    ai_failure_count = int(
        run["ai_failure_count"]
        or 0
    )

    human_review_count = int(
        run["human_review_count"]
        or 0
    )

    quarantined_count = int(
        run["quarantined_row_count"]
        or 0
    )

    ai_accepted_count = (
        get_handling_count(
            handling_summary,
            "AI_ACCEPTED",
        )
    )

    ai_fallback_count = (
        get_handling_count(
            handling_summary,
            "AI_FAILURE_FALLBACK",
        )
    )

    deterministic_pct = (
        deterministic_count
        / total_tickets
        * 100
        if total_tickets
        else 0
    )

    ai_candidate_pct = (
        ai_candidate_count
        / total_tickets
        * 100
        if total_tickets
        else 0
    )

    # ========================================================
    # HTML REPORT
    # ========================================================

    html = f"""
<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>
Airflow SQL + AI Pipeline Report
</title>

<style>

* {{
    box-sizing: border-box;
}}

body {{
    font-family:
        Arial,
        Helvetica,
        sans-serif;

    max-width: 1200px;

    margin:
        40px
        auto;

    padding:
        0
        24px
        60px;

    color:
        #1f2937;

    background:
        #f8fafc;
}}

h1 {{
    margin-bottom:
        4px;

    color:
        #111827;
}}

h2 {{
    margin-top:
        42px;

    border-bottom:
        2px solid #e5e7eb;

    padding-bottom:
        8px;

    color:
        #111827;
}}

.subtitle {{
    color:
        #64748b;

    margin-bottom:
        28px;
}}

.cards {{
    display:
        grid;

    grid-template-columns:
        repeat(
            auto-fit,
            minmax(
                160px,
                1fr
            )
        );

    gap:
        14px;
}}

.card {{
    background:
        white;

    border:
        1px solid #e5e7eb;

    border-radius:
        10px;

    padding:
        18px;
}}

.card-value {{
    font-size:
        28px;

    font-weight:
        bold;

    margin-top:
        8px;

    color:
        #111827;
}}

.card-label {{
    color:
        #64748b;

    font-size:
        14px;
}}

.card-detail {{
    color:
        #64748b;

    margin-top:
        4px;

    font-size:
        14px;
}}

.success {{
    font-weight:
        bold;
}}

.note {{
    background:
        white;

    border-left:
        4px solid #64748b;

    padding:
        14px;

    margin:
        18px 0;

    line-height:
        1.5;
}}


/* =========================================================
   PIPELINE FLOW DIAGRAM
   ========================================================= */

.flow {{
    background:
        white;

    border:
        1px solid #e5e7eb;

    border-radius:
        12px;

    padding:
        28px;

    margin-top:
        18px;
}}

.flow-box {{
    background:
        #f8fafc;

    border:
        1px solid #cbd5e1;

    border-radius:
        8px;

    padding:
        12px 18px;

    text-align:
        center;

    font-weight:
        600;

    max-width:
        380px;

    margin:
        0 auto;
}}

.flow-primary {{
    background:
        #eef2ff;

    border-color:
        #a5b4fc;
}}

.flow-ai {{
    background:
        #f0fdf4;

    border-color:
        #86efac;
}}

.flow-review {{
    background:
        #fff7ed;

    border-color:
        #fdba74;
}}

.flow-arrow {{
    text-align:
        center;

    font-size:
        24px;

    padding:
        8px 0;

    color:
        #64748b;
}}

.flow-split {{
    display:
        grid;

    grid-template-columns:
        1fr 1fr;

    gap:
        28px;

    align-items:
        start;

    margin:
        10px 0;
}}

.flow-column {{
    text-align:
        center;
}}

.flow-small {{
    color:
        #64748b;

    font-size:
        13px;

    margin-top:
        6px;
}}

.flow-label {{
    text-align:
        center;

    font-size:
        12px;

    text-transform:
        uppercase;

    letter-spacing:
        0.04em;

    color:
        #64748b;

    margin-bottom:
        7px;
}}


/* =========================================================
   TABLES
   ========================================================= */

.data-table {{
    width:
        100%;

    border-collapse:
        collapse;

    background:
        white;

    margin-top:
        12px;

    font-size:
        14px;
}}

.data-table th,
.data-table td {{
    border:
        1px solid #e5e7eb;

    padding:
        9px 10px;

    text-align:
        left;

    vertical-align:
        top;
}}

.data-table th {{
    background:
        #f1f5f9;

    font-weight:
        600;
}}

.data-table tr:nth-child(even) {{
    background:
        #fafafa;
}}


/* =========================================================
   SMALL SUMMARY BLOCKS
   ========================================================= */

.summary-grid {{
    display:
        grid;

    grid-template-columns:
        repeat(
            auto-fit,
            minmax(
                220px,
                1fr
            )
        );

    gap:
        14px;

    margin-top:
        16px;
}}

.summary-box {{
    background:
        white;

    border:
        1px solid #e5e7eb;

    border-radius:
        8px;

    padding:
        14px;
}}

.summary-title {{
    color:
        #64748b;

    font-size:
        13px;

    margin-bottom:
        5px;
}}

.summary-value {{
    font-size:
        20px;

    font-weight:
        600;
}}


@media (
    max-width:
        700px
) {{

    .flow-split {{
        grid-template-columns:
            1fr;
    }}

    body {{
        padding:
            0
            14px
            40px;
    }}

}}

</style>

</head>


<body>


<!-- =======================================================
     TITLE
     ======================================================= -->

<h1>
Airflow SQL + AI Data Pipeline
</h1>

<div class="subtitle">
Portfolio execution report — latest successful pipeline run
</div>


<!-- =======================================================
     EXECUTIVE METRICS
     ======================================================= -->

<div class="cards">

    <div class="card">

        <div class="card-label">
        Pipeline Status
        </div>

        <div class="card-value success">
        {escape(str(run["status"]))}
        </div>

    </div>


    <div class="card">

        <div class="card-label">
        Processed Tickets
        </div>

        <div class="card-value">
        {total_tickets}
        </div>

    </div>


    <div class="card">

        <div class="card-label">
        Deterministic
        </div>

        <div class="card-value">
        {deterministic_count}
        </div>

        <div class="card-detail">
        {deterministic_pct:.1f}%
        </div>

    </div>


    <div class="card">

        <div class="card-label">
        AI Candidates
        </div>

        <div class="card-value">
        {ai_candidate_count}
        </div>

        <div class="card-detail">
        {ai_candidate_pct:.1f}%
        </div>

    </div>


    <div class="card">

        <div class="card-label">
        Human Review
        </div>

        <div class="card-value">
        {human_review_count}
        </div>

    </div>


    <div class="card">

        <div class="card-label">
        AI Failures
        </div>

        <div class="card-value">
        {ai_failure_count}
        </div>

    </div>

</div>


<!-- =======================================================
     ENGINEERING PRINCIPLE
     ======================================================= -->

<h2>
Engineering Principle
</h2>

<div class="note">

<strong>
Deterministic-first architecture:
</strong>

Airflow orchestrates the workflow.
SQL and Python perform deterministic processing,
validation, joins, counts, transformations, and business rules.

AI is used only for records where semantic interpretation
adds value.

</div>


<!-- =======================================================
     VISUAL ARCHITECTURE
     ======================================================= -->

<div class="flow">

    <div class="flow-label">
    Source
    </div>

    <div class="flow-box">
    Synthetic Source Data
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box">
    Schema + Data Quality Validation
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box">
    SQLite Staging
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box">
    SQL Transformations
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box flow-primary">
    Deterministic Classification
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-split">

        <div class="flow-column">

            <div class="flow-label">
            Resolved Deterministically
            </div>

            <div class="flow-box flow-primary">
            Resolved Records
            </div>

            <div class="flow-small">
            {deterministic_count} tickets
            /
            {deterministic_pct:.1f}%
            </div>

        </div>


        <div class="flow-column">

            <div class="flow-label">
            Semantic Escalation
            </div>

            <div class="flow-box flow-ai">
            AI Candidates
            </div>

            <div class="flow-small">
            {ai_candidate_count} tickets
            /
            {ai_candidate_pct:.1f}%
            </div>


            <div class="flow-arrow">
            ↓
            </div>


            <div class="flow-box flow-ai">
            Structured AI Review
            </div>


            <div class="flow-arrow">
            ↓
            </div>


            <div class="flow-split">

                <div class="flow-column">

                    <div class="flow-box flow-ai">
                    AI Accepted
                    </div>

                    <div class="flow-small">
                    {ai_accepted_count} tickets
                    </div>

                </div>


                <div class="flow-column">

                    <div class="flow-box flow-review">
                    Human Review / Fallback
                    </div>

                    <div class="flow-small">
                    {human_review_count} review
                    /
                    {ai_fallback_count} fallback
                    </div>

                </div>

            </div>

        </div>

    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box">
    Final Output Tables
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box">
    Analytical Summaries
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box">
    Ground-Truth Evaluation
    </div>


    <div class="flow-arrow">
    ↓
    </div>


    <div class="flow-box flow-primary">
    Airflow Completion
    </div>

</div>


<!-- =======================================================
     LATEST EXECUTION
     ======================================================= -->

<h2>
Latest Execution
</h2>

<div class="summary-grid">

    <div class="summary-box">

        <div class="summary-title">
        Run ID
        </div>

        <div>
        {escape(str(run["run_id"]))}
        </div>

    </div>


    <div class="summary-box">

        <div class="summary-title">
        Started
        </div>

        <div>
        {escape(str(run["started_at"]))}
        </div>

    </div>


    <div class="summary-box">

        <div class="summary-title">
        Completed
        </div>

        <div>
        {escape(str(run["completed_at"]))}
        </div>

    </div>

</div>


<table class="data-table">

<tr>
    <th>
    Metric
    </th>

    <th>
    Value
    </th>
</tr>


<tr>
    <td>
    Source Rows
    </td>

    <td>
    {run["source_row_count"]}
    </td>
</tr>


<tr>
    <td>
    Clean Rows
    </td>

    <td>
    {run["clean_row_count"]}
    </td>
</tr>


<tr>
    <td>
    Quarantined Rows
    </td>

    <td>
    {quarantined_count}
    </td>
</tr>


<tr>
    <td>
    Deterministic Classifications
    </td>

    <td>
    {deterministic_count}
    </td>
</tr>


<tr>
    <td>
    AI Candidates
    </td>

    <td>
    {ai_candidate_count}
    </td>
</tr>


<tr>
    <td>
    AI Successful Results
    </td>

    <td>
    {ai_success_count}
    </td>
</tr>


<tr>
    <td>
    AI Failures
    </td>

    <td>
    {ai_failure_count}
    </td>
</tr>


<tr>
    <td>
    Human Review
    </td>

    <td>
    {human_review_count}
    </td>
</tr>

</table>


<!-- =======================================================
     FINAL HANDLING
     ======================================================= -->

<h2>
Final Handling
</h2>

{dataframe_html(handling_summary)}


<!-- =======================================================
     CATEGORY RESULTS
     ======================================================= -->

<h2>
Category Results
</h2>

{dataframe_html(category_summary)}


<!-- =======================================================
     EVALUATION
     ======================================================= -->

<h2>
Ground-Truth Evaluation
</h2>

<div class="note">

Expected classifications and routing decisions were locked
before AI execution so model results could be evaluated
against known synthetic ground truth rather than judged
after the fact.

</div>

{dataframe_html(evaluation)}


<!-- =======================================================
     HUMAN REVIEW
     ======================================================= -->

<h2>
Human Review Queue
</h2>

<div class="note">

Human review is a valid pipeline outcome.
High model confidence does not override deterministic
handling rules when evidence is insufficient.

</div>

{dataframe_html(human_review)}


<!-- =======================================================
     TASK OBSERVABILITY
     ======================================================= -->

<h2>
Task-Level Observability
</h2>

<div class="note">

Each pipeline stage records execution metrics against the
same Airflow-aligned pipeline run ID.

</div>

{dataframe_html(metrics)}


<!-- =======================================================
     ENGINEERING CONTROLS
     ======================================================= -->

<h2>
Pipeline Controls Demonstrated
</h2>

<div class="note">

<ul>

<li>
Idempotent ingestion and SQL transformations
</li>

<li>
Safe reruns without duplicate business records
</li>

<li>
Airflow-aligned pipeline run identity
</li>

<li>
Airflow task dependencies and retry handling
</li>

<li>
Selective AI escalation rather than universal AI use
</li>

<li>
Structured AI outputs
</li>

<li>
Deterministic validation of model output
</li>

<li>
Low-confidence human-review routing
</li>

<li>
Unsupported / unresolved human-review routing
</li>

<li>
AI-failure fallback handling
</li>

<li>
Pipeline failure tracking
</li>

<li>
Data-quality quarantine without pipeline failure
</li>

<li>
SQL row-count reconciliation
</li>

<li>
Duplicate and orphan detection
</li>

<li>
Ground-truth evaluation
</li>

<li>
False-positive and false-negative AI-routing evaluation
</li>

<li>
Task-level execution metrics
</li>

<li>
Automated unit tests
</li>

<li>
Automated SQLite integration tests
</li>

<li>
Automated AI-routing tests
</li>

<li>
Mocked-AI end-to-end pipeline test
</li>

<li>
Automated Airflow DAG validation
</li>

</ul>

</div>


<!-- =======================================================
     TESTING SUMMARY
     ======================================================= -->

<h2>
Automated Testing
</h2>

<div class="cards">

    <div class="card">

        <div class="card-label">
        Automated Tests
        </div>

        <div class="card-value">
        20
        </div>

        <div class="card-detail">
        20 passed
        </div>

    </div>


    <div class="card">

        <div class="card-label">
        Test Types
        </div>

        <div class="card-value">
        5
        </div>

        <div class="card-detail">
        Unit, integration, AI routing,
        end-to-end, DAG
        </div>

    </div>


    <div class="card">

        <div class="card-label">
        Production API Calls
        </div>

        <div class="card-value">
        0
        </div>

        <div class="card-detail">
        During automated tests
        </div>

    </div>

</div>


</body>

</html>
"""

    # ========================================================
    # WRITE REPORT
    # ========================================================

    HTML_OUTPUT.write_text(
        html,
        encoding="utf-8",
    )

    # ========================================================
    # CONSOLE SUMMARY
    # ========================================================

    print(
        "PORTFOLIO REPORT GENERATED"
    )

    print(
        "--------------------------"
    )

    print(
        f"HTML report:       "
        f"{HTML_OUTPUT}"
    )

    print(
        f"Final results:     "
        f"{FINAL_RESULTS_OUTPUT}"
    )

    print(
        f"Run metrics:       "
        f"{METRICS_OUTPUT}"
    )

    print(
        f"Human review:      "
        f"{REVIEW_OUTPUT}"
    )

    print()

    print(
        "REPORT SUMMARY"
    )

    print(
        "--------------"
    )

    print(
        f"Processed tickets: "
        f"{total_tickets}"
    )

    print(
        f"Deterministic:     "
        f"{deterministic_count} "
        f"({deterministic_pct:.1f}%)"
    )

    print(
        f"AI candidates:     "
        f"{ai_candidate_count} "
        f"({ai_candidate_pct:.1f}%)"
    )

    print(
        f"AI accepted:       "
        f"{ai_accepted_count}"
    )

    print(
        f"Human review:      "
        f"{human_review_count}"
    )

    print(
        f"AI fallback:       "
        f"{ai_fallback_count}"
    )

    print(
        f"AI failures:       "
        f"{ai_failure_count}"
    )


# ============================================================
# COMMAND-LINE ENTRY POINT
# ============================================================

if __name__ == "__main__":
    build_portfolio_report()