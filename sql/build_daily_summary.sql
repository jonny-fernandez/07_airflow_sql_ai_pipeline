INSERT INTO daily_ticket_summary (
    summary_date,
    category,
    ticket_count,
    deterministic_count,
    ai_enriched_count,
    human_review_count,
    ai_failure_fallback_count,
    refreshed_at
)
SELECT
    tc.created_date AS summary_date,

    CASE
        WHEN ftc.final_handling = 'HUMAN_REVIEW'
            THEN 'HUMAN_REVIEW'

        WHEN ftc.final_handling = 'AI_FAILURE_FALLBACK'
            THEN 'AI_FAILURE_FALLBACK'

        ELSE COALESCE(
            ftc.final_category,
            'UNCLASSIFIED'
        )
    END AS category,

    COUNT(*) AS ticket_count,

    SUM(
        CASE
            WHEN ftc.final_handling = 'DETERMINISTIC'
            THEN 1
            ELSE 0
        END
    ) AS deterministic_count,

    SUM(
        CASE
            WHEN ftc.final_handling = 'AI_ACCEPTED'
            THEN 1
            ELSE 0
        END
    ) AS ai_enriched_count,

    SUM(
        CASE
            WHEN ftc.final_handling = 'HUMAN_REVIEW'
            THEN 1
            ELSE 0
        END
    ) AS human_review_count,

    SUM(
        CASE
            WHEN ftc.final_handling = 'AI_FAILURE_FALLBACK'
            THEN 1
            ELSE 0
        END
    ) AS ai_failure_fallback_count,

    :refreshed_at

FROM ticket_clean AS tc

INNER JOIN final_ticket_classification AS ftc
    ON tc.ticket_id = ftc.ticket_id

GROUP BY
    tc.created_date,

    CASE
        WHEN ftc.final_handling = 'HUMAN_REVIEW'
            THEN 'HUMAN_REVIEW'

        WHEN ftc.final_handling = 'AI_FAILURE_FALLBACK'
            THEN 'AI_FAILURE_FALLBACK'

        ELSE COALESCE(
            ftc.final_category,
            'UNCLASSIFIED'
        )
    END

ON CONFLICT (
    summary_date,
    category
)
DO UPDATE SET
    ticket_count =
        excluded.ticket_count,

    deterministic_count =
        excluded.deterministic_count,

    ai_enriched_count =
        excluded.ai_enriched_count,

    human_review_count =
        excluded.human_review_count,

    ai_failure_fallback_count =
        excluded.ai_failure_fallback_count,

    refreshed_at =
        excluded.refreshed_at;
