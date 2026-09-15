INSERT INTO ai_candidate (
    ticket_id,
    evidence_text,
    candidate_reason,
    input_hash,
    created_at
)
SELECT
    tc.ticket_id,
    tc.ticket_text,
    'NO_HIGH_CONFIDENCE_DETERMINISTIC_RULE',
    tc.source_hash,
    :created_at

FROM ticket_clean AS tc

LEFT JOIN deterministic_classification AS dc
    ON tc.ticket_id = dc.ticket_id

WHERE dc.ticket_id IS NULL;
