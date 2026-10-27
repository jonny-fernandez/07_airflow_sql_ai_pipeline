select
    t.ticket_id,
    t.created_date,
    t.ticket_text,
    o.final_category,
    o.sentiment,
    o.summary,
    o.classification_source,
    o.ai_used,
    o.final_handling,
    t.source_batch,
    t.ticket_content_sha256,
    o.outcome_content_sha256,
    greatest(t.source_loaded_at, o.outcome_loaded_at) as source_loaded_at,
    (o.final_handling = 'DETERMINISTIC') as is_deterministic,
    (o.final_handling = 'HUMAN_REVIEW') as requires_human_review
from {{ ref('stg_tickets') }} t
inner join {{ ref('stg_ticket_outcomes') }} o on t.ticket_id = o.ticket_id
