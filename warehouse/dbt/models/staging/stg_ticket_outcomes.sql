select
    ticket_id,
    nullif(trim(final_category), '') as final_category,
    nullif(trim(sentiment), '') as sentiment,
    nullif(trim(summary), '') as summary,
    classification_source,
    ai_used,
    final_handling,
    content_sha256 as outcome_content_sha256,
    source_loaded_at as outcome_loaded_at
from {{ source('ticket_raw', 'ticket_outcomes') }}
