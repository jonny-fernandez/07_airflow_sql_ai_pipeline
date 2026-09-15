INSERT INTO ticket_clean (
    ticket_id,
    created_date,
    ticket_text,
    normalized_text,
    source_hash,
    cleaned_at
)
SELECT
    ticket_id,
    created_date,
    ticket_text,
    LOWER(TRIM(ticket_text)) AS normalized_text,
    source_hash,
    :cleaned_at
FROM stg_raw_tickets
WHERE 1 = 1

ON CONFLICT(ticket_id)
DO UPDATE SET
    created_date = excluded.created_date,
    ticket_text = excluded.ticket_text,
    normalized_text = excluded.normalized_text,
    source_hash = excluded.source_hash,
    cleaned_at = excluded.cleaned_at

WHERE
    ticket_clean.source_hash <> excluded.source_hash;
