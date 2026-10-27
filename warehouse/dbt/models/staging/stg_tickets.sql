select
    ticket_id,
    created_date,
    nullif(trim(ticket_text), '') as ticket_text,
    content_sha256 as ticket_content_sha256,
    source_batch,
    source_loaded_at
from {{ source('ticket_raw', 'incoming_tickets') }}
