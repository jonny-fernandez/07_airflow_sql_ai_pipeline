{{ config(materialized='incremental', unique_key='ticket_id', incremental_strategy='delete+insert') }}
select
    md5(cast(ticket_id as varchar)) as ticket_sk,
    ticket_id,
    created_date,
    final_category,
    classification_source,
    ai_used,
    final_handling,
    is_deterministic,
    requires_human_review,
    source_batch,
    source_loaded_at
from {{ ref('int_ticket_resolution') }}
{% if is_incremental() %}
where source_loaded_at >= (select coalesce(max(source_loaded_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
