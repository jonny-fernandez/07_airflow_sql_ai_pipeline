select distinct
    coalesce(final_category, 'UNRESOLVED') as category,
    md5(coalesce(final_category, 'UNRESOLVED')) as category_sk
from {{ ref('int_ticket_resolution') }}
