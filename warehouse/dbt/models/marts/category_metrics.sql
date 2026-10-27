select
    coalesce(final_category, 'UNRESOLVED') as category,
    count(*) as total_tickets,
    count(*) filter (where is_deterministic) as deterministic_tickets,
    count(*) filter (where final_handling = 'AI_ACCEPTED') as ai_accepted_tickets,
    count(*) filter (where requires_human_review) as human_review_tickets
from {{ ref('fct_ticket_outcomes') }}
group by coalesce(final_category, 'UNRESOLVED')
