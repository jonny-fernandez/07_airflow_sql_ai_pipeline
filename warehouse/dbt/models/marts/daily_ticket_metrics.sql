select
    created_date,
    count(*) as total_tickets,
    count(*) filter (where is_deterministic) as deterministic_tickets,
    count(*) filter (where final_handling = 'AI_ACCEPTED') as ai_accepted_tickets,
    count(*) filter (where requires_human_review) as human_review_tickets,
    round(100.0 * count(*) filter (where is_deterministic) / nullif(count(*), 0), 2) as deterministic_rate_pct
from {{ ref('fct_ticket_outcomes') }}
group by created_date
