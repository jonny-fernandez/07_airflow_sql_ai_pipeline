select
    count(*) as total_tickets,
    count(*) filter (where is_deterministic) as deterministic_tickets,
    count(*) filter (where ai_used) as ai_candidate_tickets,
    count(*) filter (where final_handling = 'AI_ACCEPTED') as ai_accepted_tickets,
    count(*) filter (where requires_human_review) as human_review_tickets,
    round(100.0 * count(*) filter (where is_deterministic) / nullif(count(*), 0), 2) as deterministic_rate_pct,
    round(100.0 * count(*) filter (where ai_used) / nullif(count(*), 0), 2) as ai_candidate_rate_pct
from {{ ref('fct_ticket_outcomes') }}
