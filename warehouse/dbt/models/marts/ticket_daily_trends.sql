-- Advanced SQL practice: CTEs, conditional aggregation, LAG and window SUM.
-- Rolling window is 7 observed ticket dates, NOT 7 consecutive calendar days.
with daily as (
    select
        created_date,
        count(*) as tickets,
        count(*) filter (where final_handling = 'AI_ACCEPTED') as ai_accepted,
        count(*) filter (where final_handling = 'HUMAN_REVIEW') as review_tickets
    from {{ ref('fct_ticket_outcomes') }}
    group by created_date
), windowed as (
    select
        created_date,
        tickets,
        ai_accepted,
        review_tickets,
        lag(tickets) over (order by created_date) as prior_observed_day_tickets,
        sum(tickets) over (
            order by created_date rows between 6 preceding and current row
        ) as rolling_7_observed_dates_tickets
    from daily
)
select
    created_date,
    tickets,
    ai_accepted,
    review_tickets,
    prior_observed_day_tickets,
    rolling_7_observed_dates_tickets,
    tickets - coalesce(prior_observed_day_tickets, 0) as ticket_change_from_prior_observed_date
from windowed
