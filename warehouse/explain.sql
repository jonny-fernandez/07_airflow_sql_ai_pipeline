-- Run after dbt build; synthetic 50-row fixture is intentionally too small for performance claims.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT d.category, count(*) AS tickets, sum(case when final_handling = 'AI_ACCEPTED' then 1 else 0 end) AS ai_accepted
FROM mart.fct_ticket_outcomes f
LEFT JOIN mart.dim_category d
  ON coalesce(f.final_category, 'UNRESOLVED') = d.category
GROUP BY d.category ORDER BY tickets DESC;
