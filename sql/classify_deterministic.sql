INSERT INTO deterministic_classification (
    ticket_id,
    category,
    rule_id,
    evidence,
    classified_at
)
SELECT
    ticket_id,

    CASE
        WHEN normalized_text LIKE '%paycheck%'
          OR normalized_text LIKE '%direct deposit%'
            THEN 'PAYROLL'

        WHEN normalized_text LIKE '%log into%'
          OR normalized_text LIKE '%password reset%'
            THEN 'IT_SUPPORT'

        WHEN normalized_text LIKE '%course%'
          OR normalized_text LIKE '%training certificate%'
          OR normalized_text LIKE '%required training%'
            THEN 'TRAINING'

        WHEN normalized_text LIKE '%onboarding documents%'
          OR normalized_text LIKE '%onboarding forms%'
            THEN 'ONBOARDING'
    END AS category,

    CASE
        WHEN normalized_text LIKE '%paycheck%'
          OR normalized_text LIKE '%direct deposit%'
            THEN 'RULE_PAYROLL_001'

        WHEN normalized_text LIKE '%log into%'
          OR normalized_text LIKE '%password reset%'
            THEN 'RULE_IT_SUPPORT_001'

        WHEN normalized_text LIKE '%course%'
          OR normalized_text LIKE '%training certificate%'
          OR normalized_text LIKE '%required training%'
            THEN 'RULE_TRAINING_001'

        WHEN normalized_text LIKE '%onboarding documents%'
          OR normalized_text LIKE '%onboarding forms%'
            THEN 'RULE_ONBOARDING_001'
    END AS rule_id,

    CASE
        WHEN normalized_text LIKE '%paycheck%'
          OR normalized_text LIKE '%direct deposit%'
            THEN 'Matched high-confidence payroll terminology.'

        WHEN normalized_text LIKE '%log into%'
          OR normalized_text LIKE '%password reset%'
            THEN 'Matched high-confidence access/authentication terminology.'

        WHEN normalized_text LIKE '%course%'
          OR normalized_text LIKE '%training certificate%'
          OR normalized_text LIKE '%required training%'
            THEN 'Matched high-confidence training terminology.'

        WHEN normalized_text LIKE '%onboarding documents%'
          OR normalized_text LIKE '%onboarding forms%'
            THEN 'Matched high-confidence onboarding terminology.'
    END AS evidence,

    :classified_at

FROM ticket_clean

WHERE
       normalized_text LIKE '%paycheck%'
    OR normalized_text LIKE '%direct deposit%'
    OR normalized_text LIKE '%log into%'
    OR normalized_text LIKE '%password reset%'
    OR normalized_text LIKE '%course%'
    OR normalized_text LIKE '%training certificate%'
    OR normalized_text LIKE '%required training%'
    OR normalized_text LIKE '%onboarding documents%'
    OR normalized_text LIKE '%onboarding forms%';
