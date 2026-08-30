-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
-- =====================================================================
WITH agg AS (
    SELECT
        event_date  AS event_date,
        count(*)    AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
)
SELECT
    event_date                                        AS event_date,
    events                                             AS events,
    LAG(events) OVER (ORDER BY event_date)             AS prev_day_events,
    events - LAG(events) OVER (ORDER BY event_date)    AS delta_events
FROM agg
