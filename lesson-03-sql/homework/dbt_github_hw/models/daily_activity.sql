-- =====================================================================
-- TASK 3 — daily_activity (12 балів). Специфікація: ../../MODELS.md → «daily_activity».
-- Кількість подій по днях + накопичувальний підсумок: SUM(...) OVER (ORDER BY ...).
-- =====================================================================
WITH agg AS (
    SELECT
        event_date  AS event_date,
        count(*)    AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
)
SELECT
    event_date AS event_date,
    events     AS events,
    SUM(events) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_events
FROM agg
