-- =====================================================================
-- TASK 2 — repo_top_events (12 балів). Специфікація: ../../MODELS.md → «repo_top_events».
-- TOP-5 репозиторіїв за кількістю подій у кожному event_type: ROW_NUMBER() + QUALIFY.
-- =====================================================================
WITH agg AS (
    SELECT
        event_type            AS event_type,
        repo_name              AS repo_name,
        count(*)                AS event_count
    FROM {{ ref('stg_events') }}
    GROUP BY event_type, repo_name
)
SELECT
    event_type   AS event_type,
    repo_name    AS repo_name,
    event_count  AS event_count,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_count DESC, repo_name) AS type_rank
FROM agg
QUALIFY type_rank <= 5
