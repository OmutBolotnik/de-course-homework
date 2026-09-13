-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.

WITH rollup_total AS (
    SELECT sum(commits) AS total FROM {{ ref('fact_repo_activity_daily') }}
),

fact_total AS (
    SELECT count(*) AS total FROM {{ ref('fact_commit') }}
)

SELECT *
FROM rollup_total
JOIN fact_total ON rollup_total.total != fact_total.total
