-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».
-- Згенерований безперервний календар (БЕЗ seed): explode(sequence(min, max, interval 1 day)).
-- Межі min/max — підзапитом по фактичних датах з {{ ref('commits') }}, {{ ref('pull_requests') }},
-- {{ ref('issues') }} (pushed_at / opened_at / merged_at / closed_at). Не хардкодьте.
-- Колонки: date_id (int yyyyMMdd), date_day (date), day_of_week, is_weekend, iso_week, year.

WITH all_dates AS (
    SELECT to_date(pushed_at) AS d FROM {{ ref('commits') }}
    UNION ALL
    SELECT to_date(opened_at) FROM {{ ref('pull_requests') }}
    UNION ALL
    SELECT to_date(merged_at) FROM {{ ref('pull_requests') }} WHERE merged_at IS NOT NULL
    UNION ALL
    SELECT to_date(opened_at) FROM {{ ref('issues') }}
    UNION ALL
    SELECT to_date(closed_at) FROM {{ ref('issues') }} WHERE closed_at IS NOT NULL
),

bounds AS (
    SELECT min(d) AS min_date, max(d) AS max_date
    FROM all_dates
),

calendar AS (
    SELECT explode(sequence(min_date, max_date, interval 1 day)) AS date_day
    FROM bounds
)

SELECT
    cast(date_format(date_day, 'yyyyMMdd') AS int) AS date_id,
    date_day,
    ((dayofweek(date_day)+5) % 7) + 1              AS day_of_week,
    ((dayofweek(date_day)+5) % 7) + 1 IN (6, 7)    AS is_weekend,
    weekofyear(date_day)                           AS iso_week,
    year(date_day)                                 AS year
FROM calendar
