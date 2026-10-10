-- Крок 10: gold.fact_repo_activity_daily. Специфікація: ../../SPEC.md → «Крок 10».
-- Грануляція: (repo_id, date_id). Багатоджерельний rollup з {{ ref('commits') }},
-- {{ ref('pull_requests') }}, {{ ref('issues') }} та {{ ref('events') }} (WatchEvent/ForkEvent).
-- Патерн: денний агрегат на джерело (метрика + нулі для решти) → union all → group by.
-- Відсутні метрики → 0, не NULL. Порядок і типи колонок у всіх CTE мають збігатися.
-- Колонки: activity_id (md5(concat_ws('|', repo_id, date_id))), repo_id, date_id, commits,
--          distinct_committers, prs_opened, prs_merged, issues_opened, issues_closed, stars, forks.

WITH commits_daily AS (
    SELECT
        md5(repo_name)                                    AS repo_id,
        cast(date_format(pushed_at, 'yyyyMMdd') AS int)   AS date_id,
        count(*)                                          AS commits,
        count(DISTINCT author_email)                      AS distinct_committers,
        0                                                  AS prs_opened,
        0                                                  AS prs_merged,
        0                                                  AS issues_opened,
        0                                                  AS issues_closed,
        0                                                  AS stars,
        0                                                  AS forks
    FROM {{ ref('commits') }}
    GROUP BY repo_name, date_format(pushed_at, 'yyyyMMdd')
),

prs_opened_daily AS (
    SELECT
        md5(repo_name)                                    AS repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') AS int)   AS date_id,
        0                                                  AS commits,
        0                                                  AS distinct_committers,
        count(*)                                          AS prs_opened,
        0                                                  AS prs_merged,
        0                                                  AS issues_opened,
        0                                                  AS issues_closed,
        0                                                  AS stars,
        0                                                  AS forks
    FROM {{ ref('pull_requests') }}
    GROUP BY repo_name, date_format(opened_at, 'yyyyMMdd')
),

prs_merged_daily AS (
    SELECT
        md5(repo_name)                                    AS repo_id,
        cast(date_format(merged_at, 'yyyyMMdd') AS int)   AS date_id,
        0                                                  AS commits,
        0                                                  AS distinct_committers,
        0                                                  AS prs_opened,
        count(*)                                          AS prs_merged,
        0                                                  AS issues_opened,
        0                                                  AS issues_closed,
        0                                                  AS stars,
        0                                                  AS forks
    FROM {{ ref('pull_requests') }}
    WHERE merged_at IS NOT NULL
    GROUP BY repo_name, date_format(merged_at, 'yyyyMMdd')
),

issues_opened_daily AS (
    SELECT
        md5(repo_name)                                    AS repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') AS int)   AS date_id,
        0                                                  AS commits,
        0                                                  AS distinct_committers,
        0                                                  AS prs_opened,
        0                                                  AS prs_merged,
        count(*)                                          AS issues_opened,
        0                                                  AS issues_closed,
        0                                                  AS stars,
        0                                                  AS forks
    FROM {{ ref('issues') }}
    GROUP BY repo_name, date_format(opened_at, 'yyyyMMdd')
),

issues_closed_daily AS (
    SELECT
        md5(repo_name)                                    AS repo_id,
        cast(date_format(closed_at, 'yyyyMMdd') AS int)   AS date_id,
        0                                                  AS commits,
        0                                                  AS distinct_committers,
        0                                                  AS prs_opened,
        0                                                  AS prs_merged,
        0                                                  AS issues_opened,
        count(*)                                          AS issues_closed,
        0                                                  AS stars,
        0                                                  AS forks
    FROM {{ ref('issues') }}
    WHERE closed_at IS NOT NULL
    GROUP BY repo_name, date_format(closed_at, 'yyyyMMdd')
),

stars_forks_daily AS (
    SELECT
        md5(repo_name)                                    AS repo_id,
        cast(date_format(created_at, 'yyyyMMdd') AS int)  AS date_id,
        0                                                  AS commits,
        0                                                  AS distinct_committers,
        0                                                  AS prs_opened,
        0                                                  AS prs_merged,
        0                                                  AS issues_opened,
        0                                                  AS issues_closed,
        count(CASE WHEN event_type = 'WatchEvent' THEN 1 END) AS stars,
        count(CASE WHEN event_type = 'ForkEvent' THEN 1 END)  AS forks
    FROM {{ ref('events') }}
    WHERE event_type IN ('WatchEvent', 'ForkEvent')
    GROUP BY repo_name, date_format(created_at, 'yyyyMMdd')
),

unioned AS (
    SELECT * FROM commits_daily
    UNION ALL
    SELECT * FROM prs_opened_daily
    UNION ALL
    SELECT * FROM prs_merged_daily
    UNION ALL
    SELECT * FROM issues_opened_daily
    UNION ALL
    SELECT * FROM issues_closed_daily
    UNION ALL
    SELECT * FROM stars_forks_daily
)

SELECT
    md5(concat_ws('|', repo_id, cast(date_id AS string))) AS activity_id,
    repo_id,
    date_id,
    sum(commits)             AS commits,
    sum(distinct_committers) AS distinct_committers,
    sum(prs_opened)          AS prs_opened,
    sum(prs_merged)          AS prs_merged,
    sum(issues_opened)       AS issues_opened,
    sum(issues_closed)       AS issues_closed,
    sum(stars)               AS stars,
    sum(forks)               AS forks
FROM unioned
GROUP BY repo_id, date_id
