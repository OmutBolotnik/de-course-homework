-- Крок 5: gold.dim_repo. Специфікація: ../../SPEC.md → «Крок 5».
-- Джерело: {{ ref('events') }}. Грануляція: один рядок на репозиторій.
-- Колонки: repo_id (md5(repo_name)), repo_name, repo_owner, first_seen_at, last_seen_at,
--          event_count, is_forked (є хоч одна подія ForkEvent по цьому репо).

-- TODO: замініть заглушку на запит згідно зі SPEC.md
SELECT
    md5(repo_name)                                              AS repo_id,
    repo_name,
    repo_owner,
    min(created_at)                                             AS first_seen_at,
    max(created_at)                                             AS last_seen_at,
    count(*)                                                    AS event_count,
    bool_or(event_type = 'ForkEvent')                           AS is_forked
FROM {{ ref('events') }}
GROUP BY repo_name, repo_owner