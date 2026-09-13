-- Крок 6: gold.dim_actor. Специфікація: ../../SPEC.md → «Крок 6».
-- Джерело: {{ ref('events') }}, actor_login is not null. Грануляція: один рядок на актора.
-- Колонки: actor_id (md5(actor_login)), actor_login, is_bot (закінчується на [bot]),
--          first_seen_at, last_seen_at, event_count, distinct_repos.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
SELECT
    md5(actor_login)                                    AS actor_id,
    actor_login,
    actor_login LIKE '%[bot]'                            AS is_bot,
    min(created_at)                                      AS first_seen_at,
    max(created_at)                                      AS last_seen_at,
    count(*)                                              AS event_count,
    count(DISTINCT repo_name)                             AS distinct_repos
FROM {{ ref('events') }}
WHERE actor_login IS NOT NULL
GROUP BY actor_login
