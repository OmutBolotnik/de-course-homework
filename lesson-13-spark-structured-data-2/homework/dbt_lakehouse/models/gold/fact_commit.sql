-- Крок 8: gold.fact_commit. Специфікація: ../../SPEC.md → «Крок 8».
-- Джерело: {{ ref('commits') }}. Грануляція не змінюється (1 рядок = 1 commit_sha).
-- FK-колонки: repo_id = md5(repo_name), pusher_id = md5(pushed_by),
--             date_id = cast(date_format(pushed_at,'yyyyMMdd') as int) — той самий вираз, що й у вимірах.
-- Колонки: commit_sha, repo_id, pusher_id, date_id, branch, is_merge_commit, is_distinct, message_length.

SELECT
    commit_sha,
    md5(repo_name)                                  AS repo_id,
    md5(pushed_by)                                  AS pusher_id,
    cast(date_format(pushed_at, 'yyyyMMdd') AS int)  AS date_id,
    branch,
    is_merge_commit,
    is_distinct,
    message_length
FROM {{ ref('commits') }}
