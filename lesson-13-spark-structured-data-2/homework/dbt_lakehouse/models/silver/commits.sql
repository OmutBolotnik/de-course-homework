-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».
-- Джерело: {{ ref('events') }}, лише PushEvent.
-- from_json(payload, PUSH_SCHEMA) → explode масиву commits → commit grain. PUSH_SCHEMA = var('push_schema').
-- Дедуп: один рядок на commit_sha, найраніший pushed_at.
-- Колонки: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,
--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length
-- Пастка: `distinct` — reserved word, у DDL-схемі та доступі до поля потрібні backticks.

WITH push_events AS (
    SELECT
        event_id,
        repo_name,
        actor_login AS pushed_by,
        created_at AS pushed_at,
        from_json(payload, '{{ var("push_schema") }}') AS parsed
    FROM {{ ref('events') }}
    WHERE event_type = 'PushEvent'
),

exploded AS (
    SELECT
        event_id,
        repo_name,
        pushed_by,
        pushed_at,
        regexp_replace(parsed.ref, '^refs/heads/', '') AS branch,
        c.sha                                          AS commit_sha,
        c.message                                      AS message,
        c.`distinct`                                    AS is_distinct,
        c.author.name                                  AS author_name,
        c.author.email                                 AS author_email
    FROM push_events
    LATERAL VIEW explode(parsed.commits) AS c
),

ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY commit_sha ORDER BY pushed_at, event_id) AS rn
    FROM exploded
),

deduped AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
)

SELECT
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    message LIKE 'Merge %'          AS is_merge_commit,
    split(message, '\n')[0]         AS message_subject,
    length(message)                 AS message_length
FROM deduped
