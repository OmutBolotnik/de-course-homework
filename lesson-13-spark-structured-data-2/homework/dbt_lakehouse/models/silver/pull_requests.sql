-- Крок 3: silver.pull_requests. Специфікація: ../../SPEC.md → «Крок 3».
-- Джерело: {{ ref('events') }}, лише PullRequestEvent. from_json(payload, PR_SCHEMA), PR_SCHEMA = var('pr_schema').
-- Грануляція: один рядок на (repo_name, pr_number) — стан з ОСТАННЬОЇ за часом події (row_number desc).
-- Колонки: repo_name, pr_number, title, author_login, state, is_merged, is_draft, opened_at,
--          closed_at, merged_at, additions, deletions, changed_files, commits_count, comments,
--          review_comments, author_association, label_names, last_action, last_event_at, churn, hours_open

WITH pr_events AS (
    SELECT
        event_id,
        repo_name,
        created_at AS event_at,
        from_json(payload, '{{ var("pr_schema") }}') AS parsed
    FROM {{ ref('events') }}
    WHERE event_type = 'PullRequestEvent'
),

flattened AS (
    SELECT
        event_id,
        repo_name,
        event_at,
        parsed.action                              AS last_action,
        parsed.number                              AS pr_number,
        parsed.pull_request.title                  AS title,
        parsed.pull_request.user.login             AS author_login,
        parsed.pull_request.state                  AS state,
        parsed.pull_request.merged                 AS is_merged,
        parsed.pull_request.draft                  AS is_draft,
        to_timestamp(parsed.pull_request.created_at) AS opened_at,
        to_timestamp(parsed.pull_request.closed_at)  AS closed_at,
        to_timestamp(parsed.pull_request.merged_at)  AS merged_at,
        parsed.pull_request.additions              AS additions,
        parsed.pull_request.deletions               AS deletions,
        parsed.pull_request.changed_files          AS changed_files,
        parsed.pull_request.commits                AS commits_count,
        parsed.pull_request.comments               AS comments,
        parsed.pull_request.review_comments        AS review_comments,
        parsed.pull_request.author_association     AS author_association,
        transform(parsed.pull_request.labels, l -> l.name) AS label_names
    FROM pr_events
),

ranked AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY repo_name, pr_number
            ORDER BY event_at DESC, event_id DESC
        ) AS rn
    FROM flattened
),

deduped AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
)

SELECT
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    event_at AS last_event_at,
    additions + deletions AS churn,
    (unix_timestamp(coalesce(closed_at, event_at)) - unix_timestamp(opened_at)) / 3600.0 AS hours_open
FROM deduped
