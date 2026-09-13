-- Крок 4: silver.issues. Специфікація: ../../SPEC.md → «Крок 4».
-- Джерело: {{ ref('events') }}, типи IssuesEvent ТА IssueCommentEvent (обидва несуть issue{}).
-- from_json(payload, ISSUE_SCHEMA), ISSUE_SCHEMA = var('issue_schema').
-- Грануляція: один рядок на (repo_name, issue_number) — стан з останньої за часом події.
-- Колонки: repo_name, issue_number, title, author_login, state, opened_at, closed_at,
--          comments, label_names, comment_events_seen, last_event_at, hours_to_close

WITH issue_events AS (
    SELECT
        event_id,
        event_type,
        repo_name,
        created_at AS event_at,
        from_json(payload, '{{ var("issue_schema") }}') AS parsed
    FROM {{ ref('events') }}
    WHERE event_type IN ('IssuesEvent', 'IssueCommentEvent')
),

flattened AS (
    SELECT
        event_id,
        event_type,
        repo_name,
        event_at,
        parsed.issue.number                          AS issue_number,
        parsed.issue.title                            AS title,
        parsed.issue.user.login                       AS author_login,
        parsed.issue.state                             AS state,
        to_timestamp(parsed.issue.created_at)          AS opened_at,
        to_timestamp(parsed.issue.closed_at)           AS closed_at,
        parsed.issue.comments                          AS comments,
        transform(parsed.issue.labels, l -> l.name)    AS label_names
    FROM issue_events
),

comment_counts AS (
    SELECT
        repo_name,
        issue_number,
        count(*) AS comment_events_seen
    FROM flattened
    WHERE event_type = 'IssueCommentEvent'
    GROUP BY repo_name, issue_number
),

ranked AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY repo_name, issue_number
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
    d.repo_name,
    d.issue_number,
    d.title,
    d.author_login,
    d.state,
    d.opened_at,
    d.closed_at,
    d.comments,
    d.label_names,
    coalesce(cc.comment_events_seen, 0) AS comment_events_seen,
    d.event_at                          AS last_event_at,
    CASE
        WHEN d.closed_at IS NOT NULL
        THEN (unix_timestamp(d.closed_at) - unix_timestamp(d.opened_at)) / 3600.0
    END AS hours_to_close
FROM deduped d
LEFT JOIN comment_counts cc
    ON d.repo_name = cc.repo_name AND d.issue_number = cc.issue_number

