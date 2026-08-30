{{ config(materialized='view') }}
-- =====================================================================
-- TASK 1 — stg_events (12 балів). Специфікація: ../../MODELS.md → «stg_events».
-- Прочитати партиційований Parquet і застосувати DQ-фільтри (типи, боти, порожні push).
-- =====================================================================
SELECT
    id                    AS id,
    event_type            AS event_type,
    created_at            AS created_at,
    event_date            AS event_date,
    actor_login           AS actor_login,
    repo_name             AS repo_name,
    payload_commit_count  AS payload_commit_count,
    payload_action        AS payload_action,
    payload_ref           AS payload_ref
FROM read_parquet('{{ var("events_path") }}', hive_partitioning = true)
WHERE event_type IN ('PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'IssueCommentEvent')
  AND actor_login NOT LIKE '%[bot]'
  AND NOT (event_type = 'PushEvent' AND payload_commit_count = 0)
