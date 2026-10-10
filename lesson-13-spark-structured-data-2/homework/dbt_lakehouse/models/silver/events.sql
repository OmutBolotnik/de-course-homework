{{ config(materialized='incremental', incremental_strategy='append') }}

-- Крок 1: silver.events. Специфікація: ../../SPEC.md → «Крок 1».
-- Джерело: {{ source('bronze', 'raw_events') }}. payload несемо далі сирим рядком — from_json у кроках 2–4.
-- Колонки: event_id, event_type, actor_login, repo_name, repo_owner, created_at,
--          payload, _ingested_at, _source_file
-- Фільтри: 6 типів подій; public = true (NULL відкинути); event_id/repo_name/created_at не null; дедуп по event_id.
-- incremental (append): у is_incremental()-гілці брати лише рядки з _ingested_at > max(_ingested_at) у {{ this }}.

WITH source AS (
    SELECT * 
    FROM {{ source('bronze', 'raw_events') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
    {% endif %}
),

flattened AS (
    SELECT
        id                              AS event_id,
        type                            AS event_type,
        actor.login                     AS actor_login,
        repo.name                       AS repo_name,
        split(repo.name, '/')[0]        AS repo_owner,
        to_timestamp(created_at)        AS created_at,
        public,
        payload,
        _ingested_at,
        _source_file
    FROM source
),

filtered AS (
    SELECT *,
        row_number() OVER (PARTITION BY event_id ORDER BY created_at) AS rn
    FROM flattened
    WHERE event_type IN (
            'PushEvent', 
            'PullRequestEvent', 
            'IssuesEvent',
            'IssueCommentEvent', 
            'WatchEvent', 
            'ForkEvent'
        )
        AND public IS NOT NULL AND public = true
        AND event_id IS NOT NULL
        AND repo_name IS NOT NULL
        AND created_at IS NOT NULL
),

deduped AS (
    SELECT *
    FROM filtered
    WHERE rn = 1
)

SELECT
    event_id, 
    event_type, 
    actor_login, 
    repo_name, 
    repo_owner,
    created_at, 
    payload, 
    _ingested_at, 
    _source_file
FROM deduped
