-- silver.events — ЕТАП 2. Grain: одна подія (event_id). SPEC.md, розділ 4.2.
--   * incremental, unique_key='event_id', delete+insert; межа — high_watermark() за _ingested_at
--   * відкинути source='loadtest' і рядки без occurred_at / ride_id
--   * один рядок на event_id (найраніший _ingested_at, за рівності — _source_file)
--   * payload text -> jsonb; occurred_date = utc_date(occurred_at); _loaded_at = run_started_at

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['event_id'], 'unique': True},
            {'columns': ['ride_id']},
            {'columns': ['_ingested_at']},
        ],
    )
}}

WITH candidates AS (
    SELECT *
    FROM {{ source('bronze', 'raw_events') }} AS b
    WHERE b.source IS DISTINCT FROM 'loadtest'
      AND b.occurred_at IS NOT NULL
      AND b.ride_id IS NOT NULL
    {% if is_incremental() %}
      -- Межа — час ingestion, а не occurred_at: late event має старий event time.
      AND b._ingested_at > {{ high_watermark() }}
      -- Дублікат, що приїхав пізнішим запуском, пропускаємо: вміст події незмінний, а так
      -- лишається найраніший рядок (як і при --full-refresh) і поїздку не перебудовуємо дарма.
      AND NOT EXISTS (SELECT 1 FROM {{ this }} AS t WHERE t.event_id = b.event_id)
    {% endif %}
),

ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY event_id ORDER BY _ingested_at, _source_file) AS rn
    FROM candidates
)

SELECT
    event_id,
    event_type,
    ride_id,
    source,
    occurred_at,
    {{ utc_date('occurred_at') }}               AS occurred_date,
    payload::jsonb                              AS payload,
    _source_file,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz         AS _loaded_at
FROM ranked
WHERE rn = 1
