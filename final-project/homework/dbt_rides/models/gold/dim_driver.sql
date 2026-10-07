-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--   * incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--   * «останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча
--   * плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{
    config(
        materialized='incremental',
        unique_key='driver_key',
        incremental_strategy='delete+insert',
    )
}}

WITH accepted AS (
    SELECT
        payload #>> '{driver,id}'                     AS driver_key,
        (payload #>> '{driver,rating}')::numeric      AS rating,
        payload #>> '{driver,vehicle,type}'           AS vehicle_type,
        payload #>> '{driver,vehicle,medallion}'      AS medallion,
        event_id,
        occurred_at,
        _ingested_at
    FROM {{ ref('events') }}
    WHERE event_type = 'ride_accepted'
      AND payload #>> '{driver,id}' IS NOT NULL
),

{% if is_incremental() %}
-- Водії, яких торкнулися нові події. Перераховуємо їх з усієї історії: запізніла подія зі
-- старим occurred_at не має затерти новіший рейтинг.
touched AS (
    SELECT DISTINCT driver_key
    FROM accepted
    WHERE _ingested_at > {{ high_watermark() }}
),
{% endif %}

history AS (
    SELECT *
    FROM accepted
    {% if is_incremental() %}
    WHERE driver_key IN (SELECT driver_key FROM touched)
    {% endif %}
),

-- «Поточні» атрибути (SCD1) — з останньої за event time події, а не з останньої за прибуттям.
latest AS (
    SELECT DISTINCT ON (driver_key)
        driver_key, vehicle_type, medallion, rating
    FROM history
    ORDER BY driver_key, occurred_at DESC, event_id DESC
),

seen AS (
    SELECT
        driver_key,
        min(occurred_at)  AS first_seen_at,
        max(occurred_at)  AS last_seen_at,
        max(_ingested_at) AS _ingested_at
    FROM history
    GROUP BY driver_key
)

SELECT
    latest.driver_key,
    latest.vehicle_type,
    latest.medallion,
    latest.rating                           AS latest_rating,
    seen.first_seen_at,
    seen.last_seen_at,
    seen._ingested_at,
    '{{ run_started_at }}'::timestamptz     AS _loaded_at
FROM latest
JOIN seen USING (driver_key)

{% if not is_incremental() %}
UNION ALL

-- Член 'unknown' — лише при побудові з нуля: delete+insert його більше не чіпає, тож він рівно
-- один після будь-якої кількості запусків, а повтор без нових даних нічого не змінює.
-- _ingested_at = NULL: max() його ігнорує, тож watermark він не піднімає.
SELECT
    'unknown',
    'unknown',
    NULL,
    NULL::numeric,
    NULL::timestamptz,
    NULL::timestamptz,
    NULL::timestamptz,
    '{{ run_started_at }}'::timestamptz
{% endif %}
