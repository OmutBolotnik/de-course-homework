-- gold.fact_ride — ЕТАП 2. Grain: поїздка (accumulating snapshot). SPEC.md, розділ 4.4.
--   * incremental, unique_key='ride_id', delete+insert; беріть з ref('rides') те, що змінилося
--   * FK до вимірів через LEFT JOIN + COALESCE (-1 / 'unknown'): рядок не губиться
--   * requested_date = utc_date(requested_at); requested_hour = початок години за UTC

{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        incremental_strategy='delete+insert',
    )
}}

WITH changed AS (
    SELECT *
    FROM {{ ref('rides') }}
    {% if is_incremental() %}
    -- silver.rides перебудовує поїздку з усієї історії й піднімає її _ingested_at до максимуму
    -- подій, тож «рядок змінився» = «_ingested_at вищий за вже оброблений».
    WHERE _ingested_at > {{ high_watermark() }}
    {% endif %}
)

SELECT
    r.ride_id,
    coalesce(pz.zone_key, -1)                                           AS pickup_zone_key,
    coalesce(dz.zone_key, -1)                                           AS dropoff_zone_key,
    coalesce(d.driver_key, 'unknown')                                   AS driver_key,
    r.rider_id,
    r.status,
    r.requested_at,
    r.accepted_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    r.paid_at,
    {{ utc_date('r.requested_at') }}                                    AS requested_date,
    -- date_trunc над timestamptz рахує межу години в поясі сесії — тому через UTC явно.
    date_trunc('hour', r.requested_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS requested_hour,
    r.wait_seconds,
    r.trip_seconds,
    r.distance_km,
    r.fare_amount,
    r.surge_multiplier,
    r.tolls_amount,
    r.tip_amount,
    r.total_amount,
    r.currency,
    r.payment_method,
    r.payment_amount,
    r.cancelled_by,
    r.cancel_reason,
    r.cancel_stage,
    r._ingested_at,
    '{{ run_started_at }}'::timestamptz                                 AS _loaded_at
FROM changed AS r
LEFT JOIN {{ ref('dim_zone') }}   AS pz ON pz.zone_key = r.pickup_zone_id
LEFT JOIN {{ ref('dim_zone') }}   AS dz ON dz.zone_key = r.dropoff_zone_id
LEFT JOIN {{ ref('dim_driver') }} AS d  ON d.driver_key = r.driver_id
