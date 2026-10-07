-- silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--   * incremental, unique_key='ride_id', delete+insert
--   * перебудовуйте поїздки, яких торкнулися НОВІ події, з УСІЄЇ їхньої історії в ref('events')
--   * поїздка існує, коли прийшла її ride_requested; порядок прибуття подій байдужий
--   * status, фактична зона (перекриває заявлену), wait_seconds, trip_seconds, суми з ride_completed
--   * _ingested_at = максимум _ingested_at усіх подій поїздки

{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        incremental_strategy='delete+insert',
    )
}}

WITH
{% if is_incremental() %}
-- Поїздки, яких торкнулися нові події. Самі нові події — лише «сигнал»: поїздку далі збираємо
-- з усієї її історії, бо запізнілий payment_captured має дописатися до вже збудованого рядка.
touched AS (
    SELECT DISTINCT ride_id
    FROM {{ ref('events') }}
    WHERE _ingested_at > {{ high_watermark() }}
),
{% endif %}

ride_events AS (
    SELECT e.ride_id, e.event_id, e.event_type, e.occurred_at, e.payload, e._ingested_at
    FROM {{ ref('events') }} AS e
    {% if is_incremental() %}
    WHERE e.ride_id IN (SELECT ride_id FROM touched)
    {% endif %}
),

-- Одна подія кожного типу на поїздку (детерміновано: найраніша за occurred_at), щоб згортка
-- не залежала від порядку прибуття.
per_type AS (
    SELECT DISTINCT ON (ride_id, event_type)
        ride_id, event_type, occurred_at, payload
    FROM ride_events
    ORDER BY ride_id, event_type, occurred_at, event_id
),

req AS (SELECT * FROM per_type WHERE event_type = 'ride_requested'),
acc AS (SELECT * FROM per_type WHERE event_type = 'ride_accepted'),
sta AS (SELECT * FROM per_type WHERE event_type = 'ride_started'),
com AS (SELECT * FROM per_type WHERE event_type = 'ride_completed'),
can AS (SELECT * FROM per_type WHERE event_type = 'ride_cancelled'),
pay AS (SELECT * FROM per_type WHERE event_type = 'payment_captured'),

ingested AS (
    SELECT ride_id, max(_ingested_at) AS _ingested_at
    FROM ride_events
    GROUP BY ride_id
)

SELECT
    req.ride_id,
    req.payload #>> '{rider,id}'                                    AS rider_id,
    req.payload #>> '{rider,platform}'                              AS rider_platform,
    req.payload #>> '{rider,app_version}'                           AS app_version,
    req.payload ->> 'requested_vehicle'                             AS requested_vehicle,
    coalesce(
        acc.payload #>> '{driver,id}',
        sta.payload #>> '{driver,id}',
        com.payload #>> '{driver,id}'
    )                                                               AS driver_id,
    CASE
        WHEN com.ride_id IS NOT NULL THEN 'completed'
        WHEN can.ride_id IS NOT NULL THEN 'cancelled'
        WHEN sta.ride_id IS NOT NULL THEN 'in_progress'
        WHEN acc.ride_id IS NOT NULL THEN 'accepted'
        ELSE 'requested'
    END                                                             AS status,

    req.occurred_at                                                 AS requested_at,
    acc.occurred_at                                                 AS accepted_at,
    sta.occurred_at                                                 AS started_at,
    com.occurred_at                                                 AS completed_at,
    can.occurred_at                                                 AS cancelled_at,
    pay.occurred_at                                                 AS paid_at,

    -- Те, що СТАЛОСЯ, перекриває те, що ЗАМОВИЛИ.
    coalesce(
        (sta.payload #>> '{pickup,zone_id}')::int,
        (req.payload #>> '{pickup,zone_id}')::int
    )                                                               AS pickup_zone_id,
    coalesce(
        (com.payload #>> '{dropoff,zone_id}')::int,
        (req.payload #>> '{dropoff,zone_id}')::int
    )                                                               AS dropoff_zone_id,

    extract(epoch FROM acc.occurred_at - req.occurred_at)::int      AS wait_seconds,
    extract(epoch FROM com.occurred_at - sta.occurred_at)::int      AS trip_seconds,

    (req.payload ->> 'surge_estimate')::numeric                     AS surge_estimate,
    (com.payload ->> 'distance_km')::numeric(8, 2)                  AS distance_km,
    -- Фактичний множник із завершення, а не оцінка із замовлення.
    (com.payload #>> '{fare,surge_multiplier}')::numeric(4, 2)      AS surge_multiplier,
    (com.payload #>> '{fare,amount}')::numeric(10, 2)               AS fare_amount,
    (com.payload #>> '{fare,tolls}')::numeric(10, 2)                AS tolls_amount,
    -- Готівка: fare.tip = null лишається NULL (не 0).
    (com.payload #>> '{fare,tip}')::numeric(10, 2)                  AS tip_amount,
    (com.payload #>> '{fare,total}')::numeric(10, 2)                AS total_amount,
    com.payload #>> '{fare,currency}'                               AS currency,

    can.payload ->> 'cancelled_by'                                  AS cancelled_by,
    can.payload ->> 'reason'                                        AS cancel_reason,
    can.payload ->> 'stage'                                         AS cancel_stage,

    pay.payload #>> '{payment,method}'                              AS payment_method,
    pay.payload #>> '{payment,psp_reference}'                       AS psp_reference,
    (pay.payload #>> '{payment,amount}')::numeric(10, 2)            AS payment_amount,

    ingested._ingested_at,
    '{{ run_started_at }}'::timestamptz                             AS _loaded_at
-- Поїздка існує лише тоді, коли прийшла її ride_requested; решта подій чекає в silver.events.
FROM req
LEFT JOIN acc USING (ride_id)
LEFT JOIN sta USING (ride_id)
LEFT JOIN com USING (ride_id)
LEFT JOIN can USING (ride_id)
LEFT JOIN pay USING (ride_id)
JOIN ingested USING (ride_id)
