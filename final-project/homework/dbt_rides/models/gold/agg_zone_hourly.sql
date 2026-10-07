-- gold.agg_zone_hourly — ЕТАП 2. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
--   * incremental; після будь-якого інкременту = перерахунок із fact_ride рядок у рядок
--   * подумайте: що перераховувати, коли поїздка змінила зону? Який ключ стабільний?

-- unique_key — лише requested_hour, а не (requested_hour, pickup_zone_key). Година запиту в поїздки
-- незмінна, а зона посадки — ні: запізнілий ride_started переносить поїздку в іншу зону. Тоді
-- delete+insert за (година, зона) оновив би нову зону, а стара лишилась би із застарілим лічильником.
-- Ключ-година видаляє всі рядки зачепленої години й вставляє її заново, перераховану цілком.
-- Унікальність grain (година, зона) перевіряє assert_agg_grain_unique, а гарантує унікальний
-- індекс на (requested_hour, pickup_zone_key) — він же й для пошуку за requested_hour (ведуча колонка).
{{
    config(
        materialized='incremental',
        unique_key='requested_hour',
        incremental_strategy='delete+insert',
        indexes=[{'columns': ['requested_hour', 'pickup_zone_key'], 'unique': True}],
    )
}}

WITH
{% if is_incremental() %}
touched_hours AS (
    SELECT DISTINCT requested_hour
    FROM {{ ref('fact_ride') }}
    WHERE _ingested_at > {{ high_watermark() }}
),
{% endif %}

rides AS (
    SELECT *
    FROM {{ ref('fact_ride') }}
    {% if is_incremental() %}
    WHERE requested_hour IN (SELECT requested_hour FROM touched_hours)
    {% endif %}
)

SELECT
    requested_hour,
    pickup_zone_key,
    count(*)                                                                    AS rides_requested,
    count(*) FILTER (WHERE status = 'completed')                                AS rides_completed,
    count(*) FILTER (WHERE status = 'cancelled')                                AS rides_cancelled,
    coalesce(sum(total_amount) FILTER (WHERE status = 'completed'), 0)::numeric(12, 2)
                                                                                AS gross_revenue,
    -- NULL-чайові (готівка) у сумі — як 0: sum() їх пропускає, coalesce — для години без чайових.
    coalesce(sum(tip_amount) FILTER (WHERE status = 'completed'), 0)::numeric(12, 2)
                                                                                AS tips,
    avg(wait_seconds)::numeric(10, 2)                                           AS avg_wait_seconds,
    max(_ingested_at)                                                           AS _ingested_at,
    '{{ run_started_at }}'::timestamptz                                         AS _loaded_at
FROM rides
GROUP BY requested_hour, pickup_zone_key
