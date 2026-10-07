{{ config(tags=['reconcile']) }}
-- Поточні атрибути водія = атрибути з його НАЙПІЗНІШОГО за occurred_at ride_accepted, і кожен
-- водій із silver.events є у вимірі (і навпаки). Перераховуємо очікуване напряму з silver.events,
-- окремо від логіки моделі. Ловить «останнє за прибуттям / з нового батча» замість «останнє
-- за event time»: запізніла подія зі старим occurred_at затерла б новіший рейтинг.
-- Посилається на Silver і Gold, тому тег reconcile: під cautious інакше не запуститься ніколи.
WITH accepted AS (
    SELECT
        payload #>> '{driver,id}'                   AS driver_key,
        (payload #>> '{driver,rating}')::numeric    AS rating,
        payload #>> '{driver,vehicle,type}'         AS vehicle_type,
        payload #>> '{driver,vehicle,medallion}'    AS medallion,
        -- Нічия за occurred_at розв'язується за event_id — так само, як у моделі; інакше дві події
        -- з однаковим часом дали б два «очікувані» рядки й хибне падіння.
        row_number() OVER (
            PARTITION BY payload #>> '{driver,id}'
            ORDER BY occurred_at DESC, event_id DESC
        ) AS rn
    FROM {{ ref('events') }}
    WHERE event_type = 'ride_accepted'
),

expected AS (
    SELECT driver_key, rating, vehicle_type, medallion
    FROM accepted
    WHERE rn = 1
),

actual AS (
    SELECT driver_key, latest_rating, vehicle_type, medallion
    FROM {{ ref('dim_driver') }}
    WHERE driver_key <> 'unknown'
)

SELECT
    coalesce(e.driver_key, a.driver_key) AS driver_key,
    e.rating        AS expected_rating,
    a.latest_rating AS actual_rating,
    e.vehicle_type  AS expected_vehicle_type,
    a.vehicle_type  AS actual_vehicle_type
FROM expected AS e
FULL OUTER JOIN actual AS a USING (driver_key)
WHERE e.driver_key IS NULL
   OR a.driver_key IS NULL
   OR e.rating       IS DISTINCT FROM a.latest_rating
   OR e.vehicle_type IS DISTINCT FROM a.vehicle_type
   OR e.medallion    IS DISTINCT FROM a.medallion
