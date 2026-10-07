-- gold.dim_zone — ЕТАП 2. Grain: зона. SPEC.md, розділ 4.4.
--   * table; джерело — ref('seed_taxi_zone'); порожні значення -> 'Unknown'; плюс член zone_key = -1

{{ config(materialized='table') }}

SELECT
    location_id::int                                          AS zone_key,
    coalesce(nullif(trim(borough), ''), 'Unknown')            AS borough,
    coalesce(nullif(trim(zone_name), ''), 'Unknown')          AS zone_name,
    coalesce(nullif(trim(service_zone), ''), 'Unknown')       AS service_zone
FROM {{ ref('seed_taxi_zone') }}

UNION ALL

-- Член «Unknown»: сюди складаються поїздки, чия зона відсутня в довіднику (FK не губить рядок).
SELECT -1, 'Unknown', 'Unknown', 'Unknown'
