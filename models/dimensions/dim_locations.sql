{{
  config(
    materialized='table',
    schema='dimensions',
    tags=['dimension', 'instagram', 'location']
  )
}}

SELECT
  FARM_FINGERPRINT(location_name) AS location_id,
  location_name,
  MIN(created_at) AS first_seen_at,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM {{ ref('stg_insta_posts') }}
WHERE location_name IS NOT NULL
  AND TRIM(location_name) != ''
GROUP BY location_name