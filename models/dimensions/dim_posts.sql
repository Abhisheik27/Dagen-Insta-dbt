{{
  config(
    materialized='table',
    schema='dimensions',
    tags=['dimension', 'instagram']
  )
}}

SELECT
  MD5(CAST(CONCAT(post_id, '|', short_code) AS STRING)) AS post_sk,
  post_id,
  short_code,
  post_url,
  post_type,
  caption,
  location_name,
  CAST(created_at AS DATE) AS created_date,
  created_at,
  CURRENT_TIMESTAMP() AS updated_at,
  CURRENT_TIMESTAMP() AS dim_loaded_at
FROM {{ ref('stg_insta_posts') }}
WHERE post_id IS NOT NULL