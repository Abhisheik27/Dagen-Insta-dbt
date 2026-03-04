{{
  config(
    materialized='table',
    schema='dimensions',
    tags=['dimensions', 'instagram'],
    description='Dimension table for Instagram posts with post attributes'
  )
}}

WITH stg_posts AS (
  SELECT
    post_id,
    short_code,
    post_url,
    created_at,
    post_type,
    caption,
    location_name,
    dbt_loaded_at
  FROM {{ ref('stg_insta_posts') }}
),

dim_posts AS (
  SELECT
    {{ generate_surrogate_key(['post_id']) }} AS post_sk,
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
  FROM stg_posts
)

SELECT * FROM dim_posts