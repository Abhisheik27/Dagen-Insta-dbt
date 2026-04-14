{{
  config(
    materialized='incremental',
    schema='facts',
    tags=['facts', 'metrics', 'instagram'],
    unique_id='metric_id'
  )
}}

WITH stg_posts AS (
  SELECT
    post_id,
    owner_username,
    created_at,
    updated_at,
    likes_count,
    comments_count,
    video_view_count,
    shares_count
  FROM {{ ref('stg_insta_posts_clean') }}
  WHERE post_id IS NOT NULL
)

SELECT
  {{ generate_surrogate_key(['post_id', 'updated_at']) }} AS metric_id,
  post_id,
  owner_username,
  likes_count,
  comments_count,
  video_view_count,
  shares_count,
  created_at,
  updated_at AS measured_at,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM stg_posts

{% if execute %}
  {% if flags.FULL_REFRESH %}
    -- Full refresh: load all records
  {% else %}
    -- Incremental: only load new/updated records
    WHERE updated_at > (SELECT COALESCE(MAX(measured_at), '1900-01-01') FROM {{ this }})
  {% endif %}
{% endif %}