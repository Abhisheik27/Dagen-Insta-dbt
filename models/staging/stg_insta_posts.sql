{{
  config(
    materialized='view',
    schema='staging',
    tags=['staging', 'instagram'],
    description='Staging layer for raw Instagram posts - parses JSON and extracts key fields'
  )
}}

WITH raw_posts AS (
  SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_loaded_at,
    _airbyte_data
  FROM {{ source('airbyte_raw', 'airbyte_raw_insta_united_posts') }}
),

parsed_data AS (
  SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_loaded_at,
    -- Parse JSON fields
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64) AS post_id,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.shortCode') AS short_code,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.url') AS post_url,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.timestamp') AS timestamp_str,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.type') AS post_type,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.ownerUsername') AS owner_username,
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.likesCount') AS INT64) AS likes_count,
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.commentsCount') AS INT64) AS comments_count,
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.videoViewCount') AS INT64) AS video_view_count,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.caption') AS caption,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.locationName') AS location_name
  FROM raw_posts
),

cleaned AS (
  SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_loaded_at,
    post_id,
    short_code,
    post_url,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', timestamp_str) AS created_at,
    post_type,
    owner_username,
    likes_count,
    comments_count,
    COALESCE(video_view_count, 0) AS video_view_count,
    caption,
    location_name,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
  FROM parsed_data
  -- Remove nulls in critical fields
  WHERE post_id IS NOT NULL
    AND short_code IS NOT NULL
    AND owner_username IS NOT NULL
),

deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY _airbyte_loaded_at DESC) AS rn
  FROM cleaned
)

SELECT
  _airbyte_raw_id,
  _airbyte_extracted_at,
  _airbyte_loaded_at,
  post_id,
  short_code,
  post_url,
  created_at,
  post_type,
  owner_username,
  likes_count,
  comments_count,
  video_view_count,
  caption,
  location_name,
  dbt_loaded_at
FROM deduped
WHERE rn = 1