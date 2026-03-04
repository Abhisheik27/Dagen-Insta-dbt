{{ config(
    materialized='table',
    schema='staging',
    description='Cleaned and deduplicated Instagram posts from raw sources'
) }}

WITH source1_parsed AS (
    -- Parse JSON from airbyte_raw_insta_united_posts
    SELECT
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.id') AS INT64) AS post_id,
        JSON_EXTRACT_SCALAR(json_data, '$.ownerUsername') AS username,
        JSON_EXTRACT_SCALAR(json_data, '$.shortCode') AS short_code,
        JSON_EXTRACT_SCALAR(json_data, '$.type') AS post_type,
        JSON_EXTRACT_SCALAR(json_data, '$.caption') AS caption,
        JSON_EXTRACT_SCALAR(json_data, '$.url') AS post_url,
        NULL AS image_url,
        NULL AS video_url,
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.likesCount') AS INT64) AS likes_count,
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.commentsCount') AS INT64) AS comments_count,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(json_data, '$.videoViewCount'), '0') AS INT64) AS view_count,
        NULL AS shares_count,
        JSON_EXTRACT_SCALAR(json_data, '$.locationName') AS location_name,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', JSON_EXTRACT_SCALAR(json_data, '$.timestamp')) AS posted_at,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        '1' AS source_system
    FROM {{ source('airbyte_instagram', 'airbyte_raw_insta_united_posts') }},
    UNNEST([PARSE_JSON(_airbyte_data)]) AS json_data
    WHERE JSON_EXTRACT_SCALAR(json_data, '$.id') IS NOT NULL
),

source2_parsed AS (
    -- Parse JSON from airbyte_raw_instagram_posts
    SELECT
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.post_id') AS INT64) AS post_id,
        JSON_EXTRACT_SCALAR(json_data, '$.username') AS username,
        NULL AS short_code,
        CASE 
            WHEN JSON_EXTRACT_SCALAR(json_data, '$.is_video') = 'true' THEN 'Video'
            ELSE 'Image'
        END AS post_type,
        JSON_EXTRACT_SCALAR(json_data, '$.caption') AS caption,
        NULL AS post_url,
        JSON_EXTRACT_SCALAR(json_data, '$.image_url') AS image_url,
        JSON_EXTRACT_SCALAR(json_data, '$.video_url') AS video_url,
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.likes_count') AS INT64) AS likes_count,
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.comments_count') AS INT64) AS comments_count,
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.view_count') AS INT64) AS view_count,
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.shares_count') AS INT64) AS shares_count,
        JSON_EXTRACT_SCALAR(json_data, '$.location') AS location_name,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S', JSON_EXTRACT_SCALAR(json_data, '$.posted_at')) AS posted_at,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S', JSON_EXTRACT_SCALAR(json_data, '$.created_at')) AS created_at,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S', JSON_EXTRACT_SCALAR(json_data, '$.updated_at')) AS updated_at,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        '2' AS source_system
    FROM {{ source('airbyte_instagram', 'airbyte_raw_instagram_posts') }},
    UNNEST([PARSE_JSON(_airbyte_data)]) AS json_data
    WHERE JSON_EXTRACT_SCALAR(json_data, '$.post_id') IS NOT NULL
),

combined AS (
    SELECT * FROM source1_parsed
    UNION ALL
    SELECT * FROM source2_parsed
),

deduplicated AS (
    -- Deduplicate by post_id, preferring source2 (more complete data)
    SELECT
        * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY source_system DESC, _airbyte_loaded_at DESC) AS rn
        FROM combined
    )
    WHERE rn = 1
)

SELECT
    post_id,
    username,
    short_code,
    post_type,
    caption,
    post_url,
    image_url,
    video_url,
    likes_count,
    comments_count,
    view_count,
    shares_count,
    location_name,
    posted_at,
    created_at,
    updated_at,
    _airbyte_extracted_at,
    _airbyte_loaded_at,
    source_system,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM deduplicated
ORDER BY posted_at DESC