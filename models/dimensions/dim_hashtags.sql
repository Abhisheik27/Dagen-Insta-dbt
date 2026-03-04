{{ config(
    materialized='incremental',
    schema='dimensions',
    unique_id='hashtag_id',
    description='Dimension table for Instagram hashtags extracted from posts',
    on_schema_change='fail'
) }}

WITH raw_hashtags AS (
    -- Extract hashtags from JSON array in airbyte_raw_instagram_posts
    SELECT
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.post_id') AS INT64) AS post_id,
        hashtag,
        JSON_EXTRACT_SCALAR(json_data, '$.posted_at') AS posted_at_str,
        _airbyte_loaded_at
    FROM {{ source('airbyte_instagram', 'airbyte_raw_instagram_posts') }},
    UNNEST([PARSE_JSON(_airbyte_data)]) AS json_data,
    UNNEST(JSON_EXTRACT_ARRAY(json_data, '$.hashtags')) AS hashtag
    WHERE JSON_EXTRACT_ARRAY(json_data, '$.hashtags') IS NOT NULL
        AND ARRAY_LENGTH(JSON_EXTRACT_ARRAY(json_data, '$.hashtags')) > 0
),

parsed_hashtags AS (
    SELECT
        TRIM(JSON_EXTRACT_SCALAR(hashtag, '$')) AS hashtag_name,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S', JSON_EXTRACT_SCALAR(json_data, '$.posted_at')) AS first_seen_at,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM {{ source('airbyte_instagram', 'airbyte_raw_instagram_posts') }},
    UNNEST([PARSE_JSON(_airbyte_data)]) AS json_data,
    UNNEST(JSON_EXTRACT_ARRAY(json_data, '$.hashtags')) AS hashtag
    WHERE JSON_EXTRACT_ARRAY(json_data, '$.hashtags') IS NOT NULL
        AND ARRAY_LENGTH(JSON_EXTRACT_ARRAY(json_data, '$.hashtags')) > 0
),

deduplicated AS (
    SELECT DISTINCT
        hashtag_name,
        MIN(first_seen_at) AS first_seen_at,
        MAX(dbt_loaded_at) AS dbt_loaded_at
    FROM parsed_hashtags
    WHERE hashtag_name IS NOT NULL
        AND TRIM(hashtag_name) != ''
    GROUP BY hashtag_name
),

with_surrogate_key AS (
    SELECT
        FARM_FINGERPRINT(hashtag_name) AS hashtag_id,
        hashtag_name,
        first_seen_at,
        dbt_loaded_at
    FROM deduplicated
)

{% if execute %}
    {% if run_started_at is not none %}
        -- Incremental logic: only insert new hashtags
        SELECT *
        FROM with_surrogate_key
        {% if is_incremental() %}
            WHERE hashtag_name NOT IN (
                SELECT hashtag_name FROM {{ this }}
            )
        {% endif %}
    {% else %}
        SELECT * FROM with_surrogate_key
    {% endif %}
{% else %}
    SELECT * FROM with_surrogate_key
{% endif %}