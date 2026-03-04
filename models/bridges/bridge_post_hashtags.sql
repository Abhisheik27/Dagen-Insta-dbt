{{ config(
    materialized='incremental',
    schema='bridges',
    description='Bridge table for many-to-many relationship between posts and hashtags',
    on_schema_change='fail'
) }}

WITH hashtag_data AS (
    -- Extract hashtags from JSON array in airbyte_raw_instagram_posts
    SELECT
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.post_id') AS INT64) AS post_id,
        TRIM(JSON_EXTRACT_SCALAR(hashtag, '$')) AS hashtag_name,
        _airbyte_loaded_at,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM {{ source('airbyte_instagram', 'airbyte_raw_instagram_posts') }},
    UNNEST([PARSE_JSON(_airbyte_data)]) AS json_data,
    UNNEST(JSON_EXTRACT_ARRAY(json_data, '$.hashtags')) AS hashtag
    WHERE JSON_EXTRACT_ARRAY(json_data, '$.hashtags') IS NOT NULL
        AND ARRAY_LENGTH(JSON_EXTRACT_ARRAY(json_data, '$.hashtags')) > 0
        AND TRIM(JSON_EXTRACT_SCALAR(hashtag, '$')) IS NOT NULL
),

with_hashtag_ids AS (
    SELECT
        hd.post_id,
        hd.hashtag_name,
        FARM_FINGERPRINT(hd.hashtag_name) AS hashtag_id,
        hd._airbyte_loaded_at,
        hd.dbt_loaded_at
    FROM hashtag_data hd
),

deduplicated AS (
    SELECT DISTINCT
        post_id,
        hashtag_id,
        dbt_loaded_at
    FROM with_hashtag_ids
)

{% if execute %}
    {% if run_started_at is not none %}
        -- Incremental logic: only insert new post-hashtag relationships
        SELECT *
        FROM deduplicated
        {% if is_incremental() %}
            WHERE (post_id, hashtag_id) NOT IN (
                SELECT post_id, hashtag_id FROM {{ this }}
            )
        {% endif %}
    {% else %}
        SELECT * FROM deduplicated
    {% endif %}
{% else %}
    SELECT * FROM deduplicated
{% endif %}