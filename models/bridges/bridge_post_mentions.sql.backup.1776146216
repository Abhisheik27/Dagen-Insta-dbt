{{ config(
    materialized='incremental',
    schema='bridges',
    description='Bridge table for many-to-many relationship between posts and mentioned accounts',
    on_schema_change='fail'
) }}

WITH mention_data AS (
    -- Extract mentions from JSON array in airbyte_raw_instagram_posts
    SELECT
        CAST(JSON_EXTRACT_SCALAR(json_data, '$.post_id') AS INT64) AS post_id,
        TRIM(JSON_EXTRACT_SCALAR(mention, '$')) AS mentioned_username,
        _airbyte_loaded_at,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM {{ source('airbyte_instagram', 'airbyte_raw_instagram_posts') }},
    UNNEST([PARSE_JSON(_airbyte_data)]) AS json_data,
    UNNEST(JSON_EXTRACT_ARRAY(json_data, '$.mentions')) AS mention
    WHERE JSON_EXTRACT_ARRAY(json_data, '$.mentions') IS NOT NULL
        AND ARRAY_LENGTH(JSON_EXTRACT_ARRAY(json_data, '$.mentions')) > 0
        AND TRIM(JSON_EXTRACT_SCALAR(mention, '$')) IS NOT NULL
),

with_account_ids AS (
    SELECT
        md.post_id,
        md.mentioned_username,
        -- Use the same surrogate key generation as dim_accounts
        FARM_FINGERPRINT(md.mentioned_username) AS mentioned_account_id,
        md._airbyte_loaded_at,
        md.dbt_loaded_at
    FROM mention_data md
),

deduplicated AS (
    SELECT DISTINCT
        post_id,
        mentioned_account_id,
        dbt_loaded_at
    FROM with_account_ids
)

{% if execute %}
    {% if run_started_at is not none %}
        -- Incremental logic: only insert new post-mention relationships
        SELECT *
        FROM deduplicated
        {% if is_incremental() %}
            WHERE (post_id, mentioned_account_id) NOT IN (
                SELECT post_id, mentioned_account_id FROM {{ this }}
            )
        {% endif %}
    {% else %}
        SELECT * FROM deduplicated
    {% endif %}
{% else %}
    SELECT * FROM deduplicated
{% endif %}