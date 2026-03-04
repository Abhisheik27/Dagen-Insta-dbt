{{ config(
    materialized='incremental',
    schema='facts',
    unique_id='metric_id',
    description='Fact table for Instagram post engagement metrics',
    on_schema_change='fail'
) }}

WITH source_data AS (
    SELECT
        post_id,
        username,
        likes_count,
        comments_count,
        view_count,
        shares_count,
        posted_at,
        updated_at,
        _airbyte_loaded_at,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM {{ ref('stg_posts') }}
    WHERE post_id IS NOT NULL
),

with_surrogate_key AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(CAST(post_id AS STRING), '_', CAST(updated_at AS STRING))) AS metric_id,
        post_id,
        username,
        likes_count,
        comments_count,
        view_count,
        shares_count,
        posted_at,
        updated_at AS measured_at,
        dbt_loaded_at
    FROM source_data
)

{% if execute %}
    {% if run_started_at is not none %}
        -- Incremental logic: only insert new or updated metrics
        SELECT *
        FROM with_surrogate_key
        {% if is_incremental() %}
            WHERE post_id NOT IN (
                SELECT post_id FROM {{ this }}
            )
            OR measured_at > (SELECT MAX(measured_at) FROM {{ this }})
        {% endif %}
    {% else %}
        SELECT * FROM with_surrogate_key
    {% endif %}
{% else %}
    SELECT * FROM with_surrogate_key
{% endif %}