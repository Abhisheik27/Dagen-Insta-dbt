{{ config(
    materialized='incremental',
    schema='dimensions',
    unique_id='location_id',
    description='Dimension table for Instagram locations with surrogate keys',
    on_schema_change='fail'
) }}

WITH source_data AS (
    SELECT DISTINCT
        location_name,
        MIN(posted_at) AS first_seen_at,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM {{ ref('stg_posts') }}
    WHERE location_name IS NOT NULL
        AND TRIM(location_name) != ''
    GROUP BY location_name
),

with_surrogate_key AS (
    SELECT
        FARM_FINGERPRINT(location_name) AS location_id,
        location_name,
        first_seen_at,
        dbt_loaded_at
    FROM source_data
)

{% if execute %}
    {% if run_started_at is not none %}
        -- Incremental logic: only insert new locations
        SELECT *
        FROM with_surrogate_key
        {% if is_incremental() %}
            WHERE location_name NOT IN (
                SELECT location_name FROM {{ this }}
            )
        {% endif %}
    {% else %}
        SELECT * FROM with_surrogate_key
    {% endif %}
{% else %}
    SELECT * FROM with_surrogate_key
{% endif %}