{{ config(
    materialized='incremental',
    schema='dimensions',
    unique_key='account_sk',
    description='3NF Dimension table for Instagram accounts with surrogate keys',
    on_schema_change='sync_all_columns',
    alias='dim_accounts_3nf'
) }}

WITH source_data AS (
    SELECT DISTINCT
        username,
        MIN(posted_at) AS first_seen_at,
        MAX(updated_at) AS last_updated_at,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM {{ ref('stg_posts') }}
    WHERE username IS NOT NULL
    GROUP BY username
),

with_surrogate_key AS (
    SELECT
        FARM_FINGERPRINT(username) AS account_sk,
        username AS account_username,
        first_seen_at,
        last_updated_at,
        dbt_loaded_at
    FROM source_data
)

{% if execute %}
    {% if run_started_at is not none %}
        -- Incremental logic: only insert new or updated accounts
        SELECT *
        FROM with_surrogate_key
        {% if is_incremental() %}
            WHERE account_username NOT IN (
                SELECT account_username FROM {{ this }}
            )
            OR last_updated_at > (SELECT MAX(last_updated_at) FROM {{ this }})
        {% endif %}
    {% else %}
        SELECT * FROM with_surrogate_key
    {% endif %}
{% else %}
    SELECT * FROM with_surrogate_key
{% endif %}