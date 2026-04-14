{{
  config(
    materialized='table',
    schema='dimensions',
    tags=['dimension', 'instagram', 'account']
  )
}}

SELECT
  FARM_FINGERPRINT(owner_username) AS account_sk,
  owner_username AS account_username,
  MIN(created_at) AS first_seen_at,
  MAX(created_at) AS last_updated_at,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM {{ ref('stg_insta_posts') }}
WHERE owner_username IS NOT NULL
GROUP BY owner_username