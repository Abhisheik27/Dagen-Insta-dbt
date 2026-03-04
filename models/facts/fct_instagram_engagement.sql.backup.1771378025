{{
  config(
    materialized='table',
    schema='facts',
    tags=['facts', 'instagram'],
    description='Fact table for Instagram engagement metrics - central fact table for star schema'
  )
}}

WITH stg_posts AS (
  SELECT
    post_id,
    owner_username,
    created_at,
    likes_count,
    comments_count,
    video_view_count
  FROM {{ ref('stg_insta_posts') }}
),

dim_posts AS (
  SELECT
    post_sk,
    post_id
  FROM {{ ref('dim_posts') }}
),

dim_accounts AS (
  SELECT
    account_sk,
    account_username
  FROM {{ ref('dim_accounts') }}
),

dim_timestamps AS (
  SELECT
    timestamp_sk,
    date
  FROM {{ ref('dim_timestamps') }}
),

joined_data AS (
  SELECT
    dp.post_sk,
    da.account_sk,
    dt.timestamp_sk,
    stg.post_id,
    stg.owner_username,
    stg.likes_count,
    stg.comments_count,
    stg.video_view_count,
    stg.created_at
  FROM stg_posts stg
  LEFT JOIN dim_posts dp ON stg.post_id = dp.post_id
  LEFT JOIN dim_accounts da ON stg.owner_username = da.account_username
  LEFT JOIN dim_timestamps dt ON CAST(stg.created_at AS DATE) = dt.date
),

engagement_metrics AS (
  SELECT
    post_sk,
    account_sk,
    timestamp_sk,
    post_id,
    owner_username,
    likes_count,
    comments_count,
    video_view_count,
    created_at,
    -- Calculated metrics
    (likes_count + comments_count) AS total_engagement,
    CASE 
      WHEN (likes_count + comments_count) > 0 
      THEN ROUND(
        (likes_count + comments_count) / NULLIF((likes_count + comments_count + 1), 0), 
        4
      )
      ELSE 0
    END AS engagement_rate,
    -- Viral score (simplified - based on engagement relative to account average)
    ROUND(
      (likes_count + comments_count) * 100.0 / NULLIF(
        AVG(likes_count + comments_count) OVER (PARTITION BY owner_username), 
        0
      ),
      2
    ) AS viral_score,
    CURRENT_TIMESTAMP() AS fact_loaded_at
  FROM joined_data
)

SELECT * FROM engagement_metrics