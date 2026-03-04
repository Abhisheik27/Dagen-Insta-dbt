{{
  config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'kpi', 'instagram'],
    description='Intermediate model for post-level KPIs and performance metrics'
  )
}}

WITH fct_engagement AS (
  SELECT
    post_sk,
    account_sk,
    post_id,
    owner_username,
    likes_count,
    comments_count,
    video_view_count,
    total_engagement,
    engagement_rate,
    viral_score,
    created_at
  FROM {{ ref('fct_instagram_engagement') }}
),

dim_posts AS (
  SELECT
    post_sk,
    post_type,
    caption,
    location_name
  FROM {{ ref('dim_posts') }}
),

account_averages AS (
  SELECT
    account_sk,
    AVG(likes_count) AS avg_likes_per_post,
    AVG(comments_count) AS avg_comments_per_post,
    AVG(total_engagement) AS avg_total_engagement,
    MAX(total_engagement) AS max_engagement_for_account
  FROM fct_engagement
  GROUP BY account_sk
),

post_performance AS (
  SELECT
    fe.post_id,
    fe.owner_username,
    dp.post_type,
    fe.likes_count,
    fe.comments_count,
    fe.video_view_count,
    fe.total_engagement,
    fe.engagement_rate,
    fe.viral_score,
    -- Reach efficiency (engagement per view for videos, per like for images)
    CASE 
      WHEN fe.video_view_count > 0 
      THEN ROUND(fe.total_engagement / fe.video_view_count, 4)
      ELSE ROUND(fe.total_engagement / NULLIF(fe.likes_count, 0), 4)
    END AS reach_efficiency,
    -- Performance tier based on engagement
    CASE 
      WHEN fe.viral_score >= 150 THEN 'Viral'
      WHEN fe.viral_score >= 100 THEN 'High'
      WHEN fe.viral_score >= 50 THEN 'Medium'
      ELSE 'Low'
    END AS performance_tier,
    -- Rank within account
    ROW_NUMBER() OVER (
      PARTITION BY fe.owner_username 
      ORDER BY fe.total_engagement DESC
    ) AS rank_within_account,
    fe.created_at
  FROM fct_engagement fe
  LEFT JOIN dim_posts dp ON fe.post_sk = dp.post_sk
  LEFT JOIN account_averages aa ON fe.account_sk = aa.account_sk
)

SELECT * FROM post_performance