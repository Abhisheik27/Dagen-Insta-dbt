{{
  config(
    materialized='view',
    schema='reporting',
    tags=['reporting', 'kpi', 'instagram'],
    description='Reporting view for post performance KPIs - ready for dashboard consumption'
  )
}}

WITH post_perf AS (
  SELECT
    post_id,
    owner_username,
    post_type,
    likes_count,
    comments_count,
    video_view_count,
    total_engagement,
    engagement_rate,
    viral_score,
    reach_efficiency,
    performance_tier,
    rank_within_account,
    created_at
  FROM {{ ref('int_post_performance') }}
),

dim_posts AS (
  SELECT
    post_sk,
    post_id,
    short_code,
    post_url,
    caption,
    location_name,
    created_date
  FROM {{ ref('dim_posts') }}
),

enriched_posts AS (
  SELECT
    pp.post_id,
    pp.owner_username,
    pp.post_type,
    dp.created_date,
    dp.short_code,
    dp.post_url,
    pp.likes_count,
    pp.comments_count,
    pp.video_view_count,
    pp.total_engagement,
    pp.engagement_rate,
    pp.viral_score,
    pp.reach_efficiency,
    pp.performance_tier,
    pp.rank_within_account,
    -- Top post flag (in top 10% for account)
    CASE 
      WHEN pp.rank_within_account <= CEIL(
        COUNT(*) OVER (PARTITION BY pp.owner_username) * 0.1
      ) 
      THEN TRUE 
      ELSE FALSE 
    END AS is_top_post,
    -- Engagement rank overall
    ROW_NUMBER() OVER (ORDER BY pp.total_engagement DESC) AS overall_engagement_rank,
    pp.created_at
  FROM post_perf pp
  LEFT JOIN dim_posts dp ON pp.post_id = dp.post_id
)

SELECT
  post_id,
  owner_username,
  post_type,
  created_date,
  short_code,
  post_url,
  likes_count,
  comments_count,
  video_view_count,
  total_engagement,
  engagement_rate,
  viral_score,
  reach_efficiency,
  performance_tier,
  rank_within_account,
  is_top_post,
  overall_engagement_rank,
  created_at
FROM enriched_posts
ORDER BY total_engagement DESC