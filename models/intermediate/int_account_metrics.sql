{{
  config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'kpi', 'instagram'],
    description='Intermediate model for account-level aggregated metrics and KPIs'
  )
}}

WITH fct_engagement AS (
  SELECT
    account_sk,
    post_id,
    owner_username,
    likes_count,
    comments_count,
    video_view_count,
    total_engagement,
    engagement_rate,
    created_at
  FROM {{ ref('fct_instagram_engagement') }}
),

dim_posts AS (
  SELECT
    post_sk,
    post_type
  FROM {{ ref('dim_posts') }}
),

account_aggregates AS (
  SELECT
    fe.account_sk,
    fe.owner_username,
    COUNT(DISTINCT fe.post_id) AS total_posts,
    ROUND(AVG(fe.likes_count), 2) AS avg_likes,
    ROUND(AVG(fe.comments_count), 2) AS avg_comments,
    ROUND(AVG(fe.engagement_rate), 4) AS avg_engagement_rate,
    SUM(fe.likes_count) AS total_likes,
    SUM(fe.comments_count) AS total_comments,
    SUM(fe.total_engagement) AS total_reach,
    ROUND(SUM(fe.total_engagement) / COUNT(DISTINCT fe.post_id), 2) AS avg_reach_per_post,
    -- Posts per week (assuming data spans from min to max date)
    ROUND(
      COUNT(DISTINCT fe.post_id) / 
      NULLIF(
        DATE_DIFF(MAX(CAST(fe.created_at AS DATE)), MIN(CAST(fe.created_at AS DATE)), WEEK) + 1,
        0
      ),
      2
    ) AS post_frequency_per_week,
    MAX(CAST(fe.created_at AS DATE)) AS last_post_date,
    DATE_DIFF(CURRENT_DATE(), MAX(CAST(fe.created_at AS DATE)), DAY) AS days_since_last_post,
    MIN(fe.created_at) AS first_post_date,
    MAX(fe.created_at) AS most_recent_post_date
  FROM fct_engagement fe
  GROUP BY fe.account_sk, fe.owner_username
),

best_post_type AS (
  SELECT
    fe.account_sk,
    fe.owner_username,
    dp.post_type,
    ROW_NUMBER() OVER (
      PARTITION BY fe.account_sk 
      ORDER BY AVG(fe.total_engagement) DESC
    ) AS post_type_rank
  FROM fct_engagement fe
  LEFT JOIN dim_posts dp ON fe.post_sk = dp.post_sk
  GROUP BY fe.account_sk, fe.owner_username, dp.post_type
),

account_metrics AS (
  SELECT
    aa.account_sk,
    aa.owner_username,
    aa.total_posts,
    aa.avg_likes,
    aa.avg_comments,
    aa.avg_engagement_rate,
    aa.total_likes,
    aa.total_comments,
    aa.total_reach,
    aa.avg_reach_per_post,
    aa.post_frequency_per_week,
    aa.last_post_date,
    aa.days_since_last_post,
    aa.first_post_date,
    aa.most_recent_post_date,
    bpt.post_type AS best_performing_post_type,
    -- Performance tier
    CASE 
      WHEN aa.avg_engagement_rate >= 0.0075 THEN 'High'
      WHEN aa.avg_engagement_rate >= 0.005 THEN 'Medium'
      ELSE 'Low'
    END AS performance_tier
  FROM account_aggregates aa
  LEFT JOIN best_post_type bpt ON aa.account_sk = bpt.account_sk AND bpt.post_type_rank = 1
)

SELECT * FROM account_metrics