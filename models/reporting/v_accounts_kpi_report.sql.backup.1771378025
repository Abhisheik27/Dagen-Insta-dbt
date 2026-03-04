{{
  config(
    materialized='view',
    schema='reporting',
    tags=['reporting', 'kpi', 'instagram'],
    description='Reporting view for account performance KPIs - ready for dashboard consumption'
  )
}}

WITH account_metrics AS (
  SELECT
    owner_username,
    total_posts,
    avg_likes,
    avg_comments,
    avg_engagement_rate,
    total_likes,
    total_comments,
    total_reach,
    avg_reach_per_post,
    post_frequency_per_week,
    last_post_date,
    days_since_last_post,
    best_performing_post_type,
    performance_tier
  FROM {{ ref('int_account_metrics') }}
),

account_ranking AS (
  SELECT
    owner_username,
    total_posts,
    avg_likes,
    avg_comments,
    avg_engagement_rate,
    total_likes,
    total_comments,
    total_reach,
    avg_reach_per_post,
    post_frequency_per_week,
    last_post_date,
    days_since_last_post,
    best_performing_post_type,
    performance_tier,
    -- Engagement trend (compare to account average)
    CASE 
      WHEN avg_engagement_rate > (
        AVG(avg_engagement_rate) OVER ()
      ) THEN 'Growing'
      WHEN avg_engagement_rate < (
        AVG(avg_engagement_rate) OVER ()
      ) THEN 'Declining'
      ELSE 'Stable'
    END AS engagement_trend,
    -- Growth rate (posts per week trend)
    ROUND(
      ((post_frequency_per_week - AVG(post_frequency_per_week) OVER ()) / 
       NULLIF(AVG(post_frequency_per_week) OVER (), 0)) * 100,
      2
    ) AS growth_rate_pct,
    -- Overall account rank
    ROW_NUMBER() OVER (ORDER BY avg_engagement_rate DESC) AS account_rank
  FROM account_metrics
)

SELECT
  owner_username,
  total_posts,
  avg_likes,
  avg_comments,
  avg_engagement_rate,
  total_likes,
  total_comments,
  total_reach,
  avg_reach_per_post,
  post_frequency_per_week,
  last_post_date,
  days_since_last_post,
  best_performing_post_type,
  performance_tier,
  engagement_trend,
  growth_rate_pct,
  account_rank
FROM account_ranking
ORDER BY avg_engagement_rate DESC