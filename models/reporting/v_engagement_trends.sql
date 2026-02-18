{{
  config(
    materialized='view',
    schema='reporting',
    tags=['reporting', 'kpi', 'instagram'],
    description='Reporting view for engagement trends and time-series analysis'
  )
}}

WITH engagement_trends AS (
  SELECT
    date,
    day_of_week,
    day_name,
    week_of_year,
    month,
    month_name,
    year,
    total_posts,
    total_likes,
    total_comments,
    total_engagement,
    avg_engagement_rate,
    top_account,
    engagement_vs_avg,
    trend_direction,
    growth_rate_pct
  FROM {{ ref('int_engagement_trends') }}
),

trends_with_stats AS (
  SELECT
    date,
    day_of_week,
    day_name,
    week_of_year,
    month,
    month_name,
    year,
    total_posts,
    total_likes,
    total_comments,
    total_engagement,
    avg_engagement_rate,
    top_account,
    engagement_vs_avg,
    trend_direction,
    growth_rate_pct,
    -- 7-day moving average
    ROUND(
      AVG(total_engagement) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ),
      2
    ) AS engagement_7day_ma,
    -- 30-day moving average
    ROUND(
      AVG(total_engagement) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ),
      2
    ) AS engagement_30day_ma,
    -- Volatility (std dev over 7 days)
    ROUND(
      STDDEV(total_engagement) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ),
      2
    ) AS engagement_volatility_7day
  FROM engagement_trends
)

SELECT
  date,
  day_of_week,
  day_name,
  week_of_year,
  month,
  month_name,
  year,
  total_posts,
  total_likes,
  total_comments,
  total_engagement,
  avg_engagement_rate,
  top_account,
  engagement_vs_avg,
  trend_direction,
  growth_rate_pct,
  engagement_7day_ma,
  engagement_30day_ma,
  engagement_volatility_7day
FROM trends_with_stats
ORDER BY date DESC