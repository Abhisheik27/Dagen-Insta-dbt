{{
  config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'kpi', 'instagram'],
    description='Intermediate model for time-series engagement trends and analysis'
  )
}}

WITH fct_engagement AS (
  SELECT
    post_id,
    owner_username,
    likes_count,
    comments_count,
    total_engagement,
    engagement_rate,
    created_at
  FROM {{ ref('fct_instagram_engagement') }}
),

dim_timestamps AS (
  SELECT
    timestamp_sk,
    date,
    day_of_week,
    day_name,
    week_of_year,
    month,
    month_name,
    year
  FROM {{ ref('dim_timestamps') }}
),

daily_engagement AS (
  SELECT
    dt.date,
    dt.day_of_week,
    dt.day_name,
    dt.week_of_year,
    dt.month,
    dt.month_name,
    dt.year,
    COUNT(DISTINCT fe.post_id) AS total_posts,
    SUM(fe.likes_count) AS total_likes,
    SUM(fe.comments_count) AS total_comments,
    SUM(fe.total_engagement) AS total_engagement,
    ROUND(AVG(fe.engagement_rate), 4) AS avg_engagement_rate,
    -- Top account for the day
    ARRAY_AGG(
      STRUCT(fe.owner_username, SUM(fe.total_engagement) AS engagement),
      IGNORE NULLS => TRUE
      ORDER BY SUM(fe.total_engagement) DESC
      LIMIT 1
    )[OFFSET(0)].owner_username AS top_account
  FROM fct_engagement fe
  LEFT JOIN dim_timestamps dt ON CAST(fe.created_at AS DATE) = dt.date
  GROUP BY dt.date, dt.day_of_week, dt.day_name, dt.week_of_year, dt.month, dt.month_name, dt.year
),

engagement_with_avg AS (
  SELECT
    *,
    -- Calculate average engagement for the entire period
    AVG(total_engagement) OVER () AS avg_engagement_overall,
    -- Calculate trend direction (comparing to previous day)
    LAG(total_engagement) OVER (ORDER BY date) AS prev_day_engagement,
    CASE 
      WHEN LAG(total_engagement) OVER (ORDER BY date) IS NULL THEN 'New'
      WHEN total_engagement > LAG(total_engagement) OVER (ORDER BY date) THEN 'Up'
      WHEN total_engagement < LAG(total_engagement) OVER (ORDER BY date) THEN 'Down'
      ELSE 'Stable'
    END AS trend_direction,
    -- Growth rate vs previous day
    CASE 
      WHEN LAG(total_engagement) OVER (ORDER BY date) IS NULL THEN 0
      ELSE ROUND(
        ((total_engagement - LAG(total_engagement) OVER (ORDER BY date)) / 
         NULLIF(LAG(total_engagement) OVER (ORDER BY date), 0)) * 100,
        2
      )
    END AS growth_rate_pct
  FROM daily_engagement
),

engagement_trends AS (
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
    CASE 
      WHEN total_engagement > avg_engagement_overall THEN 'Above Average'
      WHEN total_engagement < avg_engagement_overall THEN 'Below Average'
      ELSE 'Average'
    END AS engagement_vs_avg,
    trend_direction,
    growth_rate_pct
  FROM engagement_with_avg
)

SELECT * FROM engagement_trends